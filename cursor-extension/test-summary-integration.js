// Headless integration check using a newly created synthetic Codex profile.
const assert = require('assert');
const child = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const Module = require('module');
const os = require('os');
const path = require('path');

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cosyncx-status-'));
const python = process.env.NEXUS_PYTHON || 'python3';
const script = path.resolve(__dirname, '..', 'src', 'nexus_sync.py');
const config = path.join(root, 'config.json');
const dirs = Object.fromEntries(['workspace', 'codex', 'shared', 'state'].map(name => [name, path.join(root, name)]));
for (const directory of Object.values(dirs)) fs.mkdirSync(directory);
const sessions = path.join(dirs.codex, 'sessions', '2026', '01', '01');
fs.mkdirSync(sessions, { recursive: true });
fs.writeFileSync(path.join(sessions, 'rollout-synthetic.jsonl'), '{"type":"session_meta","payload":{"id":"synthetic"}}\n');
const key = path.join(root, 'key');
fs.writeFileSync(key, crypto.randomBytes(32), { mode: 0o600 });
try {
    child.execFileSync(python, [script, 'init-config', '--output', config, '--nexus', dirs.workspace,
        '--codex-home', dirs.codex, '--shared', dirs.shared, '--state-dir', dirs.state,
        '--key-file', key, '--machine', 'linux'], { stdio: 'pipe' });
    child.execFileSync(python, [script, 'snapshot', '--codex-home', dirs.codex, '--shared', dirs.shared,
        '--machine', 'linux', '--key-file', key], { stdio: 'pipe' });
} catch (error) {
    fs.rmSync(root, { recursive: true, force: true });
    throw error;
}

let status;
const settings = { python, script, config };
const vscode = {
    StatusBarAlignment: { Left: 1 },
    window: { createStatusBarItem() { status = { show() {}, dispose() {} }; return status; } },
    commands: { registerCommand() { return { dispose() {} }; } },
    workspace: {
        getConfiguration() { return { get(key) { return settings[key]; } }; },
        onDidChangeConfiguration() { return { dispose() {} }; },
    },
};
const originalLoad = Module._load;
const originalExec = child.execFile;
const disposables = [];
Module._load = function(request, parent, main) {
    return request === 'vscode' ? vscode : originalLoad.apply(this, arguments);
};
child.execFile = function(file, args, options, callback) {
    return originalExec(file, args, options, (error, stdout, stderr) => {
        try {
            callback(error, stdout, stderr);
            assert.ifError(error);
            assert(status.text.includes('наблюдатель'));
            assert(status.tooltip.includes('не подтверждена'));
            console.log('Synthetic Cursor status: OK; import not claimed');
        } catch (failure) {
            console.error('Status integration test failed:', failure.message);
            process.exitCode = 1;
        } finally {
            for (const disposable of disposables) disposable.dispose();
            Module._load = originalLoad;
            child.execFile = originalExec;
            fs.rmSync(root, { recursive: true, force: true });
        }
    });
};
require('./extension').activate({ subscriptions: disposables });
