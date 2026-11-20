// Headless integration check against a synthetic demo configuration only.
const assert = require('assert');
const child = require('child_process');
const fs = require('fs');
const Module = require('module');
const os = require('os');
const path = require('path');

const input = process.argv[2];
if (!input) throw new Error('Укажите путь к тестовой конфигурации из status_configs');
const config = fs.realpathSync(input);
const relative = path.relative(fs.realpathSync(os.tmpdir()), config);
assert(!relative.startsWith('..') && !path.isAbsolute(relative));
assert(relative.split(path.sep)[0].startsWith('nexus-sync-demo-'));

let status;
const settings = {
    python: process.env.NEXUS_PYTHON || 'python3',
    script: path.resolve(__dirname, '..', 'src', 'nexus_sync.py'),
    config,
};
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
            assert(status.tooltip.includes('не импортированы'));
            console.log('Тестовый статус: ответ Python прочитан, импорт не подтверждён');
        } catch (failure) {
            console.error('Тест статуса не прошёл:', failure.message);
            process.exitCode = 1;
        } finally {
            for (const disposable of disposables) disposable.dispose();
            Module._load = originalLoad;
            child.execFile = originalExec;
        }
    });
};
require('./extension').activate({ subscriptions: disposables });
