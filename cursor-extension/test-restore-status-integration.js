const assert = require('assert');
const fs = require('fs');
const Module = require('module');
const os = require('os');
const path = require('path');

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cosyncx-status-integration-'));
const file = path.join(root, 'status.json');
let stored;
const calls = [];
let refresh;
const statusItem = { show() {}, dispose() {} };
const vscode = {
    StatusBarAlignment: {Left: 1},
    window: {createStatusBarItem() { return statusItem; }},
    workspace: {
        getConfiguration() { return {get(key) {
            if (key === 'restoreStatusFile') return file;
            if (key === 'autoReloadAfterRestore') return true;
            return '';
        }}; },
        onDidChangeConfiguration() { return {dispose() {}}; }
    },
    commands: {
        registerCommand(_name, handler) { refresh = handler; return {dispose() {}}; },
        async executeCommand(name) { calls.push(name); }
    }
};
const originalLoad = Module._load;
Module._load = function(request) { return request === 'vscode' ? vscode : originalLoad.apply(this, arguments); };
const extension = require('./extension');
Module._load = originalLoad;
const context = { subscriptions: [], workspaceState: { get() { return stored; }, async update(_key, value) { stored = value; } } };
const waitFor = async predicate => {
    for (let i = 0; i < 100; i++) {
        if (predicate()) return;
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    throw new Error('Timed out waiting for status');
};
(async () => {
    try {
        const event = { id: 'a'.repeat(32), state: 'RESTORED', indexed: true, changed_chats: 1, mock_only: false };
        fs.writeFileSync(file, JSON.stringify({format: 'cosyncx-restore-status-v1', updated_at_ms: Date.now(),
            mock_only: false, state: 'COMPLETE', restore_event: event}), {mode: 0o600});
        extension.activate(context);
        await waitFor(() => calls.length === 1);
        assert.deepStrictEqual(calls, ['workbench.action.reloadWindow']);
        assert.strictEqual(statusItem.text, '$(check) CoSyncX: восстановлено');
        refresh();
        await new Promise(resolve => setTimeout(resolve, 50));
        assert.strictEqual(calls.length, 1);
        const later = {...event, id: 'b'.repeat(32)};
        fs.writeFileSync(file, JSON.stringify({format: 'cosyncx-restore-status-v1', updated_at_ms: Date.now(),
            mock_only: false, state: 'PENDING', restore_event: later}));
        refresh();
        await waitFor(() => calls.length === 2);
        assert.strictEqual(statusItem.text, '$(warning) CoSyncX: ожидание');
        assert.deepStrictEqual(calls, ['workbench.action.reloadWindow', 'workbench.action.reloadWindow']);
        console.log('Cursor status-to-reload integration: OK');
    } finally {
        for (const item of context.subscriptions) if (item && item.dispose) item.dispose();
        fs.rmSync(root, {recursive: true, force: true});
    }
})().catch(error => { console.error(error); process.exitCode = 1; });
