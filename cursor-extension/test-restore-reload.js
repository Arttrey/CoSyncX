const assert = require('assert');
const Module = require('module');
const originalLoad = Module._load;
const calls = [];
let fail = false;
const vscode = { commands: { async executeCommand(name) {
    calls.push(name);
    if (fail) throw new Error('reload failed');
} } };
Module._load = function(request) {
    return request === 'vscode' ? vscode : originalLoad.apply(this, arguments);
};
const { makeRestoreReloader } = require('./extension');
Module._load = originalLoad;
let value;
const state = { get() { return value; }, async update(_key, next) { value = next; } };
const reload = makeRestoreReloader(vscode, state);
const event = { id: 'a'.repeat(32), state: 'RESTORED', indexed: true, changed_chats: 1, mock_only: false };
(async () => {
    assert.strictEqual(await reload(event, false), false);
    assert.strictEqual(await reload({...event, state: 'SYNCED'}, true), false);
    assert.strictEqual(await reload({...event, indexed: false}, true), false);
    assert.strictEqual(await reload({...event, changed_chats: 0}, true), false);
    assert.strictEqual(await reload({...event, mock_only: true}, true), false);
    assert.strictEqual(await reload({...event, id: 'untrusted'}, true), false);
    assert.strictEqual(await reload(event, true), true);
    assert.deepStrictEqual(calls, ['workbench.action.reloadWindow']);
    assert.strictEqual(await reload(event, true), false);
    fail = true;
    const later = {...event, id: 'b'.repeat(32)};
    await assert.rejects(reload(later, true));
    assert.strictEqual(value, event.id);
    fail = false;
    assert.strictEqual(await reload(later, true), true);
    console.log('Cursor restore reload test: OK');
})().catch(error => { console.error(error); process.exitCode = 1; });
