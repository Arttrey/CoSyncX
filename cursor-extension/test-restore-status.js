const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { readRestoreStatus } = require('./restore-status');

(async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cosyncx-status-test-'));
    try {
        const file = path.join(root, 'status.json');
        const status = { format: 'cosyncx-restore-status-v1', mock_only: false, updated_at_ms: Date.now(), state: 'COMPLETE',
                         restore_event: { state: 'RESTORED', indexed: true, changed_chats: 1,
                                          mock_only: false, id: 'a'.repeat(32) } };
        fs.writeFileSync(file, JSON.stringify(status), { mode: 0o600 });
        assert.deepStrictEqual(await readRestoreStatus(file), status);
        assert.strictEqual(await readRestoreStatus('relative.json'), null);
        fs.writeFileSync(file, JSON.stringify({...status, mock_only: true}));
        assert.strictEqual(await readRestoreStatus(file), null);
        fs.writeFileSync(file, JSON.stringify({...status, updated_at_ms: Date.now() - 100000}));
        assert.strictEqual(await readRestoreStatus(file), null);
        fs.writeFileSync(file, JSON.stringify({...status, state: 'SYNCED'}));
        assert.strictEqual(await readRestoreStatus(file), null);
        fs.writeFileSync(file, JSON.stringify(status));
        if (process.platform !== 'win32') {
            fs.chmodSync(file, 0o644);
            assert.strictEqual(await readRestoreStatus(file), null);
            fs.chmodSync(file, 0o600);
            const link = path.join(root, 'link');
            fs.symlinkSync(file, link);
            assert.strictEqual(await readRestoreStatus(link), null);
        }
        console.log('Cursor restore status test: OK');
    } finally {
        fs.rmSync(root, {recursive: true, force: true});
    }
})().catch(error => { console.error(error); process.exitCode = 1; });
