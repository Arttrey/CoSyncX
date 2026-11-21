const fs = require('fs');
const path = require('path');

// The status file is local private state. Never follow a symlink or read a
// file owned by someone else; a transfer status cannot certify restoration.
async function readRestoreStatus(filename) {
    if (typeof filename !== 'string' || !path.isAbsolute(filename)) return null;
    let handle;
    try {
        const before = await fs.promises.lstat(filename);
        if (!before.isFile() || before.isSymbolicLink() || before.size > 65536) return null;
        const flags = fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW || 0);
        handle = await fs.promises.open(filename, flags);
        const opened = await handle.stat();
        if (!opened.isFile() || opened.size > 65536 || opened.ino !== before.ino || opened.dev !== before.dev) return null;
        if (process.platform !== 'win32' && (opened.mode & 0o077 || opened.uid !== process.getuid())) return null;
        const buffer = Buffer.alloc(opened.size);
        let offset = 0;
        while (offset < buffer.length) {
            const result = await handle.read(buffer, offset, buffer.length - offset, offset);
            if (!result.bytesRead) return null;
            offset += result.bytesRead;
        }
        const after = await handle.stat();
        if (after.size !== opened.size || after.mtimeMs !== opened.mtimeMs || after.ctimeMs !== opened.ctimeMs) return null;
        const value = JSON.parse(buffer.toString('utf8'));
        if (!value || value.format !== 'cosyncx-restore-status-v1' || value.mock_only !== false
                || !Number.isSafeInteger(value.updated_at_ms) || value.updated_at_ms > Date.now() + 5000
                || Date.now() - value.updated_at_ms > 90000
                || !['TRANSFERRING', 'CHECKING', 'RESTORING', 'PENDING', 'COMPLETE', 'ERROR'].includes(value.state)) return null;
        if (value.missing_path_chats !== undefined && (!Number.isSafeInteger(value.missing_path_chats)
                || value.missing_path_chats < 1 || typeof value.missing_path_report !== 'string'
                || !path.isAbsolute(value.missing_path_report))) return null;
        return value;
    } catch (_) {
        return null;
    } finally {
        if (handle) await handle.close();
    }
}

module.exports = { readRestoreStatus };
