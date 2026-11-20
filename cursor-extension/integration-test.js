const assert = require('assert');
const vscode = require('vscode');

async function run() {
    const extension = vscode.extensions.getExtension('nexus-local.nexus-sync-status');
    assert(extension, `Nexus Sync development extension was not discovered (extensions: ${vscode.extensions.all.map(item => item.id).join(', ')})`);
    await extension.activate();
    const commands = await vscode.commands.getCommands(true);
    assert(commands.includes('nexusSync.refresh'), 'Nexus Sync refresh command was not registered');
    await vscode.commands.executeCommand('nexusSync.refresh');
    if (process.env.NEXUS_CURSOR_TEST_MARKER) {
        require('fs').writeFileSync(process.env.NEXUS_CURSOR_TEST_MARKER, 'OK\n');
    }
    process.stdout.write('Nexus Sync Cursor extension host: OK\n');
}
module.exports = { run };
