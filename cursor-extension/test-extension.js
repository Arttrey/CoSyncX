const assert = require('assert');
const Module = require('module');
const child = require('child_process');
const originalLoad = Module._load;
const originalExec = child.execFile;
let status;
let command;
let onConfiguration;
const calls = [];
const config = { python: 'python3', script: '/local/nexus_sync.py', shared: '/local/shared', machine: 'linux' };
const disposables = [];
const vscode = {
    StatusBarAlignment: { Left: 1 },
    window: { createStatusBarItem() { status = { show() {}, dispose() {} }; return status; } },
    commands: { registerCommand(_name, callback) { command = callback; return { dispose() {} }; } },
    workspace: {
        getConfiguration() { return { get(key) { return config[key]; } }; },
        onDidChangeConfiguration(callback) { onConfiguration = callback; return { dispose() {} }; }
    }
};
Module._load = function(request, parent, isMain) {
    return request === 'vscode' ? vscode : originalLoad.apply(this, arguments);
};
child.execFile = (program, args, options, callback) => {
    assert.strictEqual(program, 'python3');
    assert(['status', 'summary'].includes(args[1]));
    calls.push({ args, callback });
};
try {
    require('./extension').activate({ subscriptions: disposables });
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_ready', message: 'Доставка не подтверждена'}));
    assert(status.text.includes('снимок'));
    assert(status.tooltip.includes('не подтверждена'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_ready', authenticity_verified: false,
        expected_receivers: 2, confirmed_receivers: 0, message: 'Подлинность не подтверждена'}));
    assert(status.text.includes('ключ ?'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', message: 'Снимок проверен; в Codex не применён'}));
    assert(status.text.includes('снимок проверен'));
    assert(status.tooltip.includes('не применён'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', expected_receivers: 2, confirmed_receivers: 1, message: 'Импорт не подтверждён'}));
    assert(status.text.includes('1/2'));
    assert(status.tooltip.includes('не подтверждён'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_ready', expected_receivers: 2, confirmed_receivers: 0, message: 'Импорт не подтверждён'}));
    assert(status.text.includes('снимок 0/2'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', expected_receivers: 2, confirmed_receivers: 2, message: 'Импорт не подтверждён'}));
    assert(status.text.includes('снимок 2/2'));
    assert(status.tooltip.includes('Импорт не подтверждён'));
    command();
    config.machine = 'mac';
    onConfiguration({ affectsConfiguration: () => true });
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', message: 'Устаревший ответ'}));
    assert(!status.text.includes('получен'));
    const next = calls.shift();
    assert.strictEqual(next.args[next.args.indexOf('--machine') + 1], 'mac');
    next.callback(null, JSON.stringify({state: 'snapshot_ready', message: 'Новая конфигурация'}));
    assert(status.tooltip.includes('Новая конфигурация'));
    assert(status.tooltip.includes('состояние наблюдателя не проверяется'));
    config.config = '/local/config.json';
    onConfiguration({ affectsConfiguration: () => true });
    const configured = calls.shift();
    assert.deepStrictEqual(configured.args.slice(1), ['summary', '--config', '/local/config.json']);
    configured.callback(null, JSON.stringify({state: 'no_snapshot', prepared_records: 2, message: 'Копии подготовлены; в Codex не импортированы'}));
    assert(status.text.includes('копии 2'));
    assert(status.tooltip.includes('не импортированы'));
    Object.assign(config, {mockSyncthingConfig: '/tmp/mock/config.xml', mockSyncthingFolder: '/tmp/mock/folder',
        mockSyncthingId: 'mock-folder', mockSyncthingPort: 12345});
    command();
    const runtime = calls.shift();
    assert(runtime.args.includes('--mock-syncthing-config'));
    assert.strictEqual(runtime.args[runtime.args.indexOf('--mock-syncthing-port') + 1], '12345');
    runtime.callback(null, JSON.stringify({state: 'no_snapshot', message: 'Ожидание', mock_syncthing_runtime:
        {state: 'WAITING', current_peers: 0, expected_peers: 1}}));
    assert(status.text.includes('тест сети'));
    assert(status.tooltip.includes('тестовые узлы 0/1'));
    assert(status.tooltip.includes('импорт чатов') && status.tooltip.includes('не подтверждены'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'no_snapshot', message: 'Ожидание', mock_syncthing_runtime:
        {state: 'READY', current_peers: 1, expected_peers: 1}}));
    assert(status.tooltip.includes('тестовые узлы 1/1'));
    assert(!status.text.includes('синхронизировано'));
    delete config.mockSyncthingConfig;
    delete config.mockSyncthingFolder;
    delete config.mockSyncthingId;
    delete config.mockSyncthingPort;
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_unverified', prepared_records: 2, message: 'Восстановление формата не подтверждено'}));
    assert(status.text.includes('формат'));
    assert(status.tooltip.includes('не подтверждено'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_ready', prepared_records: 1, prepared_skipped: 1, message: 'Пропущен неподдерживаемый чат'}));
    assert(status.text.includes('частично'));
    assert(status.tooltip.includes('Пропущен'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_ready', prepared_records: 1, prepared_incomplete: 1, message: 'Неполный снимок'}));
    assert(status.text.includes('частично'));
    assert(status.tooltip.includes('Неполный'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', observer: {state: 'stale'}, message: 'Давно нет отчёта наблюдателя'}));
    assert(status.text.includes('наблюдатель'));
    assert(status.tooltip.includes('Давно нет отчёта'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', observer: {state: 'error'}, message: 'В последнем цикле были ошибки'}));
    assert(status.text.includes('ошибка'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_ready', observer: {state: 'recent'}, outgoing_package_invalid: 1, message: 'Исходящий пакет не подтверждён'}));
    assert(status.text.includes('пакет ?'));
    assert(status.tooltip.includes('не подтверждён'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_ready', observer: {state: 'recent'}, prepared_secret_invalid: 1, message: 'Неподтверждённая отметка карантина'}));
    assert(status.text.includes('карантин'));
    assert(status.tooltip.includes('Неподтверждённая'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', observer: {state: 'recent'}, index_activity: 'running', message: 'Подготовка копии'}));
    assert(status.text.includes('индекс'));
    assert(status.tooltip.includes('Подготовка'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', observer: {state: 'recent'}, index_activity: 'unknown', message: 'Состояние неизвестно'}));
    assert(status.text.includes('индекс ?'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_ready', observer: {state: 'recent'},
        fingerprint_activity: 'running', message: 'Проверка содержимого историй'}));
    assert(status.text.includes('проверка'));
    assert(status.tooltip.includes('Проверка содержимого'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', observer: {state: 'recent'}, auto_preview_partial: 1, message: 'Есть частичные тестовые копии'}));
    assert(status.text.includes('копии ?'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', observer: {state: 'recent'}, index_activity: 'idle', auto_preview_records: 2, message: 'Только отметки, не импорт'}));
    assert(status.text.includes('тест 2'));
    assert(status.tooltip.includes('не импорт'));
    config.mockPreviewRoot = '/tmp/mock-preview';
    config.mockChatProfile = '/tmp/mock-profile';
    command();
    const mockChat = calls.shift();
    assert.deepStrictEqual(mockChat.args.slice(1), ['summary', '--config', '/local/config.json',
        '--mock-preview-root', '/tmp/mock-preview', '--mock-chat-profile', '/tmp/mock-profile']);
    mockChat.callback(null, JSON.stringify({state: 'snapshot_received', observer: {state: 'recent'},
        mock_chat_watch: {state: 'PENDING'}, message: 'Снимок получен'}));
    assert(status.text.includes('чаты ожидают'));
    assert(status.tooltip.includes('видимость в Cursor не подтверждена'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_received', observer: {state: 'recent'},
        message: 'Ответ без состояния мокового приёмника'}));
    assert(status.text.includes('мок-приёмник ?'));
    config.mockChatProfile = '';
    command();
    const mockReady = calls.shift();
    assert.deepStrictEqual(mockReady.args.slice(1), ['summary', '--config', '/local/config.json', '--mock-preview-root', '/tmp/mock-preview']);
    mockReady.callback(null, JSON.stringify({state: 'no_snapshot', observer: {state: 'recent'},
        mock_chat_preview: {state: 'READY_LOCAL'}, message: 'Локальная отметка'}));
    assert(status.text.includes('мок готов'));
    assert(status.tooltip.includes('индекс, видимость в Cursor и актуальность VPS не подтверждены'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'no_snapshot', mock_chat_preview: {state: 'ATTENTION'}, message: 'Ожидание'}));
    assert(status.text.includes('мок ?'));
    assert(status.tooltip.includes('незавершённая моковая попытка'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'no_snapshot', observer: {state: 'recent'},
        mock_chat_preview: {state: 'OUTDATED'}, message: 'Ожидание'}));
    assert(status.text.includes('мок ожидает'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'invalid', mock_chat_preview: {state: 'READY_LOCAL'}, message: 'Ошибка состояния'}));
    assert(status.text.includes('ошибка'));
    config.syncthingConfig = '/tmp/syncthing-config.xml';
    onConfiguration({ affectsConfiguration: () => true });
    assert(status.text.includes('настройка Syncthing'));
    assert.strictEqual(calls.length, 0);
    config.syncthingProjectFolder = 'projects';
    onConfiguration({ affectsConfiguration: () => true });
    const sourceCheck = calls.shift();
    assert.deepStrictEqual(sourceCheck.args.slice(1), ['summary', '--config', '/local/config.json',
        '--mock-preview-root', '/tmp/mock-preview', '--syncthing-config', '/tmp/syncthing-config.xml',
        '--syncthing-project-folder', 'projects']);
    sourceCheck.callback(null, JSON.stringify({state: 'snapshot_ready', syncthing_source_config: {state: 'ATTENTION'}, message: 'Снимок'}));
    assert(status.text.includes('источник ?'));
    assert(status.tooltip.includes('действующая конфигурация Syncthing не проверена'));
    command();
    calls.shift().callback(null, JSON.stringify({state: 'snapshot_ready', syncthing_source_config: {state: 'CONFIG_MATCH'}, message: 'Снимок'}));
    assert(status.text.includes('снимок'));
    assert(status.tooltip.includes('файл конфигурации источника соответствует Send Only'));
    for (const entry of disposables) entry.dispose();
    process.stdout.write('Cursor status test: OK\n');
} finally {
    Module._load = originalLoad;
    child.execFile = originalExec;
}
