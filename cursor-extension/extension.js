const vscode = require('vscode');
const { execFile } = require('child_process');
const { readRestoreStatus } = require('./restore-status');

// A transfer or a preview is not a restoration. Only a completed, indexed
// restore may request a reload, and each window handles an event at most once.
function makeRestoreReloader(vscode, workspaceState) {
    let pending = false;
    return async function maybeReload(event, enabled) {
        if (!enabled || pending || !workspaceState || !event || event.mock_only !== false || event.state !== 'RESTORED'
                || event.indexed !== true || !Number.isSafeInteger(event.changed_chats)
                || event.changed_chats < 1 || typeof event.id !== 'string'
                || !/^[0-9a-f]{32}$/.test(event.id)) return false;
        const key = 'cosyncx.lastRestoredEvent';
        if (workspaceState.get(key) === event.id) return false;
        pending = true;
        const previous = workspaceState.get(key);
        try {
            await workspaceState.update(key, event.id);
            await vscode.commands.executeCommand('workbench.action.reloadWindow');
            return true;
        } catch (error) {
            try { await workspaceState.update(key, previous); } catch (_) { /* retry on next start */ }
            throw error;
        } finally {
            pending = false;
        }
    };
}

function activate(context) {
    const maybeReload = makeRestoreReloader(vscode, context.workspaceState);
    const item = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 30);
    item.command = 'nexusSync.refresh';
    item.text = '$(sync) Nexus: настройка';
    item.tooltip = 'Укажите настройки Nexus Sync на этом устройстве.';
    item.show();
    let generation = 0;
    let running = false;
    let pending = false;
    let disposed = false;
    function refreshRestoreStatus(filename, cfg, current) {
        readRestoreStatus(filename).then(status => {
            if (disposed || current !== generation) return;
            if (!status) {
                item.text = '$(warning) CoSyncX: нет статуса';
                item.tooltip = 'Не удалось проверить локальный статус восстановления CoSyncX.';
                return;
            }
            const labels = { TRANSFERRING: '$(sync~spin) CoSyncX: передача',
                RESTORING: '$(sync~spin) CoSyncX: восстановление', PENDING: '$(warning) CoSyncX: ожидание',
                COMPLETE: status.restore_event ? '$(check) CoSyncX: восстановлено' : '$(check) CoSyncX: передача завершена',
                ERROR: '$(warning) CoSyncX: ошибка' };
            item.text = labels[status.state];
            item.tooltip = 'Локальный статус CoSyncX';
            if (status.missing_path_chats) {
                if (status.state === 'COMPLETE') item.text = '$(warning) CoSyncX: проверьте пути';
                item.tooltip = `В ${status.missing_path_chats} чатах есть недоступные на этом устройстве пути. Отчёт: ${status.missing_path_report}`;
            }
            // An earlier successful restore may still need a reload even when
            // a newer transfer is already pending or in progress.
            maybeReload(status.restore_event, cfg.get('autoReloadAfterRestore') === true).catch(() => {});
        }).catch(() => {});
    }
    function refresh() {
        if (disposed) return;
        if (running) {
            generation++;
            pending = true;
            return;
        }
        const cfg = vscode.workspace.getConfiguration('nexusSync');
        const python = cfg.get('python');
        const script = cfg.get('script');
        const configFile = cfg.get('config');
        const shared = cfg.get('shared');
        const machine = cfg.get('machine');
        const keyFile = cfg.get('keyFile');
        const mockPreviewRoot = cfg.get('mockPreviewRoot');
        const mockChatProfile = cfg.get('mockChatProfile');
        const mockSyncthingConfig = cfg.get('mockSyncthingConfig');
        const mockSyncthingFolder = cfg.get('mockSyncthingFolder');
        const mockSyncthingId = cfg.get('mockSyncthingId');
        const mockSyncthingPort = cfg.get('mockSyncthingPort');
        const restoreStatusFile = cfg.get('restoreStatusFile');
        if (restoreStatusFile) {
            refreshRestoreStatus(restoreStatusFile, cfg, ++generation);
            return;
        }
        if (!python || !script || (!configFile && (!shared || !machine))) {
            item.text = '$(sync) Nexus: настройка';
            item.tooltip = 'Укажите путь к программе, папку обмена и ID устройства в настройках Cursor.';
            return;
        }
        if (mockChatProfile && !configFile) {
            item.text = '$(warning) Nexus: настройка теста';
            item.tooltip = 'Для проверки мокового приёмника укажите конфигурацию Nexus Sync.';
            return;
        }
        const mockSyncthingSettings = [mockSyncthingConfig, mockSyncthingFolder, mockSyncthingId, mockSyncthingPort];
        const mockSyncthingEnabled = mockSyncthingSettings.some(Boolean);
        if (mockSyncthingEnabled && (!configFile || !mockSyncthingSettings.every(Boolean)
                || !Number.isInteger(mockSyncthingPort) || mockSyncthingPort < 1 || mockSyncthingPort > 65535)) {
            item.text = '$(warning) Nexus: настройка теста';
            item.tooltip = 'Укажите все параметры тестового Syncthing и конфигурацию Nexus Sync.';
            return;
        }
        running = true;
        const current = ++generation;
        const args = configFile ? [script, 'summary', '--config', configFile] : [script, 'status', '--shared', shared, '--machine', machine];
        if (configFile && mockPreviewRoot) args.push('--mock-preview-root', mockPreviewRoot);
        if (configFile && mockChatProfile) args.push('--mock-chat-profile', mockChatProfile);
        if (mockSyncthingEnabled) args.push('--mock-syncthing-config', mockSyncthingConfig,
            '--mock-syncthing-folder', mockSyncthingFolder, '--mock-syncthing-id', mockSyncthingId,
            '--mock-syncthing-port', String(mockSyncthingPort));
        if (!configFile && keyFile) args.push('--key-file', keyFile);
        execFile(python, args, { timeout: 15000, maxBuffer: 65536 }, (error, stdout) => {
            running = false;
            if (disposed) return;
            if (current !== generation) {
                if (pending) {
                    pending = false;
                    refresh();
                }
                return;
            }
            if (error) {
                item.text = '$(warning) Nexus: недоступно';
                item.tooltip = 'Не удалось проверить локальную папку обмена.';
                return;
            }
            try {
                const data = JSON.parse(stdout);
                item.text = data.state === 'snapshot_received' ? '$(cloud) Nexus: снимок проверен' : data.state === 'snapshot_ready' ? '$(cloud) Nexus: снимок' : data.state === 'snapshot_unverified' ? '$(warning) Nexus: формат' : data.state === 'invalid' ? '$(warning) Nexus: ошибка' : '$(sync) Nexus: нет снимка';
                if (data.state === 'snapshot_ready' && data.authenticity_verified === false) {
                    item.text = '$(warning) Nexus: ключ ?';
                } else if (['snapshot_ready', 'snapshot_received'].includes(data.state) && Number.isSafeInteger(data.expected_receivers) && data.expected_receivers > 0 && Number.isSafeInteger(data.confirmed_receivers) && data.confirmed_receivers >= 0 && data.confirmed_receivers <= data.expected_receivers) {
                    item.text = `$(cloud) Nexus: снимок ${data.confirmed_receivers}/${data.expected_receivers}`;
                }
                const mockPreview = data.mock_chat_preview;
                // The receiver must publish this only after an actual indexed
                // restore. Ordinary Syncthing status never triggers a reload.
                maybeReload(data.restore_event, cfg.get('autoReloadAfterRestore') === true).catch(() => {});
                const mockChat = data.mock_chat_watch;
                const mockSyncthing = data.mock_syncthing_runtime;
                if (mockSyncthing && mockSyncthing.state !== 'READY') {
                    item.text = '$(warning) Nexus: тест сети';
                } else if (mockPreview && ['ATTENTION', 'INVALID'].includes(mockPreview.state)) {
                    item.text = '$(warning) Nexus: мок ?';
                } else if (mockChat && mockChat.state === 'PENDING') {
                    item.text = '$(warning) Nexus: чаты ожидают';
                } else if ((configFile && mockChatProfile && !mockChat) || (mockChat && mockChat.state !== 'COMPLETE')) {
                    item.text = '$(warning) Nexus: мок-приёмник ?';
                } else if (data.state !== 'invalid' && data.observer && data.observer.state !== 'recent') {
                    item.text = data.observer.state === 'error' ? '$(warning) Nexus: ошибка' : '$(warning) Nexus: наблюдатель';
                } else if (data.state !== 'invalid' && ((Number.isSafeInteger(data.auto_preview_invalid) && data.auto_preview_invalid > 0) || (Number.isSafeInteger(data.auto_preview_partial) && data.auto_preview_partial > 0))) {
                    item.text = '$(warning) Nexus: копии ?';
                } else if (data.state !== 'invalid' && data.state !== 'snapshot_unverified' && ((Number.isSafeInteger(data.prepared_skipped) && data.prepared_skipped > 0) || (Number.isSafeInteger(data.prepared_incomplete) && data.prepared_incomplete > 0))) {
                    item.text = '$(warning) Nexus: частично';
                } else if (data.state !== 'invalid' && data.state !== 'snapshot_unverified' && data.index_activity === 'running') {
                    item.text = '$(sync~spin) Nexus: индекс';
                } else if (data.state !== 'invalid' && data.state !== 'snapshot_unverified' && data.index_activity === 'unknown') {
                    item.text = '$(warning) Nexus: индекс ?';
                } else if (data.state !== 'invalid' && data.state !== 'snapshot_unverified' && data.fingerprint_activity === 'running') {
                    item.text = '$(sync~spin) Nexus: проверка';
                } else if (data.state !== 'invalid' && data.state !== 'snapshot_unverified' && Number.isSafeInteger(data.auto_preview_records) && data.auto_preview_records > 0) {
                    item.text = `$(inbox) Nexus: тест ${data.auto_preview_records}`;
                } else if (data.state !== 'invalid' && data.state !== 'snapshot_unverified' && Number.isSafeInteger(data.prepared_records) && data.prepared_records > 0) {
                    item.text = `$(inbox) Nexus: копии ${data.prepared_records}`;
                } else if (data.state !== 'invalid' && data.state !== 'snapshot_unverified' && mockPreview && mockPreview.state === 'OUTDATED') {
                    item.text = '$(sync) Nexus: мок ожидает';
                } else if (data.state !== 'invalid' && data.state !== 'snapshot_unverified' && mockPreview && mockPreview.state === 'READY_LOCAL') {
                    item.text = '$(inbox) Nexus: мок готов';
                }
                item.tooltip = data.message + (configFile ? '' : '; состояние наблюдателя не проверяется');
                if (mockPreview) {
                    const labels = { READY_LOCAL: 'отметка мокового профиля подтверждена', OUTDATED: 'доступен новый моковый снимок', NOT_PREPARED: 'моковая копия ещё не подготовлена', ATTENTION: 'незавершённая моковая попытка', INVALID: 'моковая отметка не подтверждена' };
                    item.tooltip += `; ${labels[mockPreview.state] || 'состояние моковой копии неизвестно'}; индекс, видимость в Cursor и актуальность VPS не подтверждены`;
                }
                if (mockChat) {
                    const labels = { COMPLETE: 'последний доступный моковый снимок обработан', PENDING: 'моковые чаты ожидают обработки', STOPPED: 'моковый приёмник остановлен', STALE: 'отметка мокового приёмника устарела', INVALID: 'отметка мокового приёмника не подтверждена' };
                    item.tooltip += `; ${labels[mockChat.state] || 'состояние мокового приёмника неизвестно'}; видимость в Cursor не подтверждена; актуальность VPS не подтверждена`;
                }
                if (mockSyncthing) {
                    const counts = [mockSyncthing.current_peers, mockSyncthing.expected_peers];
                    const suffix = counts.every(value => Number.isSafeInteger(value) && value >= 0)
                        ? `; тестовые узлы ${counts[0]}/${counts[1]}` : '';
                    item.tooltip += `; тестовый Syncthing: ${mockSyncthing.state}${suffix}; импорт чатов и актуальность VPS не подтверждены`;
                }
            } catch (_) {
                item.text = '$(warning) Nexus: ошибка';
                item.tooltip = 'Ответ программы не распознан.';
            }
        });
    }
    context.subscriptions.push(item, vscode.commands.registerCommand('nexusSync.refresh', refresh), vscode.workspace.onDidChangeConfiguration(e => { if (e.affectsConfiguration('nexusSync')) refresh(); }));
    const timer = setInterval(() => {
        if (!vscode.workspace.getConfiguration('nexusSync').get('restoreStatusFile')) refresh();
    }, 60000);
    const restoreTimer = setInterval(() => {
        if (vscode.workspace.getConfiguration('nexusSync').get('restoreStatusFile')) refresh();
    }, 5000);
    context.subscriptions.push({ dispose: () => { disposed = true; generation++; clearInterval(timer); clearInterval(restoreTimer); } });
    refresh();
}
exports.activate = activate;
exports.makeRestoreReloader = makeRestoreReloader;
