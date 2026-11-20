#!/usr/bin/env python3
"""Ask an isolated local Syncthing to scan synthetic files under supplied mock rules."""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET

import mock_chat_sync as mock
import nexus_sync as ns


def validate_inputs(rules: Path, paths: list[str]) -> tuple[bytes, list[str]]:
    rules = mock._mock_path(rules)
    if rules.is_symlink() or not rules.is_file() or rules.stat().st_size > 128 * 1024:
        raise ns.SyncError('Некорректный тестовый файл правил')
    before = rules.stat()
    fd = os.open(rules, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_BINARY', 0))
    with os.fdopen(fd, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise ns.SyncError('Тестовые правила изменились')
        data = stream.read(128 * 1024 + 1)
    after = rules.stat()
    if (len(data) > 128 * 1024 or (before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_ino) !=
            (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino)):
        raise ns.SyncError('Тестовые правила изменились')
    text = data.decode('utf-8')
    if any(line.lstrip('\ufeff \t').startswith('#include') for line in text.splitlines()):
        raise ns.SyncError('Включённые файлы правил в тесте пока не поддерживаются')
    if not 1 <= len(paths) <= 100:
        raise ns.SyncError('Укажите от одного до ста тестовых файлов')
    seen = set()
    for value in paths:
        parts = ns._secret_parts(value, nested=True)
        if any(part in ('.stignore', '.stfolder') or part.startswith('.nexus-probe-') for part in parts):
            raise ns.SyncError('Служебный путь нельзя проверять')
        folded = tuple(unicodedata.normalize('NFD', part.casefold()) for part in parts)
        if any(folded[:len(old)] == old or old[:len(folded)] == folded for old in seen):
            raise ns.SyncError('Повторяющиеся или вложенные тестовые файлы')
        seen.add(folded)
    return data, list(paths)


@contextmanager
def _workspace():
    root = Path(tempfile.mkdtemp(prefix='nexus-ignore-probe-'))
    state = {'process': None}
    try:
        yield root, state
    finally:
        process = state['process']
        if process is None or process.poll() is not None:
            shutil.rmtree(root)


def probe(rules: Path, paths: list[str], *, timeout: float = 30) -> dict:
    if not 3 <= timeout <= 120:
        raise ns.SyncError('Недопустимое время проверки')
    data, paths = validate_inputs(rules, paths)
    executable = shutil.which('syncthing')
    if executable is None:
        raise ns.SyncError('Syncthing не найден')
    with _workspace() as (root, workspace_state):
        home, folder = root / 'home', root / 'folder'
        folder.mkdir(mode=0o700)
        (folder / '.stignore').write_bytes(data)
        for value in paths:
            target = folder.joinpath(*ns._secret_parts(value, nested=True))
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            target.write_bytes(b'synthetic\n')
        control = '.nexus-probe-' + uuid.uuid4().hex
        (folder / control).write_bytes(b'synthetic\n')
        subprocess.run([executable, 'generate', '--home', str(home), '--no-port-probing'],
                       check=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        config = ET.parse(home / 'config.xml')
        document = config.getroot()
        options = document.find('options')
        for name in ('globalAnnounceEnabled', 'localAnnounceEnabled', 'relaysEnabled', 'natEnabled', 'startBrowser'):
            options.find(name).text = 'false'
        options.find('listenAddress').text = 'tcp://127.0.0.1:0'
        options.find('urAccepted').text = '-1'
        ET.SubElement(document, 'folder', {'id': 'mock-probe', 'path': str(folder), 'type': 'sendonly',
                                           'rescanIntervalS': '3600', 'fsWatcherEnabled': 'false'})
        key = document.find('gui').find('apikey').text
        config.write(home / 'config.xml', encoding='utf-8', xml_declaration=True)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        process = subprocess.Popen([executable, 'serve', '--home', str(home), '--no-browser', '--no-restart',
                                    '--no-upgrade', '--gui-address', f'http://127.0.0.1:{port}'],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        workspace_state['process'] = process
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        def request(endpoint: str, params: dict):
            url = f'http://127.0.0.1:{port}/rest/{endpoint}?' + urllib.parse.urlencode(params)
            with opener.open(urllib.request.Request(url, headers={'X-API-Key': key}), timeout=3) as response:
                raw = response.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ns.SyncError('Слишком большой ответ тестового Syncthing')
            return json.loads(raw)
        def lookup(value: str):
            try:
                return request('db/file', {'folder': 'mock-probe', 'file': value})
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    exc.close()
                    return None
                raise
        try:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise ns.SyncError('Тестовый Syncthing завершился')
                try:
                    folder_state = request('db/status', {'folder': 'mock-probe'})
                    errors = request('folder/errors', {'folder': 'mock-probe'})
                    control_record = lookup(control)
                    local = control_record.get('local') if isinstance(control_record, dict) else None
                    if (folder_state.get('state') == 'idle' and folder_state.get('pullErrors') == 0
                            and isinstance(errors, dict) and 'errors' in errors and errors['errors'] in (None, [])
                            and isinstance(local, dict) and local.get('ignored') is False
                            and local.get('invalid') is False and local.get('deleted') is False):
                        break
                except (OSError, ValueError, TypeError, urllib.error.URLError):
                    pass
                time.sleep(0.25)
            else:
                return {'state': 'UNKNOWN', 'mock_only': True, 'reason': 'CONTROL_NOT_INDEXED'}
            results = []
            for value in paths:
                record = lookup(value)
                local = record.get('local') if isinstance(record, dict) else None
                if record is None:
                    file_state = 'IGNORED_IN_MOCK'
                elif (isinstance(local, dict) and local.get('ignored') is False
                      and local.get('invalid') is False and local.get('deleted') is False):
                    file_state = 'INDEXED_IN_MOCK'
                elif isinstance(local, dict) and local.get('ignored') is True:
                    file_state = 'IGNORED_IN_MOCK'
                else:
                    file_state = 'UNKNOWN'
                results.append({'path': value, 'state': file_state})
            final_state = request('db/status', {'folder': 'mock-probe'})
            final_errors = request('folder/errors', {'folder': 'mock-probe'})
            if (final_state.get('state') != 'idle' or final_state.get('pullErrors') != 0
                    or not isinstance(final_errors, dict) or 'errors' not in final_errors
                    or final_errors['errors'] not in (None, [])
                    or final_state.get('sequence') != folder_state.get('sequence')
                    or (folder / '.stignore').read_bytes() != data
                    or any(not folder.joinpath(*ns._secret_parts(value, nested=True)).is_file() for value in paths)):
                return {'state': 'UNKNOWN', 'mock_only': True, 'reason': 'SCAN_NOT_STABLE'}
            return {'state': 'CHECKED', 'mock_only': True, 'real_folder_checked': False, 'files': results}
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=20)


def main() -> int:
    parser = argparse.ArgumentParser(description='Проверить правила только на синтетических файлах в /tmp')
    parser.add_argument('--rules', type=Path, required=True)
    parser.add_argument('--path', action='append', required=True, help='относительный путь синтетического файла')
    parser.add_argument('--timeout', type=float, default=30)
    args = parser.parse_args()
    try:
        result = probe(args.rules, args.path, timeout=args.timeout)
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result['state'] == 'CHECKED' else 1
    except (ns.SyncError, OSError, ValueError, TypeError, ET.ParseError, subprocess.SubprocessError):
        print('Моковая проверка правил отложена', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise System.exit(main())
