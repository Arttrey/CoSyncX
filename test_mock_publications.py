import json
import os
import signal
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

import mock_publications as publications
import nexus_sync as ns


class MockPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='nexus-publications-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.share = self.root / 'share'
        self.share.mkdir()
        self.project = self.root / 'source' / 'Bot'
        self.project.mkdir(parents=True)
        self.secret = self.project / '.env'
        self.key = b'k' * 32
        self.name = 'Bot/.env'

    def package(self, value):
        self.secret.write_text(value)
        return ns.secrets_pack([self.secret], self.share, 'linux', self.key, root=self.project.parent)

    def publish(self, package):
        return publications.publish(self.share, 'linux', 'secret', self.name, package, self.key)

    def select(self, share=None, **kwargs):
        return publications.select(share or self.share, 'linux', 'secret', self.name, self.key, **kwargs)

    def folder(self, share):
        stream = publications._stream('secret', self.name, self.key)
        return share / 'nexus-sync' / 'mock-publications' / 'linux' / stream

    @unittest.skipUnless(shutil.which('git'), 'requires Git')
    def test_publish_all_discovered_git_markers(self):
        publish_all = getattr(publications, 'publish_all_git_markers', None)
        self.assertTrue(callable(publish_all), 'Автоматическая публикация Git не реализована')
        nexus = self.project.parent
        state = self.root / 'state'
        profile = self.root / 'profile'
        state.mkdir()
        profile.mkdir()
        key_file = self.root / 'key'
        key_file.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(profile), 'shared': str(self.share),
            'state_dir': str(state), 'key_file': str(key_file), 'git_discovery': True})

        def create_project(name):
            repo = nexus / name
            repo.mkdir(exist_ok=True)
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            (repo / 'file').write_text('synthetic repository content')
            subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
            subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Mock', '-c',
                            'user.email=mock@example.invalid', 'commit', '-qm', 'initial'], check=True)
            return repo

        create_project('Bot')
        ns.git_cycle(cfg)
        result = publish_all(self.share, state, 'linux', self.key)
        self.assertEqual((result['published'], result['invalid']), (1, 0))
        self.assertIsNotNone(publications.select(self.share, 'linux', 'git', 'Bot', self.key))
        self.assertEqual(publish_all(self.share, state, 'linux', self.key)['published'], 0)
        create_project('Second')
        ns.git_cycle(cfg)
        self.assertEqual(publish_all(self.share, state, 'linux', self.key)['published'], 1)
        self.assertIsNotNone(publications.select(self.share, 'linux', 'git', 'Second', self.key))
        publication_root = self.share / 'nexus-sync' / 'mock-publications' / 'linux'
        for entry in publication_root.glob('*/*.json'):
            self.assertNotIn('Second', entry.read_text())
            self.assertNotIn('synthetic repository content', entry.read_text())
        broken = state / ('last-git-linux-' + '0' * 64 + '.json')
        broken.write_text('{}')
        warning = publish_all(self.share, state, 'linux', self.key)
        self.assertEqual((warning['state'], warning['invalid']), ('ATTENTION', 1))
        self.assertIsNotNone(publications.select(self.share, 'linux', 'git', 'Bot', self.key))
        broken.unlink()
        process = subprocess.Popen([sys.executable, publications.__file__, 'watch-all-git',
            '--shared', str(self.share), '--state', str(state), '--key-file', str(key_file),
            '--source', 'linux', '--interval', '0.1'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            create_project('Third')
            ns.git_cycle(cfg)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                self.assertIsNone(process.poll())
                if publications.select(self.share, 'linux', 'git', 'Third', self.key):
                    break
                time.sleep(0.1)
            else:
                self.fail('Новый Git-пакет не опубликован автоматически')
        finally:
            process.terminate()
            process.wait(timeout=5)

    def test_order_checkpoint_and_no_plaintext(self):
        first = self.package('MOCK_SECRET=first')
        self.assertEqual(self.publish(first)['ordinal'], 1)
        _, first_event = self.select()
        self.assertEqual(self.publish(first)['state'], 'UNCHANGED')
        second = self.package('MOCK_SECRET=second')
        self.assertEqual(self.publish(second)['ordinal'], 2)
        os.utime(first, (2000000000, 2000000000))
        os.utime(second, (1000000000, 1000000000))
        selected, event = self.select(previous_id=first_event['id'],
                                      previous_authentication=first_event['authentication'])
        self.assertEqual(selected, second)
        self.assertEqual(event['ordinal'], 2)
        for entry in self.folder(self.share).iterdir():
            data = entry.read_text()
            self.assertNotIn('MOCK_SECRET', data)
            self.assertNotIn('Bot', data)
            self.assertNotIn('.env', data)
        with self.assertRaises(ns.SyncError):
            self.select(previous_id=first_event['id'], previous_authentication='0' * 64)

    def test_incomplete_chain_and_rollback_are_rejected(self):
        first = self.package('first')
        self.publish(first)
        _, first_event = self.select()
        second = self.package('second')
        self.publish(second)
        _, second_event = self.select()
        target = self.root / 'target'
        target.mkdir()
        target_folder = self.folder(target)
        target_folder.mkdir(parents=True)
        shutil.copyfile(self.folder(self.share) / (second_event['id'] + '.json'),
                        target_folder / (second_event['id'] + '.json'))
        with self.assertRaises(ns.SyncError):
            self.select(target)
        shutil.copyfile(self.folder(self.share) / (first_event['id'] + '.json'),
                        target_folder / (first_event['id'] + '.json'))
        destination = target / second.relative_to(self.share)
        destination.parent.mkdir(parents=True)
        shutil.copyfile(second, destination)
        self.assertEqual(self.select(target)[1]['id'], second_event['id'])
        (target_folder / (second_event['id'] + '.json')).unlink()
        with self.assertRaises(ns.SyncError):
            self.select(target, previous_id=second_event['id'],
                        previous_authentication=second_event['authentication'])

    def test_fork_and_corruption_are_rejected(self):
        first = self.package('first')
        self.publish(first)
        _, first_event = self.select()
        second = self.package('second')
        self.publish(second)
        _, second_event = self.select()
        fork = dict(second_event)
        fork['id'] = uuid.uuid4().hex
        fork['previous'] = first_event['id']
        fork['authentication'] = ns._manifest_tag(fork, self.key)
        fork_file = self.folder(self.share) / (fork['id'] + '.json')
        fork_file.write_text(json.dumps(fork))
        with self.assertRaises(ns.SyncError):
            self.select()
        fork_file.unlink()
        contents = second.read_bytes()
        second.write_bytes(contents[:-1] + bytes([contents[-1] ^ 1]))
        with self.assertRaises((ns.SyncError, ValueError)):
            self.select()

    def test_watcher_publishes_changed_authenticated_markers(self):
        state = self.root / 'state'
        state.mkdir()
        key_file = self.root / 'key'
        key_file.write_bytes(self.key)
        marker_file = state / 'last-secrets-linux.json'

        def write_marker(package):
            marker = {'format': 'nexus-secrets-marker-v1', 'fingerprint': 'synthetic', 'package': package.name}
            marker['authentication'] = ns._manifest_tag(marker, self.key)
            marker_file.write_text(json.dumps(marker))

        first = self.package('first')
        write_marker(first)
        process = subprocess.Popen([sys.executable, publications.__file__, 'watch-marker', '--shared', str(self.share),
                                    '--state', str(state), '--key-file', str(key_file), '--source', 'linux',
                                    '--kind', 'secret', '--name', self.name, '--interval', '0.1'],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(lambda: process.kill() if process.poll() is None else None)

        def wait_for(ordinal):
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    self.fail('Наблюдатель завершился')
                try:
                    result = self.select()
                    if result and result[1]['ordinal'] == ordinal:
                        return result[0]
                except (OSError, ValueError, ns.SyncError):
                    pass
                time.sleep(0.05)
            self.fail('Публикация не появилась')

        self.assertEqual(wait_for(1), first)
        second = self.package('second')
        write_marker(second)
        self.assertEqual(wait_for(2), second)
        process.terminate()
        process.wait(timeout=5)
        marker_file.write_text('{"format":"nexus-secrets-marker-v1"}')
        with self.assertRaises(ns.SyncError):
            publications.publish_marker(self.share, state, 'linux', 'secret', self.name, self.key)

    def test_publish_all_discovers_package_files_without_exposing_names(self):
        state = self.root / 'state-all'
        state.mkdir()
        key_file = self.root / 'key-all'
        key_file.write_bytes(self.key)
        npm = self.project / '.npmrc'
        self.secret.write_text('MOCK_ENV=first')
        npm.write_text('MOCK_NPM=first')
        package = ns.secrets_pack([self.secret, npm], self.share, 'linux', self.key, root=self.project.parent)
        marker_file = state / 'last-secrets-linux.json'
        def write_marker(path):
            marker = {'format': 'nexus-secrets-marker-v1', 'fingerprint': 'synthetic', 'package': path.name}
            marker['authentication'] = ns._manifest_tag(marker, self.key)
            marker_file.write_text(json.dumps(marker))
        write_marker(package)
        command = [sys.executable, publications.__file__, 'publish-all-secrets', '--shared', str(self.share),
                   '--state', str(state), '--key-file', str(key_file), '--source', 'linux']
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['published_count'], 2)
        self.assertNotIn('MOCK_', result.stdout + result.stderr)
        self.assertNotIn('.env', result.stdout + result.stderr)
        for relative in ('Bot/.env', 'Bot/.npmrc'):
            self.assertEqual(publications.select(self.share, 'linux', 'secret', relative, self.key)[1]['ordinal'], 1)
        self.assertEqual(publications.publish_all_secret_marker(self.share, state, 'linux', self.key)['state'], 'UNCHANGED')
        npm.write_text('MOCK_NPM=updated')
        added = self.project / '.pypirc'
        added.write_text('MOCK_PYPI=new')
        newer = ns.secrets_pack([self.secret, npm, added], self.share, 'linux', self.key, root=self.project.parent)
        write_marker(newer)
        updated = publications.publish_all_secret_marker(self.share, state, 'linux', self.key)
        self.assertEqual((updated['published_count'], updated['file_count']), (3, 3))
        self.assertEqual(publications.select(self.share, 'linux', 'secret', 'Bot/.npmrc', self.key)[1]['ordinal'], 2)
        self.assertEqual(publications.select(self.share, 'linux', 'secret', 'Bot/.pypirc', self.key)[1]['ordinal'], 1)
        marker_file.write_text('{}')
        with self.assertRaises(ns.SyncError):
            publications.publish_all_secret_marker(self.share, state, 'linux', self.key)

    def test_watch_detects_last_file_missing_in_disposable_root(self):
        state = self.root / 'watch-empty-state'
        state.mkdir()
        key_file = self.root / 'watch-empty-key'
        key_file.write_bytes(self.key)
        package = self.package('MOCK_SECRET=synthetic')
        marker = {'format': 'nexus-secrets-marker-v1', 'fingerprint': 'synthetic', 'package': package.name}
        marker['authentication'] = ns._manifest_tag(marker, self.key)
        (state / 'last-secrets-linux.json').write_text(json.dumps(marker))
        process = subprocess.Popen([sys.executable, publications.__file__, 'watch-all-secrets',
            '--shared', str(self.share), '--state', str(state), '--key-file', str(key_file),
            '--source', 'linux', '--mock-nexus', str(self.project.parent), '--interval', '0.1'],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        def wait_for(empty):
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                self.assertIsNone(process.poll())
                try:
                    selected = publications.select(self.share, 'linux', 'secret-set', 'all', self.key)
                    if selected and (selected[1].get('empty') is True) == empty:
                        return
                except (OSError, ValueError, ns.SyncError):
                    pass
                time.sleep(0.05)
            self.fail('Тестовый набор не появился')
        try:
            wait_for(False)
            self.secret.unlink()
            wait_for(True)
            time.sleep(0.3)
            self.assertTrue(publications.select(self.share, 'linux', 'secret-set', 'all', self.key)[1]['empty'])
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            process.wait(timeout=10)


if __name__ == '__main__':
    unittest.main()
