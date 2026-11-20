import json
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import mock_auto_receiver as receiver
import mock_git_sync
import mock_publications
import nexus_sync as ns


@unittest.skipUnless(sys.platform == 'linux' and shutil.which('git'), 'requires Linux and Git')
class MockAutomaticReceiverTests(unittest.TestCase):
    def test_discovered_git_accepts_two_sources_without_replacing_head(self):
        with tempfile.TemporaryDirectory(prefix='nexus-git-two-sources-') as tmp:
            root = Path(tmp)
            linux, mac, shared, target = (root / name for name in ('linux', 'mac', 'shared', 'target'))
            linux.mkdir()
            mac.mkdir()
            shared.mkdir()
            key = b'm' * 32
            mock_git_sync.init_workspace(target, shared, key)

            def git(repo, *args):
                return subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Mock', '-c',
                    'user.email=mock@example.invalid', *args], check=True, capture_output=True, text=True).stdout.strip()

            first = linux / 'App'
            first.mkdir()
            git(first, 'init', '-q')
            (first / 'code.txt').write_text('synthetic\n')
            git(first, 'add', 'code.txt')
            git(first, 'commit', '-qm', 'initial')
            package = ns.git_pack(first, shared, 'linux', key, nexus=linux)
            mock_publications.publish(shared, 'linux', 'git', 'App', package, key)
            project = target / 'App'
            project.mkdir()
            (project / 'code.txt').write_text('synthetic\n')
            self.assertEqual(receiver.receive_discovered_git(target, shared, key, 'linux')['state'], 'APPLIED_AVAILABLE')
            original_head = git(project, 'rev-parse', 'HEAD')
            subprocess.run(['git', 'clone', '-q', str(first), str(mac / 'App')], check=True)
            second = mac / 'App'
            git(second, 'commit', '--allow-empty', '-qm', 'mac update')
            mac_head = git(second, 'rev-parse', 'HEAD')
            mac_package = ns.git_pack(second, shared, 'mac', key, nexus=mac)
            mock_publications.publish(shared, 'mac', 'git', 'App', mac_package, key)
            self.assertEqual(receiver.receive_discovered_git(target, shared, key, 'mac')['state'], 'APPLIED_AVAILABLE')
            self.assertEqual(git(project, 'rev-parse', 'HEAD'), original_head)
            self.assertIn(mac_head, git(project, 'for-each-ref', '--format=%(objectname)',
                                        'refs/nexus-sync/mac').splitlines())
            git(first, 'commit', '--allow-empty', '-qm', 'linux update')
            linux_head = git(first, 'rev-parse', 'HEAD')
            updated = ns.git_pack(first, shared, 'linux', key, nexus=linux)
            mock_publications.publish(shared, 'linux', 'git', 'App', updated, key)
            self.assertEqual(receiver.receive_discovered_git(target, shared, key, 'linux')['state'], 'APPLIED_AVAILABLE')
            self.assertIn(linux_head, git(project, 'for-each-ref', '--format=%(objectname)',
                                          'refs/nexus-sync/linux').splitlines())
            self.assertEqual(receiver.receive_discovered_git(target, shared, key, 'mac')['state'], 'APPLIED_AVAILABLE')
            self.assertEqual(git(project, 'rev-parse', 'HEAD'), original_head)
            key_file = root / 'key'
            key_file.write_bytes(key)
            cli = subprocess.run([sys.executable, str(Path(receiver.__file__)), '--root', str(target),
                '--shared', str(shared), '--key-file', str(key_file), '--source', 'linux', '--also-source', 'mac',
                '--discover-git'], capture_output=True, text=True, timeout=10)
            self.assertEqual(cli.returncode, 0, cli.stderr)
            self.assertEqual(json.loads(cli.stdout)['git_sources_ready'], 2)
            self.assertNotIn('App', cli.stdout + cli.stderr)
            mac_publications = shared / 'nexus-sync' / 'mock-publications' / 'mac'
            hidden = root / 'temporarily-missing-publications'
            mac_publications.rename(hidden)
            try:
                missing = subprocess.run([sys.executable, str(Path(receiver.__file__)), '--root', str(target),
                    '--shared', str(shared), '--key-file', str(key_file), '--source', 'linux', '--also-source', 'mac',
                    '--discover-git'], capture_output=True, text=True, timeout=10)
                self.assertEqual(missing.returncode, 1)
                self.assertEqual(json.loads(missing.stdout)['git_sources_ready'], 1)
                self.assertEqual(git(project, 'rev-parse', 'HEAD'), original_head)
            finally:
                hidden.rename(mac_publications)
            git(project, 'commit', '--allow-empty', '-qm', 'local edit')
            local_head = git(project, 'rev-parse', 'HEAD')
            git(second, 'commit', '--allow-empty', '-qm', 'another mac update')
            mac_package = ns.git_pack(second, shared, 'mac', key, nexus=mac)
            mock_publications.publish(shared, 'mac', 'git', 'App', mac_package, key)
            self.assertEqual(receiver.receive_discovered_git(target, shared, key, 'mac')['state'], 'PENDING')
            self.assertEqual(git(project, 'rev-parse', 'HEAD'), local_head)

    def test_discovered_git_preserves_existing_and_tracks_updates(self):
        receive_all = getattr(receiver, 'receive_discovered_git', None)
        self.assertTrue(callable(receive_all), 'Автоматический приём Git не реализован')
        with tempfile.TemporaryDirectory(prefix='nexus-discovered-git-test-') as tmp:
            root = Path(tmp)
            source, shared, target = root / 'source', root / 'shared', root / 'target'
            source.mkdir()
            shared.mkdir()
            key = b'g' * 32
            mock_git_sync.init_workspace(target, shared, key)

            def git(repo, *args):
                return subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Mock', '-c',
                    'user.email=mock@example.invalid', *args], check=True, capture_output=True, text=True).stdout.strip()

            def create(name):
                repo = source / name
                repo.mkdir()
                git(repo, 'init', '-q')
                (repo / 'source.txt').write_text('synthetic source\n')
                git(repo, 'add', 'source.txt')
                git(repo, 'commit', '-qm', 'initial')
                package = ns.git_pack(repo, shared, 'linux', key, nexus=source)
                mock_publications.publish(shared, 'linux', 'git', name, package, key)
                return repo

            first = create('First')
            create('Second')
            for name in ('First', 'Second'):
                project = target / name
                project.mkdir()
                (project / 'source.txt').write_text('synthetic source\n')
            result = receive_all(target, shared, key, 'linux')
            self.assertEqual((result['state'], result['git_ready_count']), ('APPLIED_AVAILABLE', 2))
            self.assertNotIn('First', json.dumps(result))
            self.assertTrue((target / 'First' / '.git').is_dir())
            self.assertTrue((target / 'Second' / '.git').is_dir())
            old_head = git(target / 'First', 'rev-parse', 'HEAD')
            git(first, 'commit', '--allow-empty', '-qm', 'update')
            new_head = git(first, 'rev-parse', 'HEAD')
            package = ns.git_pack(first, shared, 'linux', key, nexus=source)
            mock_publications.publish(shared, 'linux', 'git', 'First', package, key)
            updated = receive_all(target, shared, key, 'linux')
            self.assertEqual(updated['state'], 'APPLIED_AVAILABLE')
            self.assertEqual(git(target / 'First', 'rev-parse', 'HEAD'), old_head)
            self.assertIn(new_head, git(target / 'First', 'for-each-ref', '--format=%(objectname)',
                                        'refs/nexus-sync/linux').splitlines())
            create('Third')
            missing = receive_all(target, shared, key, 'linux')
            self.assertEqual((missing['state'], missing['git_pending_count']), ('PENDING', 1))
            (target / 'Third').mkdir()
            (target / 'Third' / 'source.txt').write_text('synthetic source\n')
            third = receive_all(target, shared, key, 'linux')
            self.assertEqual((third['state'], third['git_ready_count']), ('APPLIED_AVAILABLE', 3))
            key_file = root / 'key'
            key_file.write_bytes(key)
            command = [sys.executable, str(Path(receiver.__file__)), '--root', str(target), '--shared', str(shared),
                       '--key-file', str(key_file), '--source', 'linux', '--discover-git']
            cli = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(cli.returncode, 0, cli.stderr)
            self.assertEqual(json.loads(cli.stdout)['git_ready_count'], 3)
            self.assertNotIn('Third', cli.stdout + cli.stderr)
            create('Existing')
            existing = target / 'Existing'
            existing.mkdir()
            git(existing, 'init', '-q')
            conflict = receive_all(target, shared, key, 'linux')
            self.assertEqual((conflict['state'], conflict['git_pending_count']), ('PENDING', 1))
            cli = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(cli.returncode, 1)
            self.assertEqual(json.loads(cli.stdout)['git_pending_count'], 1)
            self.assertTrue((existing / '.git').is_dir())
            _, publication = mock_publications.select(shared, 'linux', 'git', 'First', key)
            stream = mock_publications._stream('git', 'First', key)
            (shared / 'nexus-sync' / 'mock-publications' / 'linux' / stream /
             (publication['id'] + '.json')).unlink()
            rollback = receive_all(target, shared, key, 'linux')
            self.assertEqual(rollback['state'], 'PENDING')
            self.assertGreaterEqual(rollback['git_pending_count'], 2)
            self.assertEqual(git(target / 'First', 'rev-parse', 'HEAD'), old_head)
            shutil.rmtree(shared / 'nexus-sync' / 'mock-publications' / 'linux' / stream)
            missing_stream = receive_all(target, shared, key, 'linux')
            self.assertEqual(missing_stream['state'], 'PENDING')
            self.assertGreaterEqual(missing_stream['git_pending_count'], 2)

    def test_discovered_secrets_from_multiple_sources_do_not_overwrite_conflicts(self):
        with tempfile.TemporaryDirectory(prefix='nexus-multi-secret-test-') as tmp:
            root = Path(tmp)
            shared, state = root / 'shared', root / 'state'
            shared.mkdir()
            state.mkdir()
            target = root / 'target'
            key = b'm' * 32
            key_file = root / 'key'
            key_file.write_bytes(key)
            mock_git_sync.init_workspace(target, shared, key)
            sources = {name: root / name for name in ('linux', 'windows')}
            for folder in sources.values():
                folder.mkdir()
                (folder / '.env').write_text('SYNTHETIC=shared\n')
            def publish(name):
                folder = sources[name]
                package = ns.secrets_pack([folder / '.env'], shared, name, key, root=folder)
                marker = {'format': 'nexus-secrets-marker-v1', 'fingerprint': 'synthetic', 'package': package.name}
                marker['authentication'] = ns._manifest_tag(marker, key)
                (state / f'last-secrets-{name}.json').write_text(json.dumps(marker))
                mock_publications.publish_all_secret_marker(shared, state, name, key)
            for name in sources:
                publish(name)
            command = [sys.executable, str(Path(receiver.__file__)), '--root', str(target), '--shared', str(shared),
                '--key-file', str(key_file), '--source', 'linux', '--also-source', 'windows', '--secrets-only',
                '--discover-secrets', '--timeout', '1', '--interval', '0.1']
            first = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(first.returncode, 0, first.stderr)
            self.assertEqual(json.loads(first.stdout)['secret_sources_ready'], 2)
            self.assertEqual((target / '.env').read_text(), 'SYNTHETIC=shared\n')
            self.assertNotIn('SYNTHETIC=', first.stdout + first.stderr)
            (sources['windows'] / '.env').write_text('SYNTHETIC=changed\n')
            publish('windows')
            changed = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(changed.returncode, 0)
            self.assertEqual(json.loads(changed.stdout)['state'], 'PENDING')
            self.assertEqual((target / '.env').read_text(), 'SYNTHETIC=changed\n')
            pending = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(pending.returncode, 0)
            self.assertEqual(json.loads(pending.stdout)['state'], 'PENDING')
            self.assertEqual((target / '.env').read_text(), 'SYNTHETIC=changed\n')
            duplicate = subprocess.run(command + ['--also-source', 'windows'], capture_output=True, text=True, timeout=10)
            self.assertNotEqual(duplicate.returncode, 0)
            missing = subprocess.run(command + ['--also-source', 'mac'], capture_output=True, text=True, timeout=10)
            self.assertNotEqual(missing.returncode, 0)
            missing_result = json.loads(missing.stdout)
            self.assertEqual(missing_result['state'], 'PENDING')
            self.assertEqual(missing_result['secret_sources_expected'], 3)
            self.assertNotIn('secret_expected_count', missing_result)

    def test_automatic_secret_set_accepts_nexus_root_and_project_files(self):
        with tempfile.TemporaryDirectory(prefix='nexus-root-secret-test-') as tmp:
            root = Path(tmp)
            shared, source, state = root / 'share', root / 'source', root / 'state'
            shared.mkdir()
            state.mkdir()
            (source / 'Bot').mkdir(parents=True)
            (source / '.env').write_text('SYNTHETIC_ROOT=one\n')
            (source / 'Bot' / '.npmrc').write_text('SYNTHETIC_PROJECT=one\n')
            key = b's' * 32
            target = root / 'target'
            mock_git_sync.init_workspace(target, shared, key)
            (target / 'Bot').mkdir()
            package = ns.secrets_pack([source / '.env', source / 'Bot' / '.npmrc'], shared, 'linux', key, root=source)
            marker = {'format': 'nexus-secrets-marker-v1', 'fingerprint': 'synthetic', 'package': package.name}
            marker['authentication'] = ns._manifest_tag(marker, key)
            (state / 'last-secrets-linux.json').write_text(json.dumps(marker))
            published = mock_publications.publish_all_secret_marker(shared, state, 'linux', key)
            self.assertEqual(published['file_count'], 2)
            result = receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)
            self.assertEqual((result['state'], result['secret_ready_count']), ('APPLIED_AVAILABLE', 2))
            self.assertEqual((target / '.env').read_text(), 'SYNTHETIC_ROOT=one\n')
            self.assertEqual((target / 'Bot' / '.npmrc').read_text(), 'SYNTHETIC_PROJECT=one\n')
            self.assertNotIn('SYNTHETIC_', json.dumps(result))
            (target / '.env').write_text('SYNTHETIC_LOCAL=keep\n')
            (source / '.env').write_text('SYNTHETIC_REMOTE=two\n')
            package = ns.secrets_pack([source / '.env', source / 'Bot' / '.npmrc'], shared, 'linux', key, root=source)
            marker['package'] = package.name
            marker['authentication'] = ns._manifest_tag(marker, key)
            (state / 'last-secrets-linux.json').write_text(json.dumps(marker))
            mock_publications.publish_all_secret_marker(shared, state, 'linux', key)
            pending = receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)
            self.assertEqual(pending['state'], 'PENDING')
            self.assertEqual((target / '.env').read_text(), 'SYNTHETIC_LOCAL=keep\n')

    def test_automatic_secret_set_detects_new_and_missing_without_deletion(self):
        with tempfile.TemporaryDirectory(prefix='nexus-discovered-secret-test-') as tmp:
            root = Path(tmp)
            shared, source, state = root / 'share', root / 'source', root / 'state'
            shared.mkdir()
            state.mkdir()
            (source / 'Plain').mkdir(parents=True)
            key = b's' * 32
            names = ['Plain/.env', 'Plain/.npmrc']
            for index, name in enumerate(names):
                (source / name).write_text(f'MOCK_VALUE={index}\n')
            target = root / 'target'
            mock_git_sync.init_workspace(target, shared, key)
            (target / 'Plain').mkdir()
            def make_package(paths):
                package = ns.secrets_pack([source / name for name in paths], shared, 'linux', key, root=source)
                marker = {'format': 'nexus-secrets-marker-v1', 'fingerprint': 'synthetic', 'package': package.name}
                marker['authentication'] = ns._manifest_tag(marker, key)
                (state / 'last-secrets-linux.json').write_text(json.dumps(marker))
                return package
            package = make_package(names)
            mock_publications.publish(shared, 'linux', 'secret-set', 'all', package, key)
            self.assertEqual(receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)['state'], 'PENDING')
            self.assertFalse((target / names[0]).exists())
            mock_publications.publish_all_secret_marker(shared, state, 'linux', key)
            first = receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)
            self.assertEqual((first['state'], first['secret_ready_count'], first['missing_count']), ('APPLIED_AVAILABLE', 2, 0))
            self.assertNotIn('Plain', json.dumps(first))
            key_file = root / 'key'
            key_file.write_bytes(key)
            run = subprocess.run([sys.executable, str(Path(receiver.__file__)), '--root', str(target),
                '--shared', str(shared), '--key-file', str(key_file), '--source', 'linux', '--secrets-only',
                '--discover-secrets', '--timeout', '1', '--interval', '0.1'], capture_output=True, text=True, timeout=10)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertNotIn('MOCK_VALUE', run.stdout + run.stderr)
            self.assertEqual(json.loads(run.stdout)['secret_ready_count'], 2)
            (target / names[1]).write_text('MOCK_VALUE=local\n')
            (source / names[1]).write_text('MOCK_VALUE=remote\n')
            make_package(names)
            mock_publications.publish_all_secret_marker(shared, state, 'linux', key)
            conflict = receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)
            self.assertEqual((conflict['state'], conflict['secret_ready_count']), ('PENDING', 1))
            self.assertEqual((target / names[1]).read_text(), 'MOCK_VALUE=local\n')
            (target / names[1]).write_text('MOCK_VALUE=1\n')
            self.assertEqual(receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)['state'],
                             'APPLIED_AVAILABLE')
            added = 'Plain/.pypirc'
            (source / added).write_text('MOCK_VALUE=added\n')
            names.append(added)
            make_package(names)
            mock_publications.publish_all_secret_marker(shared, state, 'linux', key)
            second = receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)
            self.assertEqual((second['state'], second['secret_ready_count']), ('APPLIED_AVAILABLE', 3))
            self.assertEqual((target / added).read_text(), 'MOCK_VALUE=added\n')
            removed = names.pop(1)
            (source / removed).unlink()
            make_package(names)
            mock_publications.publish_all_secret_marker(shared, state, 'linux', key)
            third = receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)
            self.assertEqual((third['state'], third['missing_count']), ('ATTENTION', 1))
            self.assertTrue((target / removed).exists())
            self.assertEqual(receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)['missing_count'], 1)
            self.assertEqual(mock_publications.detect_empty_secret_set(shared, state, source, 'linux', key)['state'], 'UNCHANGED')
            for name in names:
                (source / name).unlink()
            self.assertEqual(mock_publications.detect_empty_secret_set(shared, state, source, 'linux', key)['state'], 'PUBLISHED')
            empty = receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)
            self.assertEqual((empty['state'], empty['missing_count']), ('ATTENTION', 3))
            self.assertTrue(all((target / name).exists() for name in (removed, *names)))
            self.assertEqual(mock_publications.publish_all_secret_marker(shared, state, 'linux', key)['state'], 'UNCHANGED')
            (source / names[0]).write_text('MOCK_VALUE=0\n')
            make_package([names[0]])
            mock_publications.publish_all_secret_marker(shared, state, 'linux', key)
            reappeared = receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)
            self.assertEqual((reappeared['state'], reappeared['missing_count']), ('ATTENTION', 2))
            stream = mock_publications._stream('secret-set', 'all', key)
            folder = mock_publications._folder(shared, 'linux', stream, create=False)
            chain = mock_publications._chain(folder, 'linux', 'secret-set', stream, key)
            (folder / (chain[-1]['id'] + '.json')).unlink()
            self.assertEqual(receiver.receive_discovered_secrets(target, shared, key, 'linux', interval=0.1)['state'], 'PENDING')

    def test_secrets_only_without_git_preserves_conflicts_and_detects_rollback(self):
        with tempfile.TemporaryDirectory(prefix='nexus-no-git-receiver-') as tmp:
            root = Path(tmp)
            shared, source = root / 'share', root / 'source'
            shared.mkdir()
            (source / 'Plain').mkdir(parents=True)
            paths = ['Plain/.env', 'Plain/.npmrc']
            for index, name in enumerate(paths):
                (source / name).write_text(f'MOCK_SECRET={index}-first\n')
            key = b'n' * 32
            package = ns.secrets_pack([source / name for name in paths], shared, 'linux', key, root=source)
            for name in paths:
                mock_publications.publish(shared, 'linux', 'secret', name, package, key)
            target = root / 'target'
            mock_git_sync.init_workspace(target, shared, key)
            (target / 'Plain').mkdir()
            first = receiver.receive_many_available(target, shared, key, 'linux', None, paths,
                                                    interval=0.1, secrets_only=True)
            self.assertEqual((first['state'], first['secret_ready_count']), ('APPLIED_AVAILABLE', 2))
            self.assertFalse((target / 'Plain' / '.git').exists())
            for name in paths:
                self.assertEqual((target / name).read_bytes(), (source / name).read_bytes())
            (source / paths[0]).write_text('MOCK_SECRET=updated\n')
            (target / paths[1]).write_text('MOCK_SECRET=local\n')
            (source / paths[1]).write_text('MOCK_SECRET=remote\n')
            newer = ns.secrets_pack([source / name for name in paths], shared, 'linux', key, root=source)
            for name in paths:
                mock_publications.publish(shared, 'linux', 'secret', name, newer, key)
            conflict = receiver.receive_many_available(target, shared, key, 'linux', None, paths,
                                                       interval=0.1, secrets_only=True)
            self.assertEqual((conflict['state'], conflict['secret_ready_count']), ('PENDING', 1))
            self.assertEqual((target / paths[0]).read_text(), 'MOCK_SECRET=updated\n')
            self.assertEqual((target / paths[1]).read_text(), 'MOCK_SECRET=local\n')
            key_file = root / 'key'
            key_file.write_bytes(key)
            command = [sys.executable, str(Path(receiver.__file__)), '--root', str(target), '--shared', str(shared),
                       '--key-file', str(key_file), '--source', 'linux', '--secrets-only', '--secret-path', paths[0],
                       '--timeout', '1', '--interval', '0.1']
            run = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertNotIn('MOCK_SECRET', run.stdout + run.stderr)
            stream = mock_publications._stream('secret', paths[0], key)
            folder = mock_publications._folder(shared, 'linux', stream, create=False)
            chain = mock_publications._chain(folder, 'linux', 'secret', stream, key)
            (folder / (chain[-1]['id'] + '.json')).unlink()
            self.assertEqual(receiver.receive_secret_available(target, shared, key, 'linux', paths[0],
                                                              timeout=1, interval=0.1)['state'], 'PENDING')
            self.assertEqual((target / paths[0]).read_text(), 'MOCK_SECRET=updated\n')

    def test_multiple_secret_streams_track_updates_and_conflicts_independently(self):
        with tempfile.TemporaryDirectory(prefix='nexus-multi-secret-test-') as tmp:
            root = Path(tmp)
            shared, source = root / 'share', root / 'source' / 'Bot'
            shared.mkdir()
            source.mkdir(parents=True)
            def git(*args):
                return subprocess.check_output(['git', '-C', str(source), '-c', 'user.name=Mock',
                                                '-c', 'user.email=mock@example.invalid', *args])
            git('init', '-q')
            (source / 'app.txt').write_text('mock\n')
            (source / '.env').write_text('MOCK_ENV=one\n')
            (source / '.npmrc').write_text('MOCK_NPM=one\n')
            # The secret is intentionally tracked to exercise Git history as well.
            git('add', 'app.txt', '.env')
            git('commit', '-qm', 'first')
            key = b'm' * 32
            git_package = ns.git_pack(source, shared, 'linux', key, nexus=source.parent)
            self.assertNotIn(b'MOCK_ENV=one', git_package.read_bytes())
            secrets = ns.secrets_pack([source / '.env', source / '.npmrc'], shared, 'linux', key, root=source.parent)
            mock_publications.publish(shared, 'linux', 'git', 'Bot', git_package, key)
            mock_publications.publish(shared, 'linux', 'secret', 'Bot/.env', secrets, key)
            target = root / 'target'
            mock_git_sync.init_workspace(target, shared, key)
            (target / 'Bot').mkdir()
            (target / 'Bot' / 'app.txt').write_text('mock\n')
            paths = ['Bot/.env', 'Bot/.npmrc']
            pending = receiver.receive_many_available(target, shared, key, 'linux', 'Bot', paths, interval=0.1)
            self.assertEqual((pending['state'], pending['secret_ready_count']), ('PENDING', 1))
            self.assertFalse((target / 'Bot' / '.npmrc').exists())
            mock_publications.publish(shared, 'linux', 'secret', 'Bot/.npmrc', secrets, key)
            ready = receiver.receive_many_available(target, shared, key, 'linux', 'Bot', paths, interval=0.1)
            self.assertEqual((ready['state'], ready['secret_ready_count']), ('APPLIED_AVAILABLE', 2))
            self.assertNotIn('Bot/.env', json.dumps(ready))
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_ENV=one\n')
            self.assertEqual((target / 'Bot' / '.npmrc').read_text(), 'MOCK_NPM=one\n')
            self.assertEqual(subprocess.check_output(['git', '-C', str(target / 'Bot'), 'show', 'HEAD:.env']),
                             b'MOCK_ENV=one\n')
            key_file = root / 'key'
            key_file.write_bytes(key)
            invocation = subprocess.run([sys.executable, str(Path(receiver.__file__)), '--root', str(target),
                '--shared', str(shared), '--key-file', str(key_file), '--source', 'linux', '--project', 'Bot',
                '--secret-path', paths[0], '--secret-path', paths[1], '--timeout', '1', '--interval', '0.1'],
                capture_output=True, text=True, timeout=10)
            self.assertEqual(invocation.returncode, 0, invocation.stderr)
            self.assertEqual(json.loads(invocation.stdout)['secret_ready_count'], 2)
            self.assertNotIn('MOCK_', invocation.stdout + invocation.stderr)
            (source / '.env').write_text('MOCK_ENV=two\n')
            (source / '.npmrc').write_text('MOCK_NPM=two\n')
            (target / 'Bot' / '.npmrc').write_text('MOCK_NPM=local\n')
            old_head = subprocess.check_output(['git', '-C', str(target / 'Bot'), 'rev-parse', 'HEAD'])
            git('commit', '--allow-empty', '-qm', 'second')
            new_head = git('rev-parse', 'HEAD').strip()
            git_package = ns.git_pack(source, shared, 'linux', key, nexus=source.parent)
            mock_publications.publish(shared, 'linux', 'git', 'Bot', git_package, key)
            secrets = ns.secrets_pack([source / '.env', source / '.npmrc'], shared, 'linux', key, root=source.parent)
            for path in paths:
                mock_publications.publish(shared, 'linux', 'secret', path, secrets, key)
            conflict = receiver.receive_many_available(target, shared, key, 'linux', 'Bot', paths, interval=0.1)
            self.assertEqual((conflict['state'], conflict['secret_ready_count']), ('PENDING', 1))
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_ENV=two\n')
            self.assertEqual((target / 'Bot' / '.npmrc').read_text(), 'MOCK_NPM=local\n')
            (target / 'Bot' / '.npmrc').write_text('MOCK_NPM=one\n')
            resolved = receiver.receive_many_available(target, shared, key, 'linux', 'Bot', paths, interval=0.1)
            self.assertEqual((resolved['state'], resolved['secret_ready_count']), ('PREPARED_AVAILABLE', 2))
            self.assertTrue(resolved['git_update_requires_review'])
            self.assertEqual((target / 'Bot' / '.npmrc').read_text(), 'MOCK_NPM=two\n')
            self.assertEqual(subprocess.check_output(['git', '-C', str(target / 'Bot'), 'rev-parse', 'HEAD']), old_head)
            refs = subprocess.check_output(['git', '-C', str(target / 'Bot'), 'for-each-ref',
                                            '--format=%(objectname)', 'refs/nexus-sync/linux']).splitlines()
            self.assertIn(new_head, refs)
            with self.assertRaises(ns.SyncError):
                receiver.receive_many_available(target, shared, key, 'linux', 'Bot', ['Bot/.env', 'bot/.ENV'])

    def test_explicit_exclusive_mock_fast_forward_and_secret_update(self):
        with tempfile.TemporaryDirectory(prefix='nexus-auto-fast-forward-test-') as tmp:
            root = Path(tmp)
            shared = root / 'share'
            shared.mkdir()
            source = root / 'source' / 'Bot'
            source.mkdir(parents=True)
            def git(path, *args):
                return subprocess.check_output(['git', '-C', str(path), '-c', 'user.name=Mock', '-c',
                                                'user.email=mock@example.invalid', *args]).decode().strip()
            git(source, 'init', '-q')
            (source / '.gitignore').write_text('.env\n')
            (source / '.env').write_text('MOCK_SECRET=one\n')
            (source / 'app.txt').write_text('one\n')
            git(source, 'add', '.gitignore', 'app.txt')
            git(source, 'commit', '-qm', 'one')
            key = b'd' * 32
            git_package = ns.git_pack(source, shared, 'linux', key, nexus=source.parent)
            secret = ns.secrets_pack([source / '.env'], shared, 'linux', key, root=source.parent)
            mock_publications.publish(shared, 'linux', 'git', 'Bot', git_package, key)
            mock_publications.publish(shared, 'linux', 'secret', 'Bot/.env', secret, key)
            target = root / 'target'
            mock_git_sync.init_workspace(target, shared, key)
            (target / 'Bot').mkdir()
            for name in ('.gitignore', 'app.txt'):
                shutil.copyfile(source / name, target / 'Bot' / name)
            first = receiver.receive_available(target, shared, key, 'linux', 'Bot', 'Bot/.env', timeout=1, interval=0.1,
                                               exclusive_mock_fast_forward=True)
            self.assertEqual(first['state'], 'APPLIED_AVAILABLE')
            (source / 'app.txt').write_text('two\n')
            (source / '.env').write_text('MOCK_SECRET=two\n')
            git(source, 'add', 'app.txt')
            git(source, 'commit', '-qm', 'two')
            git_package = ns.git_pack(source, shared, 'linux', key, nexus=source.parent)
            secret = ns.secrets_pack([source / '.env'], shared, 'linux', key, root=source.parent)
            mock_publications.publish(shared, 'linux', 'git', 'Bot', git_package, key)
            mock_publications.publish(shared, 'linux', 'secret', 'Bot/.env', secret, key)
            result = receiver.receive_available(target, shared, key, 'linux', 'Bot', 'Bot/.env', timeout=2, interval=0.1,
                                                exclusive_mock_fast_forward=True)
            self.assertEqual(result['state'], 'APPLIED_AVAILABLE')
            self.assertTrue(result['git_fast_forward_recorded'])
            self.assertEqual(git(target / 'Bot', 'rev-parse', 'HEAD'), git(source, 'rev-parse', 'HEAD'))
            self.assertEqual((target / 'Bot' / 'app.txt').read_text(), 'two\n')
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_SECRET=two\n')

    def test_new_git_publication_fetches_refs_without_changing_branch_or_index(self):
        with tempfile.TemporaryDirectory(prefix='nexus-auto-git-test-') as tmp:
            root = Path(tmp)
            share = root / 'share'
            share.mkdir()
            source = root / 'source' / 'Bot'
            source.mkdir(parents=True)
            (source / 'app.txt').write_text('mock source\n')
            (source / '.env').write_text('MOCK_SECRET=value\n')
            def git(path, *args):
                return subprocess.check_output(['git', '-C', str(path), '-c', 'user.name=Mock', '-c',
                                                'user.email=mock@example.invalid', *args]).decode().strip()
            git(source, 'init', '-q')
            git(source, 'add', 'app.txt')
            git(source, 'commit', '-qm', 'first')
            key = b'c' * 32
            first = ns.git_pack(source, share, 'linux', key, nexus=source.parent)
            secret = ns.secrets_pack([source / '.env'], share, 'linux', key, root=source.parent)
            mock_publications.publish(share, 'linux', 'git', 'Bot', first, key)
            mock_publications.publish(share, 'linux', 'secret', 'Bot/.env', secret, key)
            target = root / 'target'
            mock_git_sync.init_workspace(target, share, key)
            (target / 'Bot').mkdir()
            (target / 'Bot' / 'app.txt').write_text('mock source\n')
            self.assertEqual(receiver.receive_available(target, share, key, 'linux', 'Bot', 'Bot/.env',
                                                       timeout=1, interval=0.1)['state'], 'APPLIED_AVAILABLE')
            previous_head = git(target / 'Bot', 'rev-parse', 'HEAD')
            previous_index = (target / 'Bot' / '.git' / 'index').read_bytes()
            (source / 'app.txt').write_text('mock updated\n')
            git(source, 'add', 'app.txt')
            git(source, 'commit', '-qm', 'second')
            newer_head = git(source, 'rev-parse', 'HEAD')
            second = ns.git_pack(source, share, 'linux', key, nexus=source.parent)
            mock_publications.publish(share, 'linux', 'git', 'Bot', second, key)
            (source / '.env').write_text('MOCK_SECRET=updated\n')
            new_secret = ns.secrets_pack([source / '.env'], share, 'linux', key, root=source.parent)
            mock_publications.publish(share, 'linux', 'secret', 'Bot/.env', new_secret, key)
            result = receiver.receive_available(target, share, key, 'linux', 'Bot', 'Bot/.env',
                                                timeout=2, interval=0.1)
            self.assertEqual(result['state'], 'PREPARED_AVAILABLE')
            self.assertTrue(result['git_update_requires_review'])
            self.assertEqual(git(target / 'Bot', 'rev-parse', 'HEAD'), previous_head)
            self.assertEqual((target / 'Bot' / '.git' / 'index').read_bytes(), previous_index)
            self.assertEqual((target / 'Bot' / 'app.txt').read_text(), 'mock source\n')
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_SECRET=updated\n')
            refs = git(target / 'Bot', 'for-each-ref', '--format=%(objectname)', 'refs/nexus-sync/linux')
            self.assertIn(newer_head, refs.splitlines())
            self.assertEqual(receiver.receive_available(target, share, key, 'linux', 'Bot', 'Bot/.env',
                                                       timeout=1, interval=0.1)['state'], 'PREPARED_AVAILABLE')
            (target / 'Bot' / 'app.txt').write_text('local edit\n')
            git(target / 'Bot', 'add', 'app.txt')
            git(source, 'commit', '--allow-empty', '-qm', 'third')
            third = ns.git_pack(source, share, 'linux', key, nexus=source.parent)
            mock_publications.publish(share, 'linux', 'git', 'Bot', third, key)
            self.assertEqual(receiver.receive_available(target, share, key, 'linux', 'Bot', 'Bot/.env',
                                                       timeout=1, interval=0.1)['state'], 'PENDING')
            self.assertEqual((target / 'Bot' / 'app.txt').read_text(), 'local edit\n')
            self.assertEqual(git(target / 'Bot', 'rev-parse', 'HEAD'), previous_head)

    def test_automatic_selection_waits_and_detects_rollback(self):
        with tempfile.TemporaryDirectory(prefix='nexus-auto-publications-test-') as tmp:
            root = Path(tmp)
            source_share, target_share = root / 'source-share', root / 'target-share'
            source_share.mkdir()
            target_share.mkdir()
            source = root / 'source' / 'Bot'
            source.mkdir(parents=True)
            (source / 'app.txt').write_text('mock source\n')
            (source / '.env').write_text('MOCK_SECRET=value\n')
            subprocess.run(['git', 'init', '-q', str(source)], check=True)
            subprocess.run(['git', '-C', str(source), 'add', 'app.txt'], check=True)
            subprocess.run(['git', '-C', str(source), '-c', 'user.name=Mock', '-c',
                            'user.email=mock@example.invalid', 'commit', '-qm', 'mock'], check=True)
            key = b'b' * 32
            git = ns.git_pack(source, source_share, 'linux', key, nexus=source.parent)
            secret = ns.secrets_pack([source / '.env'], source_share, 'linux', key, root=source.parent)
            mock_publications.publish(source_share, 'linux', 'git', 'Bot', git, key)
            mock_publications.publish(source_share, 'linux', 'secret', 'Bot/.env', secret, key)
            target = root / 'target'
            mock_git_sync.init_workspace(target, target_share, key)
            (target / 'Bot').mkdir()

            def deliver():
                time.sleep(0.2)
                shutil.copytree(source_share, target_share, dirs_exist_ok=True)
            worker = threading.Thread(target=deliver)
            worker.start()
            try:
                result = receiver.receive_available(target, target_share, key, 'linux', 'Bot', 'Bot/.env',
                                                    timeout=3, interval=0.1)
            finally:
                worker.join()
            self.assertEqual(result['state'], 'APPLIED_AVAILABLE')
            self.assertFalse(result['remote_freshness_confirmed'])
            self.assertFalse(result['chats_updated'])
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_SECRET=value\n')
            repeat = receiver.receive_available(target, target_share, key, 'linux', 'Bot', 'Bot/.env',
                                                timeout=1, interval=0.1)
            self.assertEqual(repeat['state'], 'APPLIED_AVAILABLE')
            key_file = root / 'key'
            key_file.write_bytes(key)
            watcher = subprocess.Popen([sys.executable, receiver.__file__, '--root', str(target),
                '--shared', str(target_share), '--key-file', str(key_file), '--source', 'linux', '--project', 'Bot',
                '--secret-path', 'Bot/.env', '--watch', '--interval', '0.1'],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.addCleanup(lambda: watcher.kill() if watcher.poll() is None else None)
            (source / '.env').write_text('MOCK_SECRET=updated\n')
            updated = ns.secrets_pack([source / '.env'], source_share, 'linux', key, root=source.parent)
            mock_publications.publish(source_share, 'linux', 'secret', 'Bot/.env', updated, key)
            shutil.copytree(source_share, target_share, dirs_exist_ok=True)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and (target / 'Bot' / '.env').read_text() != 'MOCK_SECRET=updated\n':
                self.assertIsNone(watcher.poll())
                time.sleep(0.05)
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_SECRET=updated\n')
            watcher.send_signal(signal.SIGINT)
            watcher.wait(timeout=5)
            applied = receiver.receive_available(target, target_share, key, 'linux', 'Bot', 'Bot/.env',
                                                  timeout=1, interval=0.1)
            self.assertEqual(applied['state'], 'APPLIED_AVAILABLE')
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_SECRET=updated\n')
            (target / 'Bot' / '.env').write_text('MOCK_SECRET=local\n')
            (source / '.env').write_text('MOCK_SECRET=third\n')
            third = ns.secrets_pack([source / '.env'], source_share, 'linux', key, root=source.parent)
            mock_publications.publish(source_share, 'linux', 'secret', 'Bot/.env', third, key)
            shutil.copytree(source_share, target_share, dirs_exist_ok=True)
            conflict = receiver.receive_available(target, target_share, key, 'linux', 'Bot', 'Bot/.env',
                                                   timeout=1, interval=0.1)
            self.assertEqual(conflict['state'], 'PENDING')
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_SECRET=local\n')
            stream = mock_publications._stream('secret', 'Bot/.env', key)
            folder = target_share / 'nexus-sync' / 'mock-publications' / 'linux' / stream
            for entry in folder.glob('*.json'):
                entry.unlink()
            rollback = receiver.receive_available(target, target_share, key, 'linux', 'Bot', 'Bot/.env',
                                                  timeout=1, interval=0.1)
            self.assertEqual(rollback['state'], 'PENDING')
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_SECRET=local\n')

    def test_waits_for_selected_packages_and_preserves_conflicts(self):
        with tempfile.TemporaryDirectory(prefix='nexus-auto-receiver-test-') as tmp:
            root = Path(tmp)
            source_share, target_share = root / 'source-share', root / 'target-share'
            source_share.mkdir()
            target_share.mkdir()
            source = root / 'source' / 'Bot'
            source.mkdir(parents=True)
            (source / 'app.txt').write_text('mock source\n')
            (source / '.env').write_text('MOCK_SECRET=value\n')
            subprocess.run(['git', 'init', '-q', str(source)], check=True)
            subprocess.run(['git', '-C', str(source), 'add', 'app.txt'], check=True)
            subprocess.run(['git', '-C', str(source), '-c', 'user.name=Mock', '-c',
                            'user.email=mock@example.invalid', 'commit', '-qm', 'mock'], check=True)
            key = b'a' * 32
            git_package = ns.git_pack(source, source_share, 'linux', key, nexus=source.parent)
            secret_package = ns.secrets_pack([source / '.env'], source_share, 'linux', key, root=source.parent)
            target = root / 'target'
            mock_git_sync.init_workspace(target, target_share, key)
            (target / 'Bot').mkdir()
            (target / 'Bot' / 'app.txt').write_text('mock local edit\n')
            target_git = target_share / git_package.relative_to(source_share)
            target_secret = target_share / secret_package.relative_to(source_share)
            observations = []
            def deliver():
                time.sleep(0.15)
                target_git.parent.mkdir(parents=True)
                target_git.write_bytes(git_package.read_bytes()[:20])
                encrypted = secret_package.read_bytes()
                target_secret.write_bytes(encrypted[:-1] + bytes([encrypted[-1] ^ 1]))
                time.sleep(0.2)
                observations.append((target / 'Bot' / '.env').exists())
                shutil.copyfile(git_package, target_git)
                shutil.copyfile(secret_package, target_secret)
            worker = threading.Thread(target=deliver)
            worker.start()
            try:
                result = receiver.receive(target, target_share, key, target_git, target_secret,
                                          'Bot', 'Bot/.env', timeout=3, interval=0.1)
            finally:
                worker.join()
            self.assertEqual(result['state'], 'COMPLETE')
            self.assertEqual(observations, [False])
            self.assertFalse(result['chats_updated'])
            self.assertNotIn('MOCK_SECRET', json.dumps(result))
            self.assertEqual((target / 'Bot' / 'app.txt').read_text(), 'mock local edit\n')
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_SECRET=value\n')
            receipt = receiver._git_receipt(target, key, 'Bot')
            self.assertNotIn('Bot', receipt.read_text())
            self.assertNotIn('MOCK_SECRET', receipt.read_text())
            repeat = receiver.receive(target, target_share, key, target_git, target_secret,
                                      'Bot', 'Bot/.env', timeout=1, interval=0.1)
            self.assertEqual(repeat['state'], 'COMPLETE')
            (target / 'Bot' / '.env').write_text('MOCK_SECRET=local\n')
            subprocess.run(['git', '-C', str(target / 'Bot'), 'add', 'app.txt'], check=True)
            changed = receiver.receive(target, target_share, key, target_git, target_secret,
                                       'Bot', 'Bot/.env', timeout=1, interval=0.1)
            self.assertEqual(changed['state'], 'PENDING')
            self.assertFalse(changed['git_installed'])
            self.assertFalse(changed['secret_ready'])
            self.assertEqual((target / 'Bot' / '.env').read_text(), 'MOCK_SECRET=local\n')
            self.assertEqual((target / 'Bot' / 'app.txt').read_text(), 'mock local edit\n')


if __name__ == '__main__':
    unittest.main()
