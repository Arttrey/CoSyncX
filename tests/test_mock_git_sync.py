import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import mock_git_sync as mock_git
import nexus_sync as ns


@unittest.skipUnless(sys.platform == 'linux' and shutil.which('git'), 'requires Linux and Git')
class MockGitRestoreTests(unittest.TestCase):
    def test_fast_forward_requires_clean_matching_branch_and_ancestor(self):
        with tempfile.TemporaryDirectory(prefix='nexus-mock-git-fast-forward-') as tmp:
            root = Path(tmp)
            shared = root / 'shared'
            shared.mkdir()
            source = root / 'source' / 'Bot'
            source.mkdir(parents=True)
            def git(path, *args):
                return subprocess.check_output(['git', '-C', str(path), '-c', 'user.name=Mock', '-c',
                                                'user.email=mock@example.invalid', *args]).decode().strip()
            git(source, 'init', '-q')
            (source / 'app.txt').write_text('one\n')
            git(source, 'add', 'app.txt')
            git(source, 'commit', '-qm', 'one')
            key = b'f' * 32
            first = ns.git_pack(source, shared, 'linux', key, nexus=source.parent)
            target = root / 'target'
            mock_git.init_workspace(target, shared, key)
            project = target / 'Bot'
            project.mkdir()
            (project / 'app.txt').write_text('one\n')
            mock_git.apply_git(target, shared, key, first, 'Bot')
            (source / 'app.txt').write_text('two\n')
            git(source, 'add', 'app.txt')
            git(source, 'commit', '-qm', 'two')
            second = ns.git_pack(source, shared, 'linux', key, nexus=source.parent)
            result = mock_git.fast_forward_git(target, shared, key, second, 'Bot', 'linux')
            self.assertEqual(result['state'], 'FAST_FORWARDED')
            self.assertEqual(git(project, 'rev-parse', 'HEAD'), git(source, 'rev-parse', 'HEAD'))
            self.assertEqual((project / 'app.txt').read_text(), 'two\n')
            self.assertEqual(mock_git.fast_forward_git(target, shared, key, second, 'Bot', 'linux')['state'], 'UNCHANGED')
            (source / 'app.txt').write_text('three\n')
            git(source, 'add', 'app.txt')
            git(source, 'commit', '-qm', 'three')
            third = ns.git_pack(source, shared, 'linux', key, nexus=source.parent)
            (project / 'untracked.txt').write_text('local untracked\n')
            self.assertEqual(mock_git.fast_forward_git(target, shared, key, third, 'Bot', 'linux')['state'], 'BLOCKED')
            self.assertEqual((project / 'untracked.txt').read_text(), 'local untracked\n')
            (project / 'untracked.txt').unlink()
            branch = git(source, 'symbolic-ref', '--short', 'HEAD')
            git(source, 'checkout', '-qb', 'other-mock-branch')
            other = ns.git_pack(source, shared, 'linux', key, nexus=source.parent)
            self.assertEqual(mock_git.fast_forward_git(target, shared, key, other, 'Bot', 'linux')['state'], 'BLOCKED')
            git(source, 'checkout', '-q', branch)
            (source / 'app.txt').write_text('staged source change\n')
            git(source, 'add', 'app.txt')
            staged = ns.git_pack(source, shared, 'linux', key, nexus=source.parent)
            self.assertEqual(mock_git.fast_forward_git(target, shared, key, staged, 'Bot', 'linux')['state'], 'BLOCKED')
            (project / 'app.txt').write_text('local edit\n')
            before = git(project, 'rev-parse', 'HEAD')
            self.assertEqual(mock_git.fast_forward_git(target, shared, key, third, 'Bot', 'linux')['state'], 'BLOCKED')
            self.assertEqual(git(project, 'rev-parse', 'HEAD'), before)
            self.assertEqual((project / 'app.txt').read_text(), 'local edit\n')
            git(project, 'add', 'app.txt')
            git(project, 'commit', '-qm', 'local divergence')
            divergent_head = git(project, 'rev-parse', 'HEAD')
            self.assertEqual(mock_git.fast_forward_git(target, shared, key, third, 'Bot', 'linux')['state'], 'BLOCKED')
            self.assertEqual(git(project, 'rev-parse', 'HEAD'), divergent_head)

    def test_restore_existing_project_without_changing_its_files(self):
        with tempfile.TemporaryDirectory(prefix='nexus-mock-git-test-') as tmp:
            root = Path(tmp)
            shared = root / 'shared'
            shared.mkdir()
            source_nexus = root / 'source'
            repo = source_nexus / 'Bot'
            repo.mkdir(parents=True)
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            (repo / 'app.txt').write_text('committed mock\n')
            subprocess.run(['git', '-C', str(repo), 'add', 'app.txt'], check=True)
            subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Mock', '-c',
                            'user.email=mock@example.invalid', 'commit', '-qm', 'mock'], check=True)
            (repo / 'app.txt').write_text('staged mock\n')
            subprocess.run(['git', '-C', str(repo), 'add', 'app.txt'], check=True)
            package = ns.git_pack(repo, shared, 'linux', b'k' * 32, nexus=source_nexus)
            target = root / 'mock-nexus'
            mock_git.init_workspace(target, shared, b'k' * 32)
            project = target / 'Bot'
            project.mkdir()
            (project / 'app.txt').write_text('received uncommitted mock\n')
            (project / 'extra.txt').write_text('received extra\n')
            with self.assertRaises(ns.SyncError):
                mock_git.apply_git(target, shared, b'k' * 32, package, 'wrong')
            result = mock_git.apply_git(target, shared, b'k' * 32, package, 'Bot')
            self.assertEqual(result['state'], 'INSTALLED')
            self.assertEqual((project / 'app.txt').read_text(), 'received uncommitted mock\n')
            self.assertEqual((project / 'extra.txt').read_text(), 'received extra\n')
            self.assertEqual(subprocess.check_output(['git', '-C', str(project), 'show', ':app.txt']), b'staged mock\n')
            self.assertEqual(subprocess.check_output(['git', '-C', str(project), 'rev-parse', 'HEAD']),
                             subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD']))
            with self.assertRaises(ns.SyncError):
                mock_git.apply_git(target, shared, b'k' * 32, package, 'Bot')
            self.assertEqual((project / 'app.txt').read_text(), 'received uncommitted mock\n')
            before_head = subprocess.check_output(['git', '-C', str(project), 'rev-parse', 'HEAD'])
            before_index = (project / '.git' / 'index').read_bytes()
            subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Mock', '-c',
                            'user.email=mock@example.invalid', 'commit', '-qm', 'next'], check=True)
            subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Mock', '-c',
                            'user.email=mock@example.invalid', 'tag', '-a', 'mock-tag', '-m', 'mock'], check=True)
            update = ns.git_pack(repo, shared, 'linux', b'k' * 32, nexus=source_nexus)
            fetched = mock_git.fetch_git(target, shared, b'k' * 32, update, 'Bot', 'linux')
            self.assertEqual(fetched['state'], 'FETCHED')
            self.assertFalse(fetched['index_updated'])
            self.assertEqual((project / '.git' / 'index').read_bytes(), before_index)
            self.assertEqual(subprocess.check_output(['git', '-C', str(project), 'rev-parse', 'HEAD']), before_head)
            self.assertEqual((project / 'app.txt').read_text(), 'received uncommitted mock\n')
            source_branch = subprocess.check_output(['git', '-C', str(repo), 'symbolic-ref', '--short', 'HEAD'], text=True).strip()
            received_ref = f'refs/nexus-sync/linux/{update.stem}/heads/{source_branch}'
            self.assertEqual(subprocess.check_output(['git', '-C', str(project), 'rev-parse', received_ref]),
                             subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD']))
            self.assertEqual(subprocess.check_output(['git', '-C', str(project), 'rev-parse',
                f'refs/nexus-sync/linux/{update.stem}/tags/mock-tag']),
                subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'refs/tags/mock-tag']))
            self.assertEqual(mock_git.fetch_git(target, shared, b'k' * 32, update, 'Bot', 'linux')['state'], 'UNCHANGED')
            subprocess.run(['git', '-C', str(project), 'update-ref', received_ref, before_head.decode().strip()], check=True)
            with self.assertRaises(ns.SyncError):
                mock_git.fetch_git(target, shared, b'k' * 32, update, 'Bot', 'linux')
            self.assertEqual(subprocess.check_output(['git', '-C', str(project), 'rev-parse', received_ref]), before_head)

    def test_rejects_unmarked_workspace(self):
        with tempfile.TemporaryDirectory(prefix='nexus-mock-git-test-') as tmp:
            root = Path(tmp)
            workspace, shared = root / 'workspace', root / 'shared'
            workspace.mkdir()
            shared.mkdir()
            with self.assertRaises(ns.SyncError):
                mock_git.apply_git(workspace, shared, b'k' * 32, shared / 'missing', 'Bot')


if __name__ == '__main__':
    unittest.main()
