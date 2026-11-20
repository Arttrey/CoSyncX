import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
import zipfile

import cosync_vendor


class VendorArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def archive(self, name='syncthing', *, unsafe=False):
        path = self.root / 'release.tar.gz'
        with tarfile.open(path, 'w:gz') as output:
            for filename, data in ((name, b'synthetic executable'), ('LICENSE.txt', b'synthetic license')):
                info = tarfile.TarInfo('syncthing-release/' + filename)
                info.size = len(data)
                if unsafe and filename == name:
                    info.type = tarfile.SYMTYPE
                    info.linkname = '../outside'
                    info.size = 0
                    output.addfile(info)
                else:
                    output.addfile(info, io.BytesIO(data))
        return path

    def test_verifies_and_installs_only_binary_and_license(self):
        archive = self.archive()
        destination = self.root / 'engine'
        cosync_vendor.install_archive(archive, destination, hashlib.sha256(archive.read_bytes()).hexdigest(),
                                       binary_name='syncthing')
        self.assertEqual((destination / 'syncthing').read_bytes(), b'synthetic executable')
        self.assertEqual((destination / 'LICENSE').read_bytes(), b'synthetic license')
        with self.assertRaises(cosync_vendor.VendorError):
            cosync_vendor.install_archive(archive, destination, hashlib.sha256(archive.read_bytes()).hexdigest(),
                                           binary_name='syncthing')

    def test_bad_checksum_and_symlink_do_not_install(self):
        archive = self.archive(unsafe=True)
        destination = self.root / 'engine'
        with self.assertRaises(cosync_vendor.VendorError):
            cosync_vendor.install_archive(archive, destination, '0' * 64, binary_name='syncthing')
        self.assertFalse(destination.exists())
        with self.assertRaises(cosync_vendor.VendorError):
            cosync_vendor.install_archive(archive, destination, hashlib.sha256(archive.read_bytes()).hexdigest(),
                                           binary_name='syncthing')
        self.assertFalse(destination.exists())

    def test_windows_zip_is_supported_without_extracting_extra_entries(self):
        archive = self.root / 'release.zip'
        with zipfile.ZipFile(archive, 'w') as output:
            output.writestr('syncthing-release/syncthing.exe', b'synthetic exe')
            output.writestr('syncthing-release/LICENSE', b'synthetic license')
            output.writestr('../outside', b'must not be extracted')
        destination = self.root / 'engine'
        cosync_vendor.install_archive(archive, destination, hashlib.sha256(archive.read_bytes()).hexdigest(),
                                       binary_name='syncthing.exe')
        self.assertEqual((destination / 'syncthing.exe').read_bytes(), b'synthetic exe')
        self.assertFalse((self.root / 'outside').exists())

    def test_release_targets_are_fixed_and_unknown_systems_are_rejected(self):
        name, checksum, binary = cosync_vendor.release_asset('linux', 'amd64')
        self.assertEqual(name, 'syncthing-linux-amd64-v2.1.7.tar.gz')
        self.assertEqual(len(checksum), 64)
        self.assertEqual(binary, 'syncthing')
        self.assertEqual(cosync_vendor.release_asset('windows', 'amd64')[2], 'syncthing.exe')
        with self.assertRaises(cosync_vendor.VendorError):
            cosync_vendor.release_asset('unknown', 'amd64')


if __name__ == '__main__':
    unittest.main()
