"""Opt-in OpenSSL EVP interoperability checks with synthetic data only."""
import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import nexus_sync as ns


OPENSSL_DECRYPTOR = r'''
#include <openssl/evp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static int read_exact(unsigned char *dst, size_t n) {
    return fread(dst, 1, n, stdin) == n;
}
static uint32_t from_be(const unsigned char *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}
int main(void) {
    unsigned char key[32], nonce[12], tag[16], sizes[8];
    unsigned char *aad = NULL, *ciphertext = NULL, *plain = NULL;
    EVP_CIPHER_CTX *ctx = NULL;
    int len = 0, total = 0, result = 2;
    uint32_t aad_len, cipher_len;
    if (!read_exact(key, sizeof(key)) || !read_exact(nonce, sizeof(nonce)) ||
        !read_exact(tag, sizeof(tag)) || !read_exact(sizes, sizeof(sizes))) goto done;
    aad_len = from_be(sizes); cipher_len = from_be(sizes + 4);
    if (aad_len > 1048576 || cipher_len > 1048576) goto done;
    aad = malloc(aad_len ? aad_len : 1);
    ciphertext = malloc(cipher_len ? cipher_len : 1);
    plain = malloc((size_t)cipher_len + 16);
    if (!aad || !ciphertext || !plain || !read_exact(aad, aad_len) ||
        !read_exact(ciphertext, cipher_len) || fgetc(stdin) != EOF) goto done;
    ctx = EVP_CIPHER_CTX_new();
    if (!ctx || EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, NULL, NULL) != 1 ||
        EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, sizeof(nonce), NULL) != 1 ||
        EVP_DecryptInit_ex(ctx, NULL, NULL, key, nonce) != 1 ||
        EVP_DecryptUpdate(ctx, NULL, &len, aad, (int)aad_len) != 1 ||
        EVP_DecryptUpdate(ctx, plain, &len, ciphertext, (int)cipher_len) != 1) goto done;
    total = len;
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, sizeof(tag), tag) != 1 ||
        EVP_DecryptFinal_ex(ctx, plain + total, &len) != 1) goto done;
    total += len;
    if (fwrite(plain, 1, (size_t)total, stdout) != (size_t)total) goto done;
    result = 0;
done:
    EVP_CIPHER_CTX_free(ctx);
    free(aad); free(ciphertext); free(plain);
    return result;
}
'''


@unittest.skipUnless(os.environ.get('NEXUS_TEST_OPENSSL') == '1' and shutil.which('cc'),
                     'requires opt-in compiler and OpenSSL development files')
class OpenSSLInteropTests(unittest.TestCase):
    def test_mock_chat_and_secret_packages_with_openssl_evp(self):
        with tempfile.TemporaryDirectory(prefix='nexus-openssl-test-') as tmp:
            root = Path(tmp)
            helper_source = root / 'decrypt.c'
            helper = root / 'decrypt-helper'
            helper_source.write_text(OPENSSL_DECRYPTOR, encoding='utf-8')
            build = subprocess.run(['cc', str(helper_source), '-o', str(helper), '-lcrypto'], capture_output=True)
            self.assertEqual(build.returncode, 0, build.stderr.decode(errors='replace'))
            key = b'\x29' * 32
            def decrypt(nonce, tag, aad, ciphertext, decrypt_key=key):
                body = (decrypt_key + nonce + tag + len(aad).to_bytes(4, 'big') +
                        len(ciphertext).to_bytes(4, 'big') + aad + ciphertext)
                return subprocess.run([str(helper)], input=body, capture_output=True)
            home = root / 'codex'
            sessions = home / 'sessions' / '2026' / '01' / '01'
            sessions.mkdir(parents=True)
            chat = sessions / 'rollout-test.jsonl'
            chat.write_bytes(b'{"type":"session_meta","payload":{"id":"mock"}}\n')
            shared = root / 'shared'
            shared.mkdir()
            snapshot = ns.snapshot(home, shared, 'linux', key)
            manifest = ns.verify(snapshot, key)
            relative = manifest['files'][0]['path']
            encrypted = (snapshot / 'files' / (relative + '.enc')).read_bytes()
            result = decrypt(encrypted[:12], encrypted[-16:], relative.encode('utf-8'), encrypted[12:-16])
            self.assertEqual(result.returncode, 0)
            self.assertEqual(result.stdout, chat.read_bytes())
            wrong_path = decrypt(encrypted[:12], encrypted[-16:], b'wrong/path', encrypted[12:-16])
            self.assertNotEqual(wrong_path.returncode, 0)
            self.assertEqual(wrong_path.stdout, b'')
            altered_tag = bytes([encrypted[-16] ^ 1]) + encrypted[-15:]
            tampered = decrypt(encrypted[:12], altered_tag, relative.encode('utf-8'), encrypted[12:-16])
            self.assertNotEqual(tampered.returncode, 0)
            self.assertEqual(tampered.stdout, b'')
            secret = root / '.env'
            secret.write_bytes(b'MOCK_SECRET=interoperability')
            package = ns.secrets_pack([secret], shared, 'linux', key).read_bytes()
            offset = len(ns.SECRET_MAGIC)
            result = decrypt(package[offset:offset + 12], package[-16:], ns.SECRET_MAGIC, package[offset + 12:-16])
            self.assertEqual(result.returncode, 0)
            unpacked = json.loads(result.stdout)
            self.assertEqual(base64.b64decode(unpacked['files'][0]['data']), secret.read_bytes())
            wrong_aad = decrypt(package[offset:offset + 12], package[-16:], b'wrong', package[offset + 12:-16])
            self.assertNotEqual(wrong_aad.returncode, 0)
            self.assertEqual(wrong_aad.stdout, b'')
            nonce = package[offset:offset + 12]
            ciphertext = package[offset + 12:-16]
            tag = package[-16:]
            changes = [
                (nonce, tag, ciphertext, b'\x30' * 32),
                (bytes([nonce[0] ^ 1]) + nonce[1:], tag, ciphertext, key),
                (nonce, tag, bytes([ciphertext[0] ^ 1]) + ciphertext[1:], key),
                (nonce, bytes([tag[0] ^ 1]) + tag[1:], ciphertext, key),
                (nonce, tag, ciphertext[:-1], key),
            ]
            for index, (changed_nonce, changed_tag, changed_ciphertext, changed_key) in enumerate(changes):
                with self.subTest(case=index):
                    rejected = decrypt(changed_nonce, changed_tag, ns.SECRET_MAGIC, changed_ciphertext, changed_key)
                    self.assertNotEqual(rejected.returncode, 0)
                    self.assertEqual(rejected.stdout, b'')
            second = ns.secrets_pack([secret], shared, 'linux', key).read_bytes()
            self.assertNotEqual(package[offset:offset + 12], second[offset:offset + 12])
            self.assertNotEqual(package, second)
            independent = decrypt(second[offset:offset + 12], second[-16:], ns.SECRET_MAGIC, second[offset + 12:-16])
            self.assertEqual(independent.returncode, 0)
            self.assertEqual(base64.b64decode(json.loads(independent.stdout)['files'][0]['data']), secret.read_bytes())


if __name__ == '__main__':
    unittest.main()
