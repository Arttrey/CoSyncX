# CoSyncX

CoSyncX is an experimental companion for moving Codex conversation history
between devices using Syncthing. It provides a streamlined Syncthing interface,
a CLI, and tools to inspect and prepare encrypted chat snapshots. Device paths
are configured locally rather than tied to a particular user or operating system.

**Status: alpha.** Chat import, indexing and visibility in Cursor are not
verified for real profiles. Use isolated test data for restoration. Do not
automatically modify an active Codex profile. A Syncthing folder being in sync
does not prove that a chat is available or up to date in Codex.

The experimental restore reload handler and single-generation backup helper are
not connected to a production receiver. They do not enable automatic import or
reload of real chats. The mock receiver also supports macOS in isolated tests.

`cosyncx extensions export` can create a separately encrypted bundle of custom
skills, hooks, rules, plugin files, `AGENTS.md` and `config.toml`. The config can
contain secrets and machine-specific paths. The bundle also records plugin cache
names as an inventory, not proof that plugins are installed. Built-in skills,
plugin cache contents, local dependency/build directories and recognized
platform binaries are excluded and listed in the encrypted exclusion report.
`cosyncx extensions extract` only unpacks into a new quarantine directory; it
does not install, execute or apply extensions or configuration. Bundle creation,
transport, dependency installation and restoration are not yet automatic.

```sh
cosyncx keygen /path/outside/syncthing/extensions.key
cosyncx extensions export --codex-home "$HOME/.codex" --output /path/to/shared/extensions.bundle --key-file /path/outside/syncthing/extensions.key
cosyncx extensions extract --bundle /path/to/shared/extensions.bundle --destination /path/to/new/quarantine --key-file /path/outside/syncthing/extensions.key
```

Transfer the key separately and securely. Never point extraction at a live Codex
profile; review paths and hook commands before applying them on another OS.

Syncthing can also transfer the existing `sessions` and `archived_sessions`
directories directly. Folder encryption and direct transfer require independent
validation, careful exclusion of credentials, and safe key management. The
Codex title index lives outside those folders and is not automatically restored.
The `engine add-folder` command uses Syncthing's default marker and may create
`.stfolder` in the selected source directory; it does not configure encrypted
sharing. Do not use it on an active Codex profile. Direct encrypted setup is
not yet provided by the CLI and must only be tested with disposable directories.
Syncthing's encrypted untrusted-device mode is experimental; review its
[documentation](https://docs.syncthing.net/users/untrusted.html) before use.

## Installation

Python 3.11 or later is required for a source installation:

```sh
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install .
cosyncx --help
cosyncx engine --help
```

CoSyncX supports an isolated bundled Syncthing engine and an already-running
local Syncthing. See [engine and CLI documentation](docs/ENGINE.md). Installing
the CLI does not start or configure either engine. A VPS can use Syncthing
without running CoSyncX.

The optional `cursor-extension/` displays local experimental status; it does
not verify that chats were imported or are visible in Cursor.

## Development

```sh
python -m pip install -e .
python -m unittest discover -s tests -q
node cursor-extension/test-extension.js
```

Integration tests requiring Syncthing or Codex are opt-in and use disposable
profiles. On Linux, `NEXUS_TEST_SYNCTHING=1` enables local Syncthing integration
tests. They never need a user's real profile or VPS.

CoSyncX code is licensed under [MIT](LICENSE). The adapted Syncthing interface
and bundled Syncthing retain their [MPL-2.0 license](src/cosyncx_gui/LICENSE).
