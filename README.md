# CoSyncX

CoSyncX is an experimental, standalone companion to Syncthing for moving
project files and preparing Codex chat snapshots across devices. Each device
chooses its own project root and local configuration; it does not require a
particular username, drive, or directory name. Syncthing remains a separate
application and transports the shared folders.

**Prototype status:** use synthetic data only. Automatic chat, Git, and secret
restoration is currently implemented only for isolated Linux test profiles.
The Cursor interface, Windows, macOS, and a real VPS have not been validated.
This project is not ready to synchronize real chats, secrets, or workspaces.

The Linux demonstration starts temporary Syncthing nodes, a test relay, and
isolated Codex app-server profiles. It checks encrypted snapshots, path
mapping, mock Git and secrets, and a dependency report; no Cursor window is
opened. See [QUICKSTART.md](QUICKSTART.md) for the test and its limitations.
More detailed experiment notes are in [EXPERIMENTS.md](EXPERIMENTS.md).

## Install the command-line companion

Python 3.11 or newer is required. In a checkout, create and activate a virtual
environment, then install the package:

```sh
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install .
cosyncx --help
```

The CLI is a local process; installing it does not configure or start
Syncthing. `cosyncx init-config --help` describes per-device paths and
`cosyncx run --help` describes the observer. Do not point them at real data
until the relevant workflows have been independently verified.

## Design and current boundaries

- A Syncthing folder transports project files, and a separate exchange folder
  transports encrypted snapshots. A relay can keep data available while a
  device is offline. Folder modes are configured in Syncthing itself.
- Every device has its own project root, Codex profile, key, and local state.
  Paths from another device are mapped to the configured local project root;
  historical message text is not rewritten.
- `stignore.example` is a starting point, not a universal ignore policy.
  Review exclusions per project: dependency manifests and lock files should
  travel, while local environments and caches normally need rebuilding.
- Secrets need explicit selection, client-side encryption and key management.
  Name-based detection is incomplete; Syncthing alone does not provide the
  client-side secret handling required by this prototype.
- The included editor extension reports experimental status. It is not yet
  verified in Cursor and does not confirm that chats are visible or current.
- Concurrent edits, conflicts, platform-specific filenames, binary libraries,
  and writes to a running Codex profile still need broader validation.

## Development

Run the standard local suite with `python -m unittest discover -q`. Tests that
need Syncthing or Codex are opt-in and use synthetic data; the default suite
reports them as skipped. See [QUICKSTART.md](QUICKSTART.md) for the Linux
integration demonstration. Contributions should preserve the isolation of
test profiles and must not silently write to a user's live Codex state.

Licensed under the [MIT License](LICENSE).
