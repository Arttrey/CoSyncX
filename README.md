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
The experimental automatic change gate likewise does not run a receiver: it
waits for a connected, settled revision from a configured peer and folder; it
does not use changes to the local Codex profile as a trigger. The gate refuses
to produce a reload event without a separately confirmed successful restore
and indexing. It is experimental, does not poll Syncthing by itself, and must
not persist a remote sequence alone across Syncthing index resets.
The direct-folder pipeline is currently limited to disposable mock profiles;
it does not receive real chats. The optional macOS menu-bar status is read-only
and reports Syncthing transfer progress, not successful restoration.
The `direct-watch --experimental` prototype handles new and active chats when
Cursor and Codex are quiet, including a linear multi-page source when no other
history references its physical pages. A differing source history or title is
applied after backing up the local state. Locally archived IDs stay archived;
source archives are not transferred. Complete active relationship graphs are
applied after indexing. When a source workspace is unavailable, a leaf chat
uses the first configured local mapping root, while historical contexts remain
unchanged. Missing workspaces and local files are recorded per chat in the
private `missing-paths.json` under the backup root; Cursor displays a warning
and the report location. The report retains prior warnings for review.
`--exclude-thread-id ID` keeps a selected local conversation and its dependent
sessions untouched. This is useful for a conversation continued on another
device; changing its display name does not change its ID.
When an individual writer lock is held, the receiver defers that conversation
and its dependent sessions, processes eligible independent chats, and retries
without requiring another Syncthing change. The current real-profile watcher
still waits while Cursor or Codex is running; concurrent use has only been
tested on disposable profiles.
An indexed restore keeps its reload event until the status is published. An
interrupted indexing pass retains its authenticated metadata so it can finish
before a newer source revision is processed, provided the local state has not
changed and no affected thread has since been excluded or archived. Older
incomplete journals without this metadata remain pending for review. An
interrupted write is retried only when the backup, databases, titles and
affected histories still match the expected state; otherwise it waits for
review. The watcher is not installed or enabled automatically and must not be
used on a real profile yet.
The mock pipeline can wait for histories, titles and relationship metadata as
three independent folders. It defers any nonempty relationship graph until a
complete, verified child-restore transaction is available.
The mock pipeline can test a flattened linear multi-page history and compares
visible message and inline-image hashes against Codex's index. Flattening is
experimental: external page references and later updates are not yet proven.
The mock projection reset touches an undocumented Codex database and refuses
unknown schemas; it must not be used on a real profile. The Cursor status-file
and reload handler are prepared but are not connected to a live receiver.
An isolated mock can restore one archived, single-page conversation after a
backup that preserves duplicate local versions. Archived descendants absent
from the source are returned to the archive; incoming descendants and
multi-page sources remain unsupported. On failure it attempts a mock-only
rollback; if affected data no longer matches expected versions, it keeps an
authenticated journal and blocks further backup rotation for review.
The mock receiver leaves a locally archived chat archived when its incoming
counterpart is active. Incoming children, multiple parent groups, nonempty
relationship graphs, and path-report delivery still require a complete group
implementation.
The experimental `cosyncx prepare-lineage` command rewrites structured project
paths and dependent byte offsets for a complete paginated thread into a new
isolated directory. It does not import chats or rebuild the live Codex index.
An explicit `--missing-cwd-fallback` inside the configured local root lets that
preparation preserve unavailable historical working directories and write a
private path report. The mock archive importer retains an authenticated local
report for missing working directories and local image references and returns
its path; showing it in Cursor is not implemented yet.
`cosyncx receive-plan --incoming PARENT --profile CODEX_HOME --map-root SOURCE TARGET`
counts candidate conversations, subagent threads and existing archived or active
IDs without changing the profile. This inventory does not establish that the
Syncthing transfer is complete or that chats are indexed.
`cosyncx archive-plan --profile CODEX_HOME --thread-id ID` counts affected
threads and local history files before an unarchive operation. It is read-only;
an actual restore would need to recheck and back up the full affected set while
the profile is safely offline.
`cosyncx active-titles --profile CODEX_HOME --output PRIVATE_DIR/session_index.jsonl`
exports only titles whose threads are active and have session files. Run it
again to refresh the export; the command does not install a scheduler or set up
Syncthing. If an export fails, the command removes a previous export at that
exact filename in a private directory so it is not silently kept stale.

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
