"""Conservative process check used before guarded offline restore work.

This is not a lock: callers must also acquire Codex writer coordination and
recheck immediately before mutation. Unsupported platforms fail closed.
"""
from pathlib import PurePath
import subprocess
import sys


class QuiescenceError(Exception):
    pass


def parse_processes(output: str) -> bool:
    """Return True if a Cursor or Codex process is present; never return paths."""
    if not isinstance(output, str) or not output.strip():
        raise QuiescenceError('Process listing unavailable')
    for line in output.splitlines():
        fields = line.strip().split(None, 1)
        if len(fields) != 2 or not fields[0].isdigit():
            raise QuiescenceError('Unexpected process listing')
        command = fields[1].strip().casefold()
        name = PurePath(command).name
        if ('/cursor.app/' in command or name in {'cursor', 'codex', 'codex-code-mode-host'}
                or name.startswith('cursor helper')):
            return True
    return False


def is_quiet() -> bool:
    if sys.platform not in ('darwin', 'linux'):
        raise QuiescenceError('Process check is not verified on this platform')
    try:
        result = subprocess.run(['/bin/ps', '-axo', 'pid=,comm='], capture_output=True,
                                text=True, timeout=5, check=True)
    except (OSError, subprocess.SubprocessError) as exc:
        raise QuiescenceError('Cannot verify processes') from exc
    return not parse_processes(result.stdout)
