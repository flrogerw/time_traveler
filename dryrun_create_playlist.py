"""
Run playlists.create_playlist end to end WITHOUT changing anything: every commit() is a no-op, the
whole run is rolled back, and the .m3u goes to a temp directory instead of the production playlist
path. Takes exactly create_playlist's own arguments, e.g.

    python -m dryrun_create_playlist --hostname TV-SYN-5 --year 1976 --sim-date 2026-10-24 \
        --dow 5 --start 07:00:00 --duration 05:00:00 --duration-aware

Prints the temp playlist path when done. Nothing the run does to schedule_template, broadcast_log,
time_slot_schedules or schedule_overrides survives, so it can be repeated freely -- it sees the real
DB's current state (including rows a previous REAL run left behind), but never adds to it.
"""
import os
import sys
import tempfile

import playlists.create_playlist as create_playlist
from playlists.classes.Playlists import Playlists


class _RolledBackConnection:
    """Context manager handing create_playlist a connection that can never persist anything."""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, *exc):
        self.conn.rollback()
        self.conn.close()
        return False


def main() -> None:
    real_connect = create_playlist.get_db_connection
    out_dir = tempfile.mkdtemp(prefix="dryrun_playlist_")

    def dry_connect():
        conn = real_connect()
        conn.commit = lambda: None  # code under test commits mid-flow; none of it may stick
        return _RolledBackConnection(conn)

    create_playlist.get_db_connection = dry_connect
    # is_dev_mode=True writes ./<hostname>_playlist.m3u into the cwd, never the production path.
    create_playlist.Playlists = lambda db, hostname: Playlists(db, hostname, is_dev_mode=True)
    os.chdir(out_dir)
    create_playlist.main()
    print(f"\nDRY RUN -- nothing persisted. Playlist: {os.path.join(out_dir, sys.argv[sys.argv.index('--hostname') + 1] + '_playlist.m3u')}")


if __name__ == "__main__":
    main()
