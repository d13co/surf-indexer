#!/bin/sh
# Debian postinst. A fresh install adds the surf-indexer user and starts
# nothing. An upgrade restarts the running instances; they close the DB on SIGTERM.
set -e
[ "$1" = configure ] || exit 0

getent passwd surf-indexer >/dev/null ||
	useradd --system --user-group --home-dir /var/lib/surf-indexer --shell /usr/sbin/nologin surf-indexer

[ -d /run/systemd/system ] || exit 0
systemctl daemon-reload
# Best effort: a failed restart must not fail the upgrade; systemd shows why.
if [ -n "${2:-}" ]; then
	systemctl try-restart 'surf-indexer@*' || echo "surf-indexer: restart failed; see systemctl status 'surf-indexer@*'" >&2
fi
