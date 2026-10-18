#!/bin/sh
# Debian prerm. Removing the package stops the running instances, which would
# otherwise keep running a deleted binary. An upgrade leaves them to postinst,
# which restarts them. The DBs, env files and user stay.
set -e
[ "$1" = remove ] || exit 0
[ -d /run/systemd/system ] || exit 0
# The pattern matches the instances systemd has loaded: the running ones.
systemctl stop 'surf-indexer@*.service' || echo "surf-indexer: stop failed; see systemctl status 'surf-indexer@*'" >&2
