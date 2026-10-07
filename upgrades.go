package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/algorand/go-algorand-sdk/v2/types"
	"golang.org/x/sync/errgroup"
)

// voteRounds is consensus UpgradeVoteRounds: 10000 since v7, never overridden. A proposal at P
// is pending in the headers of exactly [P, V-1], V = P + voteRounds, and the next one can start
// at V+1 at the earliest. So a stride of voteRounds sees every proposal once or more, and V
// identifies it. The header at P is checked, in case a network ever changes the param.
const voteRounds = 10000

// backfillStep caps the samples per write, so a failed fetch during a backfill loses one step.
const backfillStep = 500

// syncUpgrades records upgrades up to hi, backfilling in steps.
func syncUpgrades(ctx context.Context, db *sql.DB, src source, concurrency int, hi uint64) error {
	for {
		next, err := upgradeStep(ctx, db, src, concurrency, hi)
		if err != nil || next > hi {
			return err
		}
	}
}

// upgradeStep samples every voteRounds-th round up to hi (at most backfillStep of them) for new
// upgrade proposals, then settles each open one as far as that allows. It writes only if every
// fetch succeeded, and returns the next round to sample.
func upgradeStep(ctx context.Context, db *sql.DB, src source, concurrency int, hi uint64) (uint64, error) {
	var next uint64
	if err := db.QueryRowContext(ctx, "SELECT next_sample FROM upgrade_sync").Scan(&next); err != nil {
		return 0, err
	}
	hi = min(hi, next+backfillStep*voteRounds-1) // the same as an earlier call with a lower hi
	ups, err := queryUpgrades(ctx, db, "WHERE passed IS NULL OR (passed AND switch_ts IS NULL) ORDER BY rnd")
	if err != nil {
		return 0, err
	}

	var samples []uint64
	for r := next; r <= hi; r += voteRounds {
		samples = append(samples, r)
	}
	states := make([]types.UpgradeState, len(samples))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for i, r := range samples {
		g.Go(func() error {
			b, err := src.get(gctx, r, true, false)
			states[i] = b.UpgradeState
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}

	// Rows are added to open as soon as they're seen: during a backfill an approved proposal
	// shows in up to 26 samples, and must be recorded once.
	open := map[uint64]bool{}
	rows := make([]*upgrade, len(ups))
	for i := range ups {
		rows[i], open[ups[i].VoteBefore] = &ups[i], true
	}
	var changed []*upgrade
	for _, s := range states {
		v := uint64(s.NextProtocolVoteBefore)
		if s.NextProtocol == "" || open[v] {
			continue
		}
		u := &upgrade{Rnd: v - voteRounds, From: s.CurrentProtocol, Proto: s.NextProtocol, VoteBefore: v, SwitchOn: uint64(s.NextProtocolSwitchOn)}
		b, err := src.get(ctx, u.Rnd, true, false)
		if err != nil {
			return 0, err
		}
		if b.UpgradePropose != u.Proto {
			return 0, fmt.Errorf("round %d proposes %q, not %q: is UpgradeVoteRounds still %d?", u.Rnd, b.UpgradePropose, u.Proto, voteRounds)
		}
		u.Ts = uint64(b.TimeStamp)
		open[u.VoteBefore] = true
		rows = append(rows, u)
		changed = append(changed, u)
	}
	for i, u := range rows {
		ok, err := settle(ctx, src, u, hi)
		if err != nil {
			return 0, err
		}
		if ok && i < len(ups) { // new rows are already in changed
			changed = append(changed, u)
		}
	}

	nextSample := next + uint64(len(samples))*voteRounds
	if nextSample == next && len(changed) == 0 {
		return next, nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, u := range changed {
		if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO upgrades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			u.Rnd, u.Ts, u.From, u.Proto, u.VoteBefore, u.SwitchOn, u.Approvals, u.Passed, u.SwitchTs); err != nil {
			return 0, err
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE upgrade_sync SET next_sample = ?", nextSample); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	for _, u := range changed {
		slog.Info("upgrade proposed", "round", u.Rnd, "from", u.From, "proto", u.Proto, "status", u.status())
	}
	return nextSample, nil
}

// settle records u's vote outcome once hi reaches V, and its switch once hi reaches S, from
// header-only fetches. It reports whether u changed.
func settle(ctx context.Context, src source, u *upgrade, hi uint64) (bool, error) {
	changed := false
	if u.Passed == nil && u.VoteBefore <= hi {
		last, err := src.get(ctx, u.VoteBefore-1, true, false) // approvals count in rounds < V
		if err != nil {
			return false, err
		}
		at, err := src.get(ctx, u.VoteBefore, true, false) // cleared at V if rejected; switched if S == V
		if err != nil {
			return false, err
		}
		approvals, passed := uint64(last.NextProtocolApprovals), at.NextProtocol != "" || at.CurrentProtocol != u.From
		u.Approvals, u.Passed, changed = &approvals, &passed, true
	}
	if u.Passed != nil && *u.Passed && u.SwitchTs == nil && u.SwitchOn <= hi {
		b, err := src.get(ctx, u.SwitchOn, true, false)
		if err != nil {
			return false, err
		}
		if b.CurrentProtocol != u.Proto {
			return false, fmt.Errorf("round %d is on %q, want the approved %q", u.SwitchOn, b.CurrentProtocol, u.Proto)
		}
		ts := uint64(b.TimeStamp)
		u.SwitchTs, changed = &ts, true
	}
	return changed, nil
}
