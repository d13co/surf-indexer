package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/types"
	"golang.org/x/sync/errgroup"
)

type algods []*algod.Client

const retryWait = 2800 * time.Millisecond

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// call tries each algod in turn, for up to 3 rounds, retryWait apart.
func call[T any](ctx context.Context, a algods, fn func(*algod.Client) (T, error)) (res T, err error) {
	for try := range 3 {
		if try > 0 {
			slog.Warn("retry", "try", try, "err", err)
			sleep(ctx, retryWait)
		}
		for _, c := range a {
			if res, err = fn(c); err == nil || ctx.Err() != nil {
				return res, err
			}
		}
	}
	return res, err
}

// txnFees is the fee paid by t and its inner txns, minus any sent by the FeeSink:
// the same rule consensus uses for the header's fc (ledger/eval takeFee).
func txnFees(t types.SignedTxnWithAD, feeSink types.Address) (sum uint64) {
	if t.Txn.Sender != feeSink {
		sum = uint64(t.Txn.Fee)
	}
	for _, itx := range t.EvalDelta.InnerTxns {
		sum += txnFees(itx, feeSink)
	}
	return sum
}

// blockFees recomputes fc from the payset, for blocks from before the incentives upgrade.
func blockFees(b types.Block) (sum uint64) {
	for _, t := range b.Payset {
		sum += txnFees(t.SignedTxnWithAD, b.FeeSink)
	}
	return sum
}

// payouts flips on at the first block with a proposer (prp). prp and fc are both
// populated iff Payouts.Enabled, so from then on the header alone gives the fees.
var payouts atomic.Bool

// source is where blocks come from: algod over REST, or an archival ledger.block.sqlite.
type source struct {
	// get fetches round rnd of genesisID's network, header only or in full.
	get func(ctx context.Context, rnd uint64, headerOnly bool) (types.Block, error)
	// tip returns the last round available, waiting for one past next-1 if the source can.
	tip func(ctx context.Context, next uint64) (uint64, error)
}

func algodSource(a algods, genesisID string) source {
	return source{
		get: func(ctx context.Context, rnd uint64, headerOnly bool) (types.Block, error) {
			return call(ctx, a, func(c *algod.Client) (types.Block, error) {
				b, err := c.Block(rnd).HeaderOnly(headerOnly).Do(ctx)
				if err == nil && b.GenesisID != genesisID { // a misconfigured failover algod: try the next one
					err = fmt.Errorf("round %d: algod is on %q, want %q", rnd, b.GenesisID, genesisID)
				}
				return b, err
			})
		},
		// Returns at once while behind; long-polls for the next block when caught up.
		tip: func(ctx context.Context, next uint64) (uint64, error) {
			st, err := call(ctx, a, func(c *algod.Client) (models.NodeStatus, error) {
				return c.StatusAfterBlock(max(next, 1) - 1).Do(ctx)
			})
			return st.LastRound, err
		},
	}
}

func fetchBlock(ctx context.Context, src source, rnd uint64) (block, error) {
	headerOnly := payouts.Load()
	b, err := src.get(ctx, rnd, headerOnly)
	if err == nil && headerOnly && b.Proposer.IsZero() {
		b, err = src.get(ctx, rnd, false) // not expected once payouts are on; recompute from the payset
	}
	if err != nil {
		return block{}, err
	}
	fees := uint64(b.FeesCollected)
	if b.Proposer.IsZero() {
		fees = blockFees(b)
	} else {
		payouts.Store(true)
	}
	return block{Rnd: rnd, Ts: uint64(b.TimeStamp), Tc: b.TxnCounter, Fees: fees}, nil
}

// ingest catches up to the source's tip in batches, then follows it block by block, syncing
// upgrades after each pass. A failing upgrades sync backs off on its own, so fees don't lag behind it.
func ingest(ctx context.Context, db *sql.DB, src source, concurrency, chunk int) {
	var upgradesAt time.Time
	for ctx.Err() == nil {
		hi, err := syncToTip(ctx, db, src, concurrency, uint64(chunk))
		if err != nil && ctx.Err() == nil {
			slog.Error("ingest", "err", err)
			sleep(ctx, retryWait)
		}
		if err == nil && time.Now().After(upgradesAt) {
			if err := syncUpgrades(ctx, db, src, concurrency, hi); err != nil && ctx.Err() == nil {
				slog.Error("upgrades", "err", err, "retryIn", "10m")
				upgradesAt = time.Now().Add(10 * time.Minute)
			}
		}
	}
}

// syncToTip ingests from the last stored round up to the source's tip, and returns the last stored round.
// Each batch is fetched in full before one insert transaction, so the table never has gaps.
func syncToTip(ctx context.Context, db *sql.DB, src source, concurrency int, chunk uint64) (uint64, error) {
	_, _, hi, err := rounds(ctx, db)
	if err != nil {
		return 0, err
	}
	next := uint64(hi + 1)
	tip, err := src.tip(ctx, next)
	if err != nil {
		return 0, err
	}
	for next <= tip {
		start, end := time.Now(), min(next+chunk-1, tip)
		rows := make([]block, end-next+1)
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(concurrency)
		for i := range rows {
			g.Go(func() (err error) {
				rows[i], err = fetchBlock(gctx, src, next+uint64(i))
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return 0, err
		}
		if err := insertBatch(ctx, db, rows); err != nil {
			return 0, err
		}
		attrs := []any{"from", next, "to", end, "blocks", len(rows), "headerOnly", payouts.Load()}
		if next != tip { // at tip one block arrives per long-poll, so a rate would only be fetch+insert latency
			attrs = append(attrs, "blkPerSec", math.Round(float64(len(rows))/time.Since(start).Seconds()))
		}
		slog.Info("inserted", attrs...)
		next = end + 1
	}
	return next - 1, nil
}
