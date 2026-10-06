package main

import (
	"context"
	"database/sql"
	"log"
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
			log.Printf("retry %d: %v", try, err)
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

func fetchBlock(ctx context.Context, a algods, rnd uint64) (block, error) {
	get := func(headerOnly bool) (types.Block, error) {
		return call(ctx, a, func(c *algod.Client) (types.Block, error) { return c.Block(rnd).HeaderOnly(headerOnly).Do(ctx) })
	}
	headerOnly := payouts.Load()
	b, err := get(headerOnly)
	if err == nil && headerOnly && b.Proposer.IsZero() {
		b, err = get(false) // not expected once payouts are on; recompute from the payset
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

// ingest catches up to the algod tip in batches, then follows it block by block.
// Each batch is fetched in full before one insert transaction, so the table never has gaps.
func ingest(ctx context.Context, db *sql.DB, a algods, concurrency, chunk int) {
	for ctx.Err() == nil {
		if err := syncToTip(ctx, db, a, concurrency, uint64(chunk)); err != nil && ctx.Err() == nil {
			log.Printf("ingest: %v", err)
			sleep(ctx, retryWait)
		}
	}
}

func syncToTip(ctx context.Context, db *sql.DB, a algods, concurrency int, chunk uint64) error {
	_, _, hi, err := rounds(ctx, db)
	if err != nil {
		return err
	}
	next := uint64(hi + 1)
	// Returns at once while behind; long-polls for the next block when caught up.
	st, err := call(ctx, a, func(c *algod.Client) (models.NodeStatus, error) {
		return c.StatusAfterBlock(max(next, 1) - 1).Do(ctx)
	})
	if err != nil {
		return err
	}
	for next <= st.LastRound {
		start, end := time.Now(), min(next+chunk-1, st.LastRound)
		rows := make([]block, end-next+1)
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(concurrency)
		for i := range rows {
			g.Go(func() (err error) {
				rows[i], err = fetchBlock(gctx, a, next+uint64(i))
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		if err := insertBatch(ctx, db, rows); err != nil {
			return err
		}
		log.Printf("inserted %d..%d (%d) %.0f blk/s headerOnly=%t", next, end, len(rows),
			float64(len(rows))/time.Since(start).Seconds(), payouts.Load())
		next = end + 1
	}
	return nil
}
