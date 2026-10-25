package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	"github.com/algorand/go-algorand-sdk/v2/types"
	"golang.org/x/sync/errgroup"
)

type algods []*algod.Client

var retryWait = 2800 * time.Millisecond // a var so tests can skip the waits

// slowInsert is how long a batch's insert transaction may take before it's warned of, while it runs
// and on its inserted line. At the tip one takes milliseconds; much longer means the DB stalled (a
// checkpoint, the disk). A var so tests can set it.
var slowInsert = 5 * time.Second

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

// blockCert is algod's /v2/blocks msgpack body, keeping only the cert fields we use (go-algorand
// agreement/bundle.go, proposal.go). The tagged embed decodes as the "block" key, not inline
// (go-codec helper.go), and still promotes the header fields.
type blockCert struct {
	types.Block `codec:"block"`
	Cert        struct {
		Prop struct {
			OProp types.Address `codec:"oprop"`
		} `codec:"prop"`
		Vote []struct {
			Snd types.Address `codec:"snd"`
		} `codec:"vote"`
	} `codec:"cert"`
}

// source is where blocks come from: algod over REST, or an archival ledger.block.sqlite.
type source struct {
	// get fetches round rnd of genesisID's network, header only or in full, with its cert if cert is set.
	get func(ctx context.Context, rnd uint64, headerOnly, cert bool) (blockCert, error)
	// tip returns the last round available, waiting for one past next-1 if the source can.
	tip func(ctx context.Context, next uint64) (uint64, error)
}

func algodSource(a algods, genesisID string) source {
	return source{
		get: func(ctx context.Context, rnd uint64, headerOnly, cert bool) (blockCert, error) {
			return call(ctx, a, func(c *algod.Client) (b blockCert, err error) {
				if headerOnly && !cert {
					b.Block, err = c.Block(rnd).HeaderOnly(true).Do(ctx)
				} else { // only a full msgpack response carries the cert; decoded as the SDK does
					var raw []byte
					if raw, err = c.BlockRaw(rnd).Do(ctx); err == nil {
						err = msgpack.NewLenientDecoder(bytes.NewReader(raw)).Decode(&b)
					}
				}
				if err == nil && b.GenesisID != genesisID { // a misconfigured failover algod: try the next one
					err = fmt.Errorf("round %d: algod is on %q, want %q", rnd, b.GenesisID, genesisID)
				}
				if err == nil && cert && rnd > 0 && (b.Cert.Prop.OProp.IsZero() || len(b.Cert.Vote) == 0) { // e.g. a stripping proxy: try the next one
					err = fmt.Errorf("round %d: algod sent no cert", rnd)
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

// fetchBlock fetches round rnd with what track asks for, and its cert voters if voters is set.
// Header only once payouts are on, unless the cert or (fullBlocks) the payset is needed. The cert is
// read only for its proposer before payouts, or for voters: a ledger rebuild would otherwise read
// every post-payout cert.
func fetchBlock(ctx context.Context, src source, rnd uint64, voters bool) (block, error) {
	p := payouts.Load()
	b, err := src.get(ctx, rnd, p && !track.fullBlocks(), voters || track.Proposers && !p)
	if err == nil && p && b.Proposer.IsZero() {
		// not expected once payouts are on; recompute from the payset
		b, err = src.get(ctx, rnd, false, voters || track.Proposers)
	}
	if err != nil {
		return block{}, err
	}
	fees, prop := uint64(b.FeesCollected), b.Proposer
	if prop.IsZero() {
		fees, prop = blockFees(b.Block), b.Cert.Prop.OProp
	} else {
		payouts.Store(true)
	}
	// A cert that silently decodes to nothing must not lock NULLs into the DB. Genesis has no cert.
	if rnd > 0 && (track.Proposers && prop.IsZero() || voters && len(b.Cert.Vote) == 0) {
		return block{}, fmt.Errorf("round %d: no proposer or votes in its cert", rnd)
	}
	blk := block{Rnd: rnd, Ts: uint64(b.TimeStamp), Tc: b.TxnCounter, Fees: fees}
	// tc only started at an upgrade (mainnet round 3317192, at 0), so before it count the payset;
	// none had inner txns yet. A header-only block has tc > 0: payouts came long after tc.
	if b.TxnCounter == 0 {
		blk.txns = uint64(len(b.Payset))
	}
	if track.Proposers {
		if !prop.IsZero() {
			blk.prop = prop.String()
		}
		blk.pp = uint64(b.ProposerPayout)
		for _, a := range b.AbsentParticipationAccounts {
			blk.absent = append(blk.absent, a.String())
		}
	}
	if voters {
		for _, v := range b.Cert.Vote {
			blk.voters = append(blk.voters, v.Snd.String())
		}
	}
	if track.Lsigs {
		blk.lsigs = lsigUses(b.Block)
	}
	if track.Msigs {
		blk.msigs = msigUses(b.Block)
	}
	return blk, nil
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
	next := max(uint64(hi+1), startRound)
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
				r := next + uint64(i)
				rows[i], err = fetchBlock(gctx, src, r, track.VoterRounds > 0 && r+track.VoterRounds > tip)
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return 0, err
		}
		ins, from, to, n := time.Now(), next, end, len(rows)
		// Warns during a stall too: the inserted line only comes once it's over.
		watch := time.AfterFunc(slowInsert, func() { slog.Warn("insert slow", "from", from, "to", to, "blocks", n, "after", slowInsert.String()) })
		err := insertBatch(ctx, db, rows)
		watch.Stop()
		took := time.Since(ins)
		if err != nil {
			return 0, fmt.Errorf("insert %d-%d after %s: %w", from, to, took.Round(time.Millisecond), err)
		}
		attrs := []any{"from", next, "to", end, "blocks", len(rows), "headerOnly", payouts.Load() && !track.fullBlocks(),
			"insertMs", float64(took.Microseconds()) / 1000}
		if next != tip { // at tip one block arrives per long-poll, so a rate would only be fetch+insert latency
			attrs = append(attrs, "blkPerSec", math.Round(float64(len(rows))/time.Since(start).Seconds()))
		}
		level := slog.LevelInfo
		if took > slowInsert {
			level = slog.LevelWarn
		}
		slog.Log(ctx, level, "inserted", attrs...)
		next = end + 1
	}
	return min(next-1, tip), nil // a START_ROUND past the tip stored nothing yet
}
