package main

import (
	"context"
	"database/sql"

	"github.com/algorand/go-algorand-sdk/v2/crypto"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

// rekeysTable is kept out of schema: its presence is the stored Rekeys flag. Unlike lsigs and msigs it
// can be turned on for a DB with blocks, which the backfill then fills (backfill.go).
// No PK, as lsigs: mergeRekeys dedupes a batch and MERGE matches with a hash join.
const rekeysTable = `CREATE TABLE IF NOT EXISTS rekeys(
	acct      VARCHAR  NOT NULL, -- the rekeyed account: the txn's sender
	auth      VARCHAR  NOT NULL, -- its new authorizer (RekeyTo); acct itself for a rekey back or a close-out that cleared one
	txns      UBIGINT  NOT NULL, -- txns that set acct's auth to auth (rekeys, plus close-outs on a self row), inner included
	first_rnd UINTEGER NOT NULL,
	last_rnd  UINTEGER NOT NULL
)`

// rekeyFrom is each network's first round that can hold a rekey: where consensus v24
// (SupportRekeying) switched on. The backfill skips the rounds before it. 0 on other networks.
var rekeyFrom = map[string]uint64{"mainnet-v1.0": 8734690, "testnet-v1.0": 8841645, "betanet-v1.0": 5107702}

// rekeyUse is a block's txns that set acct's authorizer to auth.
type rekeyUse struct {
	acct, auth string
	n          uint64
}

// rekeyUses returns the block's rekeys, inner txns included, folded per (acct, auth). A rekey to the
// sender itself is a self use (a rekey back), and so is a close-out of a rekeyed account, which
// deletes the account and with it its authorizer (go-algorand ledger/apply).
func rekeyUses(b types.Block) []rekeyUse {
	var uses []rekeyUse
	idx := map[[2]types.Address]int{}
	add := func(acct, auth types.Address) {
		k := [2]types.Address{acct, auth}
		if i, ok := idx[k]; ok {
			uses[i].n++
			return
		}
		idx[k] = len(uses)
		uses = append(uses, rekeyUse{acct.String(), auth.String(), 1})
	}
	// walk records t's changes, given whether its sender was rekeyed before it, then its inner txns'.
	var walk func(t *types.SignedTxnWithAD, rekeyed bool)
	walk = func(t *types.SignedTxnWithAD, rekeyed bool) {
		snd := t.Txn.Sender
		if to := t.Txn.RekeyTo; !to.IsZero() { // applied before the txn's own effects
			add(snd, to)
			rekeyed = to != snd
		}
		if rekeyed && t.Txn.Type == types.PaymentTx && !t.Txn.CloseRemainderTo.IsZero() {
			add(snd, snd)
		}
		if len(t.EvalDelta.InnerTxns) == 0 {
			return
		}
		// An inner txn's sender is the app's account or one rekeyed to it (go-algorand logic
		// authorizedSender), so it was rekeyed iff it isn't the app's.
		app := uint64(t.Txn.ApplicationID)
		if app == 0 { // the txn created the app
			app = uint64(t.ApplyData.ApplicationID)
		}
		appAddr := crypto.GetApplicationAddress(app)
		for i := range t.EvalDelta.InnerTxns {
			itx := &t.EvalDelta.InnerTxns[i]
			walk(itx, itx.Txn.Sender != appAddr)
		}
	}
	for i := range b.Payset {
		t := &b.Payset[i]
		// A rekeyed sender names its authorizer in sgnr, so the block (all applied) shows it.
		walk(&t.SignedTxnWithAD, !t.AuthAddr.IsZero() && t.AuthAddr != t.Txn.Sender)
	}
	return uses
}

// mergeRekeys folds rows' rekeys into rekeys. Live ingest and the backfill write rounds in any
// order, so the merge gives the same result in any order: counts add, rounds widen.
func mergeRekeys(ctx context.Context, tx *sql.Tx, rows []block) error {
	var acct, auth []string
	var txns, first, last []uint64
	seen := map[[2]string]int{}
	for _, r := range rows {
		for _, u := range r.rekeys {
			k := [2]string{u.acct, u.auth}
			i, ok := seen[k]
			if !ok {
				i, seen[k] = len(acct), len(acct)
				acct, auth = append(acct, u.acct), append(auth, u.auth)
				txns, first, last = append(txns, 0), append(first, r.Rnd), append(last, r.Rnd)
			}
			txns[i] += u.n
			first[i], last[i] = min(first[i], r.Rnd), max(last[i], r.Rnd)
		}
	}
	if len(acct) == 0 { // an empty list binds as SQLNULL[]
		return nil
	}
	_, err := tx.ExecContext(ctx, `MERGE INTO rekeys t
		USING (SELECT unnest($1) AS acct, unnest($2) AS auth, unnest($3) AS txns, unnest($4) AS first_rnd, unnest($5) AS last_rnd) s
		ON t.acct = s.acct AND t.auth = s.auth
		WHEN MATCHED THEN UPDATE SET txns = t.txns + s.txns, first_rnd = least(t.first_rnd, s.first_rnd), last_rnd = greatest(t.last_rnd, s.last_rnd)
		WHEN NOT MATCHED THEN INSERT VALUES (s.acct, s.auth, s.txns, s.first_rnd, s.last_rnd)`,
		acct, auth, txns, first, last)
	return err
}
