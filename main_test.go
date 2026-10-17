package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/crypto"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

func stxn(sender byte, fee uint64, inner ...types.SignedTxnWithAD) types.SignedTxnWithAD {
	var t types.SignedTxnWithAD
	t.Txn.Sender[0] = sender
	t.Txn.Fee = types.MicroAlgos(fee)
	t.EvalDelta.InnerTxns = inner
	return t
}

func TestBlockFees(t *testing.T) {
	var b types.Block
	b.FeeSink[0] = 'S'
	b.Payset = []types.SignedTxnInBlock{
		{SignedTxnWithAD: stxn('A', 1000, stxn('B', 2000, stxn('C', 3000)), stxn('S', 7))},
		{SignedTxnWithAD: stxn('S', 500)}, // FeeSink sender: not counted
		{SignedTxnWithAD: stxn('D', 4000)},
	}
	if got := blockFees(b); got != 10000 {
		t.Fatalf("blockFees = %d, want 10000", got)
	}
}

// fakeAlgod serves *blocks (stamped with genesisID) and counts header-only requests.
// A full fetch carries the cert, as algod's msgpack does.
func fakeAlgod(t *testing.T, genesisID string, blocks *[]blockCert, headerOnly *atomic.Int32) *algod.Client {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v2/status/wait-for-block-after/") {
			json.NewEncoder(w).Encode(models.NodeStatus{LastRound: uint64(len(*blocks) - 1)})
			return
		}
		var rnd int
		fmt.Sscanf(r.URL.Path, "/v2/blocks/%d", &rnd)
		b := (*blocks)[rnd]
		b.GenesisID = genesisID
		if r.URL.Query().Get("header-only") == "true" {
			headerOnly.Add(1)
			w.Write(msgpack.Encode(models.BlockResponse{Block: types.Block{BlockHeader: b.BlockHeader}}))
			return
		}
		w.Write(msgpack.Encode(b))
	}))
	t.Cleanup(srv.Close)
	c, err := algod.MakeClient(srv.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// mk builds round r with payset fees r*1000 + 1 and, past genesis, a cert proposed by 'O' and voted
// by 'V' and 'W'. With payouts on it has proposer 'P', fc = r*7 and pp = r*3, so the stored fees and
// proposer show which path was taken. Rounds 2 and 7 mark 'E' absent. From round 5, an escrow lsig
// (mkLsig) sends a fee 0 txn, so either fee path is unchanged.
func mk(r int, payouts bool) blockCert {
	var b blockCert
	b.Round, b.TimeStamp, b.TxnCounter = types.Round(r), int64(1000+r), uint64(2*r)
	b.FeeSink[0] = 'S'
	b.Payset = []types.SignedTxnInBlock{{SignedTxnWithAD: stxn('A', uint64(r)*1000, stxn('B', 1))}}
	if r >= 5 {
		var l types.SignedTxnInBlock
		l.Lsig = mkLsig(r)
		l.Txn.Sender = crypto.LogicSigAddress(l.Lsig)
		b.Payset = append(b.Payset, l)
		var m types.SignedTxnInBlock // K1 signs from round 7, K2 never
		m.Msig = mkMsig(r >= 7)
		b.Payset = append(b.Payset, m)
	}
	if r > 0 {
		b.Cert.Prop.OProp[0] = 'O'
		for _, c := range "VW" {
			var v struct {
				Snd types.Address `codec:"snd"`
			}
			v.Snd[0] = byte(c)
			b.Cert.Vote = append(b.Cert.Vote, v)
		}
	}
	if payouts {
		b.Proposer[0], b.FeesCollected, b.ProposerPayout = 'P', types.MicroAlgos(r*7), types.MicroAlgos(r*3)
	}
	if r == 2 || r == 7 {
		b.AbsentParticipationAccounts = []types.Address{{'E'}}
	}
	return b
}

// mkLsig is round r's escrow: pushint r, so every round's is another instance of one template.
func mkLsig(r int) types.LogicSig { return types.LogicSig{Logic: []byte{6, 0x81, byte(r)}} }

// mkMsig is the 1-of-2 multisig of keys {'K',1} and {'K',2}, signed by the first key if k1 is set.
func mkMsig(k1 bool) types.MultisigSig {
	return msigOf(1, []types.Address{{'K', 1}, {'K', 2}}, k1)
}

func addrOf(c byte) string { return types.Address{c}.String() }

// strs returns q's rows, a single VARCHAR column each.
func strs(t *testing.T, db *sql.DB, q string) []string {
	rows, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

// checkFees checks that rounds 0-10 hold mk's blocks, with payouts on for rounds 5-9, and what
// track asks for: proposers from the cert before payouts, from prp after, NULL at genesis.
func checkFees(t *testing.T, db *sql.DB) {
	for r := range 11 {
		want := uint64(r*1000 + 1)
		if r >= 5 && r <= 9 {
			want = uint64(r * 7)
		}
		b, err := queryBlock(context.Background(), db, cols+"WHERE rnd = ?", uint64(r))
		if err != nil || b == nil || b.Fees != want || b.Ts != uint64(1000+r) || b.Tc != uint64(2*r) {
			t.Errorf("round %d = %+v %v, want fees %d", r, b, err, want)
		}
	}
	var props, evictions []string
	for r := range 11 {
		prop, pp := "-", "-"
		if track.Proposers {
			switch prop, pp = addrOf('O'), "0"; {
			case r == 0:
				prop = "-"
			case r >= 5 && r <= 9:
				prop, pp = addrOf('P'), fmt.Sprint(r*3)
			}
		}
		props = append(props, fmt.Sprintf("%d %s %s", r, prop, pp))
	}
	if track.Proposers {
		evictions = []string{"2 " + addrOf('E'), "7 " + addrOf('E')}
	}
	got := strs(t, db, "SELECT rnd || ' ' || coalesce(prop, '-') || ' ' || coalesce(pp::VARCHAR, '-') FROM blocks ORDER BY rnd")
	if !slices.Equal(got, props) {
		t.Errorf("proposers = %q, want %q", got, props)
	}
	if got := strs(t, db, "SELECT rnd || ' ' || acct FROM evictions ORDER BY rnd"); len(got)+len(evictions) > 0 && !slices.Equal(got, evictions) {
		t.Errorf("evictions = %q, want %q", got, evictions)
	}
	// Only round 0 has tc 0, so only its payset is counted.
	if got := strs(t, db, "SELECT rnd || ' ' || txns FROM blocks WHERE txns > 0"); !slices.Equal(got, []string{"0 1"}) {
		t.Errorf("txns = %q, want round 0's 1", got)
	}
	if track.Msigs { // mk's msig txn is payset[2] from round 5
		m := msigAddr(t, 1, types.Address{'K', 1}, types.Address{'K', 2})
		want := []string{types.Address{'K', 1}.String() + " " + m + " true 7 2", types.Address{'K', 2}.String() + " " + m + " false 5 2"}
		if got := strs(t, db, "SELECT concat_ws(' ', signer, msig, signed, rnd, pos) FROM msig_signers ORDER BY signer"); !slices.Equal(got, want) {
			t.Errorf("msig_signers = %q, want %q", got, want)
		}
	}
	if !track.Lsigs {
		return
	}
	var lsigs []string
	for r := 5; r <= 10; r++ {
		lsigs = append(lsigs, fmt.Sprintf("%s 1 %d %d", crypto.LogicSigAddress(mkLsig(r)), r, r))
	}
	slices.Sort(lsigs)
	if got := strs(t, db, "SELECT prog || ' ' || txns || ' ' || first_rnd || ' ' || last_rnd FROM lsigs WHERE signer = '' AND sender = '' ORDER BY prog"); !slices.Equal(got, lsigs) {
		t.Errorf("lsigs = %q, want %q", got, lsigs)
	}
	if got := strs(t, db, "SELECT count(DISTINCT tmpl) || ' ' || (SELECT count(*) FROM lsig_templates) FROM lsigs"); !slices.Equal(got, []string{"1 1"}) {
		t.Errorf("templates, samples = %q, want 1 1", got)
	}
}

func TestSyncToTip(t *testing.T) {
	defer func(d time.Duration) { retryWait = d }(retryWait)
	retryWait = 0 // the cert guards fail after call's retries
	defer func(c config) { track = c }(track)
	for _, track = range []config{{Proposers: true}, {}, {Proposers: true, Lsigs: true}, {Msigs: true}, {Lsigs: true, Msigs: true}} {
		t.Run(fmt.Sprint(track), func(t *testing.T) {
			ctx := context.Background()
			var blocks []blockCert
			for r := range 10 {
				blocks = append(blocks, mk(r, r >= 5))
			}
			var headerOnly atomic.Int32
			good := fakeAlgod(t, "test-v1", &blocks, &headerOnly)
			db, err := openDB("", "test-v1")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			payouts.Store(false)
			// Batches of 5, fetched one at a time: 0-4 full, 5 full (flips payouts), 6-9 header-only
			// unless lsigs need every payset.
			if hi, err := syncToTip(ctx, db, algodSource(algods{good}, "test-v1"), 1, 5); err != nil || hi != 9 {
				t.Fatal(hi, err)
			}
			want := int32(4)
			if track.fullBlocks() {
				want = 0
			}
			if got := headerOnly.Load(); got != want {
				t.Errorf("header-only fetches = %d, want %d", got, want)
			}
			// A header-only block without prp is refetched in full; an algod on another network, or one
			// whose blocks lack the cert (as from a stripping proxy), is skipped.
			blocks = append(blocks, mk(10, false))
			other := make([]blockCert, len(blocks)) // zero fees: would show if used
			wrong := fakeAlgod(t, "other-v1", &other, &headerOnly)
			stripped := slices.Clone(blocks)
			for i := range stripped {
				stripped[i].Cert = blockCert{}.Cert
			}
			noCert := fakeAlgod(t, "test-v1", &stripped, &headerOnly)
			if hi, err := syncToTip(ctx, db, algodSource(algods{wrong, noCert, good}, "test-v1"), 1, 5); err != nil || hi != 10 {
				t.Fatal(hi, err)
			}
			checkFees(t, db)
			if got := strs(t, db, "SELECT voter FROM voters"); len(got) > 0 {
				t.Errorf("voters = %q, want none", got)
			}

			// A cert that decodes to no proposer fails the sync rather than storing NULL.
			blocks = append(blocks, mk(11, false))
			blocks[11].Cert.Prop.OProp = types.Address{}
			_, err = syncToTip(ctx, db, algodSource(algods{good}, "test-v1"), 1, 5)
			if (err != nil) != track.Proposers {
				t.Errorf("sync with no oprop: %v", err)
			}
		})
	}
}

// TestVoterWindow checks that only the last VoterRounds rounds get voters, header-only outside it.
func TestVoterWindow(t *testing.T) {
	defer func(d time.Duration) { retryWait = d }(retryWait)
	retryWait = 0 // the cert guards fail after call's retries
	defer func(c config) { track = c }(track)
	track = config{VoterRounds: 3}
	ctx := context.Background()
	var blocks []blockCert
	for r := range 10 {
		blocks = append(blocks, mk(r, r >= 5))
	}
	var headerOnly atomic.Int32
	src := algodSource(algods{fakeAlgod(t, "test-v1", &blocks, &headerOnly)}, "test-v1")
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	payouts.Store(false)
	voters := func(want ...int) {
		t.Helper()
		vw := []string{addrOf('V'), addrOf('W')}
		slices.Sort(vw)
		var w []string
		for _, r := range want {
			w = append(w, fmt.Sprintf("%d %s", r, vw[0]), fmt.Sprintf("%d %s", r, vw[1]))
		}
		if got := strs(t, db, "SELECT rnd || ' ' || voter FROM voters ORDER BY rnd, voter"); !slices.Equal(got, w) {
			t.Errorf("voters = %q, want %q", got, w)
		}
	}
	// 0-5 full, 6 header-only, 7-9 full for their certs.
	if _, err := syncToTip(ctx, db, src, 1, 5); err != nil {
		t.Fatal(err)
	}
	if got := headerOnly.Load(); got != 1 {
		t.Errorf("header-only fetches = %d, want 1", got)
	}
	voters(7, 8, 9)
	blocks = append(blocks, mk(10, true), mk(11, true))
	if _, err := syncToTip(ctx, db, src, 1, 5); err != nil {
		t.Fatal(err)
	}
	voters(9, 10, 11)

	// A cert in the window without votes fails the sync.
	blocks = append(blocks, mk(12, true))
	blocks[12].Cert.Vote = nil
	if _, err := syncToTip(ctx, db, src, 1, 5); err == nil {
		t.Error("sync with no votes should fail")
	}
}

func TestLedgerSource(t *testing.T) {
	defer func(c config) { track = c }(track)
	for _, track = range []config{{Proposers: true}, {Proposers: true, Lsigs: true}, {Proposers: true, Msigs: true}} {
		t.Run(fmt.Sprint(track), testLedgerSource)
	}
}

func testLedgerSource(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.block.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	// algod's schema and encoding (go-algorand ledger/store/blockdb BlockPut).
	if _, err := w.Exec(`CREATE TABLE blocks (rnd integer primary key, proto text, hdrdata blob, blkdata blob, certdata blob)`); err != nil {
		t.Fatal(err)
	}
	for r := range 11 {
		b := mk(r, r >= 5 && r <= 9)
		b.GenesisID = "test-v1"
		blk := msgpack.Encode(&b.Block)
		if r >= 6 && r <= 9 && !track.fullBlocks() {
			blk = nil // must be read header-only: a full fetch fails to decode
		}
		var cert []byte // a bare agreement.Certificate, where the proposer comes from before payouts
		if r > 0 && (r < 5 || r == 10) {
			cert = msgpack.Encode(&b.Cert)
		}
		if r >= 6 && r <= 9 {
			cert = []byte{0xc1} // undecodable: payouts are on, so reading it fails the sync
		}
		if _, err := w.Exec("INSERT INTO blocks VALUES (?, '', ?, ?, ?)", r, msgpack.Encode(&b.BlockHeader), blk, cert); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	src, ldb, err := ledgerSource(path, "test-v1", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ldb.Close()
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	payouts.Store(false)
	// Same path as TestSyncToTip: 0-5 full (5 flips payouts), 6-9 header-only (full with lsigs, never
	// with their certs), 10 refetched in full.
	if _, err := syncToTip(ctx, db, src, 1, 5); err != nil {
		t.Fatal(err)
	}
	checkFees(t, db)

	wrong, wdb, err := ledgerSource(path, "other-v1", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer wdb.Close()
	odb, err := openDB("", "other-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer odb.Close()
	if _, err := syncToTip(ctx, odb, wrong, 1, 5); err == nil {
		t.Error("ledger on another network should fail")
	}
}

func TestOpenDBGenesis(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.duckdb")
	db, err := openDB(path, "a-v1")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	if db, err = openDB(path, "a-v1"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if _, err := openDB(path, "b-v1"); err == nil {
		t.Error("openDB with another genesis ID should fail")
	}
}

func row(rnd, ts, tc, fees uint64) block { return block{Rnd: rnd, Ts: ts, Tc: tc, Fees: fees} }

func TestQueries(t *testing.T) {
	ctx := context.Background()
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if n, lo, hi, err := rounds(ctx, db); err != nil || n != 0 || lo != 0 || hi != -1 {
		t.Fatalf("empty rounds = %d %d %d %v", n, lo, hi, err)
	}
	rows := []block{
		row(0, 100, 1000, 0), // genesis counter starts at 1000
		row(1, 110, 1000, 0), // empty block, repeated ts
		row(2, 110, 1003, 3000),
		row(3, 110, 1005, 2500),
		row(4, 120, 1005, 0),
		row(5, 125, 1010, 7000),
	}
	if err := insertBatch(ctx, db, rows); err != nil {
		t.Fatal(err)
	}
	if n, lo, hi, err := rounds(ctx, db); err != nil || n != 6 || lo != 0 || hi != 5 {
		t.Fatalf("rounds = %d %d %d %v", n, lo, hi, err)
	}

	rnd := func(b *block, err error) int {
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			return -1
		}
		return int(b.Rnd)
	}
	for _, c := range []struct {
		ts   uint64
		dir  string
		want int
	}{
		{110, "after", 1}, {110, "before", 3}, {110, "", 1}, // exact match in a run
		{114, "", 3}, {116, "", 4}, {115, "", 4}, // nearer side; tie prefers after
		{50, "before", -1}, {50, "", 0}, {200, "after", -1}, {200, "", 5}, // out of range
	} {
		if got := rnd(blockAt(ctx, db, c.ts, c.dir)); got != c.want {
			t.Errorf("blockAt(%d, %q) = %d, want %d", c.ts, c.dir, got, c.want)
		}
	}
	for _, c := range []struct {
		tc   uint64
		want int
	}{
		{999, -1}, {1000, 2}, {1002, 2}, {1003, 3}, {1005, 5}, {1009, 5}, {1010, -1},
	} {
		if got := rnd(blockByTc(ctx, db, c.tc)); got != c.want {
			t.Errorf("blockByTc(%d) = %d, want %d", c.tc, got, c.want)
		}
	}
	for _, c := range []struct {
		minRnd, maxRnd, start, end uint64
		want                       feeSum
	}{
		{0, math.MaxUint32, 0, math.MaxUint32, feeSum{12500, 6, 0, 5, 100, 125}},
		{2, 3, 0, math.MaxUint32, feeSum{5500, 2, 2, 3, 110, 110}},
		{0, math.MaxUint32, 110, 120, feeSum{5500, 3, 1, 3, 110, 110}},
		{0, math.MaxUint32, 120, math.MaxUint32, feeSum{7000, 2, 4, 5, 120, 125}},
		{10, 20, 0, math.MaxUint32, feeSum{}},
	} {
		got, err := sumFees(ctx, db, c.minRnd, c.maxRnd, c.start, c.end)
		if err != nil || got != c.want {
			t.Errorf("sumFees(%d, %d, %d, %d) = %+v %v, want %+v", c.minRnd, c.maxRnd, c.start, c.end, got, err, c.want)
		}
	}
}

func TestSeries(t *testing.T) {
	ctx := context.Background()
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Genesis at ts 0 as on private nets, then blocks from Monday 2025-10-06 (m); hour m+2h is empty.
	const m = 1759708800
	if err := insertBatch(ctx, db, []block{row(0, 0, 1000, 0), row(1, m+3599, 1004, 100), row(2, m+3600, 1010, 200),
		row(3, m+3*3600, 1011, 300), row(4, m+4*3600, 1013, 400)}); err != nil {
		t.Fatal(err)
	}
	h := func(ts int64, fees, txns, blocks uint64) bucket {
		return bucket{ts, fees, txns, float64(txns) / 3600, blocks, nil}
	}
	blk := func(rnd uint64, ts int64, fees, txns uint64, dt float64) bucket {
		return bucket{ts, fees, txns, float64(txns) / dt, 1, &rnd}
	}
	for _, c := range []struct {
		interval   string
		start, end uint64
		want       []bucket
	}{
		// Genesis txns from its own tc; lag bridges the empty hour.
		{"hour", 0, math.MaxUint32, []bucket{h(0, 0, 0, 1), h(m, 100, 4, 1), h(m+3600, 200, 6, 1), h(m+3*3600, 300, 1, 1), h(m+4*3600, 400, 2, 1)}},
		// First txns from round 0, not round 1 at ts == start; round 4 at ts == end is excluded.
		{"hour", m + 3599, m + 4*3600, []bucket{h(m, 100, 4, 1), h(m+3600, 200, 6, 1), h(m+3*3600, 300, 1, 1)}},
		{"day", 0, math.MaxUint32, []bucket{{0, 0, 0, 0, 1, nil}, {m, 1000, 13, 13.0 / 86400, 4, nil}}},
		{"week", 0, math.MaxUint32, []bucket{{-3 * 86400, 0, 0, 0, 1, nil}, {m, 1000, 13, 13.0 / (7 * 86400), 4, nil}}},    // Mondays 1969-12-29, 2025-10-06
		{"month", 0, math.MaxUint32, []bucket{{0, 0, 0, 0, 1, nil}, {m - 5*86400, 1000, 13, 13.0 / (31 * 86400), 4, nil}}}, // 2025-10-01, 31 days
		{"hour", m + 5*3600, math.MaxUint32, []bucket{}},
		// Pre-summed per minute, not per hour: round 1 lands in minute m+59m.
		{"minute", m + 3599, m + 3*3600 + 1, []bucket{{m + 3540, 100, 4, 4.0 / 60, 1, nil}, {m + 3600, 200, 6, 6.0 / 60, 1, nil}, {m + 3*3600, 300, 1, 1.0 / 60, 1, nil}}},
		// Genesis has no txns or duration.
		{"block", 0, m + 3600, []bucket{{0, 0, 0, 0, 1, new(uint64)}, blk(1, m+3599, 100, 4, m+3599)}},
		// Round 2's txns and duration from round 1, before start; round 4 at ts == end is excluded.
		{"block", m + 3600, m + 4*3600, []bucket{blk(2, m+3600, 200, 6, 1), blk(3, m+3*3600, 300, 1, 7200)}},
	} {
		// got must be non-nil to encode as [], not null.
		if got, err := series(ctx, db, c.interval, c.start, c.end); err != nil || got == nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("series(%s, %d, %d) = %+v %v, want %+v", c.interval, c.start, c.end, got, err, c.want)
		}
	}
}

func TestRequestLog(t *testing.T) {
	var buf bytes.Buffer
	// Restoring the default slog handler leaves the log package pointed at ours, so restore it too.
	defer log.SetOutput(log.Writer())
	defer log.SetFlags(log.Flags())
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	routes(db).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v0/series?interval=second", nil))
	var got struct {
		Msg, Path, Query string
		Status           int
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil || got.Msg != "request" || got.Path != "/v0/series" || got.Query != "interval=second" || got.Status != 400 {
		t.Errorf("request log = %s %v, want a 400 /v0/series?interval=second request", buf.Bytes(), err)
	}
}

func TestParseTime(t *testing.T) {
	want := uint64(time.Date(2025, 10, 6, 0, 0, 0, 0, time.UTC).Unix())
	for _, s := range []string{"1759708800", "2025-10-06", "2025-10-06T00:00:00Z", "2025-10-06T02:00:00+02:00"} {
		if got, err := parseTime(s); err != nil || got != want {
			t.Errorf("parseTime(%q) = %d %v, want %d", s, got, err, want)
		}
	}
	if _, err := parseTime("yesterday"); err == nil {
		t.Error("parseTime(yesterday) should fail")
	}
}

func TestSyncUpgrades(t *testing.T) {
	ctx := context.Background()
	// On v1, A proposes v2 at 5000 and gets 100 approvals by V=15000: rejected. B proposes v2 at
	// 15001 and gets 9500 by V=25001: approved, switching at S=45001.
	var calls atomic.Int32
	var fail uint64 // a round whose fetch fails, if non-zero
	src := source{get: func(_ context.Context, rnd uint64, _, _ bool) (b blockCert, _ error) {
		calls.Add(1)
		if fail != 0 && rnd == fail {
			return b, fmt.Errorf("round %d unavailable", rnd)
		}
		r := types.Round(rnd)
		b.Round, b.TimeStamp, b.CurrentProtocol = r, int64(1000+r), "v1"
		vote := func(p, approvals, switchOn types.Round) {
			b.NextProtocol, b.NextProtocolVoteBefore, b.NextProtocolSwitchOn = "v2", p+voteRounds, switchOn
			b.NextProtocolApprovals = min(r-p+1, approvals)
			if r == p {
				b.UpgradePropose = "v2"
			}
		}
		switch {
		case r >= 5000 && r < 15000:
			vote(5000, 100, 35000)
		case r >= 15001 && r < 45001:
			vote(15001, 9500, 45001)
		case r >= 45001:
			b.CurrentProtocol = "v2"
		}
		return b, nil
	}}
	a := `{"round":5000,"ts":6000,"from":"v1","proto":"v2","voteBefore":15000,"switchOn":35000,`
	b := `{"round":15001,"ts":16001,"from":"v1","proto":"v2","voteBefore":25001,"switchOn":45001,`
	done := "[" + a + `"approvals":100,"status":"rejected","switchTs":null},` + b + `"approvals":9500,"status":"applied","switchTs":46001}]`
	type step struct {
		hi, fail uint64 // an error is expected iff fail != 0
		calls    int32  // catches a proposal recorded or settled twice
		want     string
	}
	for _, steps := range [][]step{
		{
			{10000, 0, 3, "[" + a + `"approvals":null,"status":"voting","switchTs":null}]`}, // samples 0, 10000; A's P
			{30000, 0, 7, "[" + a + `"approvals":100,"status":"rejected","switchTs":null},` + b + `"approvals":9500,"status":"approved","switchTs":null}]`},
			{50000, 0, 3, done}, // sample 40000 shows B again
			{50000, 0, 0, done},
		},
		{
			// A backfill settles each proposal fully within its first step of 500 samples (+7). The
			// second step's 6 samples fail, so only they are refetched.
			{5050000, 5040000, 513, done},
			{5050000, 0, 6, done},
		},
	} {
		db, err := openDB("", "test-v1")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, s := range steps {
			calls.Store(0)
			fail = s.fail
			err := syncUpgrades(ctx, db, src, 4, s.hi)
			u, qerr := queryUpgrades(ctx, db, "ORDER BY rnd")
			got, _ := json.Marshal(u)
			if (err != nil) != (s.fail != 0) || qerr != nil || calls.Load() != s.calls || string(got) != s.want {
				t.Errorf("hi=%d: %s %v %v, %d calls; want %s, %d calls", s.hi, got, err, qerr, calls.Load(), s.want, s.calls)
			}
		}
	}
}

// TestSeriesBeforeTc checks that blocks from before tc existed count their own txns.
func TestSeriesBeforeTc(t *testing.T) {
	ctx := context.Background()
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pre := func(rnd, ts, txns uint64) block { b := row(rnd, ts, 0, 0); b.txns = txns; return b }
	// tc starts at round 3, counting its own 4 txns.
	if err := insertBatch(ctx, db, []block{pre(0, 0, 0), pre(1, 10, 2), pre(2, 20, 3), row(3, 30, 4, 0), row(4, 40, 5, 0)}); err != nil {
		t.Fatal(err)
	}
	if got, err := series(ctx, db, "day", 0, math.MaxUint32); err != nil || len(got) != 1 || got[0].Txns != 10 {
		t.Errorf("day = %+v %v, want 10 txns", got, err)
	}
	var txns []uint64
	got, err := series(ctx, db, "block", 0, math.MaxUint32)
	for _, b := range got {
		txns = append(txns, b.Txns)
	}
	if err != nil || !slices.Equal(txns, []uint64{0, 2, 3, 4, 1}) {
		t.Errorf("block txns = %v %v, want [0 2 3 4 1]", txns, err)
	}
}

func TestOpenDBConfig(t *testing.T) {
	defer func(c config) { track = c }(track)
	ctx := context.Background()
	dir := t.TempDir()
	open := func(path string, c config) error {
		t.Helper()
		track = c
		db, err := openDB(path, "test-v1")
		if err == nil {
			var got config
			err := db.QueryRow(`SELECT *, (SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = 'lsigs'),
				(SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = 'msig_signers') FROM config`).Scan(&got.Proposers, &got.VoterRounds, &got.Lsigs, &got.Msigs)
			if err != nil || got != c {
				t.Errorf("stored config = %+v %v, want %+v", got, err, c)
			}
			if err := insertBatch(ctx, db, []block{row(0, 0, 0, 0)}); err != nil {
				t.Error(err)
			}
			db.Close()
		}
		return err
	}
	// refused fails if path opens under c, or its bytes change.
	refused := func(path string, c config) {
		t.Helper()
		before, _ := os.ReadFile(path)
		if err := open(path, c); err == nil {
			t.Errorf("%+v: want refused", c)
		}
		if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
			t.Errorf("%+v: refusal changed the file", c)
		}
	}

	// An empty DB may change config; open inserts a block, which fixes it.
	path := filepath.Join(dir, "new.duckdb")
	track = config{Proposers: true, VoterRounds: 5}
	db, err := openDB(path, "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err := open(path, config{VoterRounds: 3}); err != nil {
		t.Fatal(err)
	}
	refused(path, config{Proposers: true, VoterRounds: 3})
	refused(path, config{})
	refused(path, config{VoterRounds: 3, Lsigs: true})

	// Lsigs, stored as the lsigs table's presence, toggles while empty and is then fixed both ways.
	lpath := filepath.Join(dir, "lsigs.duckdb")
	track = config{Lsigs: true}
	if db, err = openDB(lpath, "test-v1"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err := open(lpath, config{}); err != nil {
		t.Fatal(err)
	}
	refused(lpath, config{Lsigs: true})
	lpath = filepath.Join(dir, "lsigs2.duckdb")
	if err := open(lpath, config{Lsigs: true}); err != nil {
		t.Fatal(err)
	}
	refused(lpath, config{})
	refused(lpath, config{Lsigs: true, Msigs: true})

	// So does Msigs, stored as the msig_signers table's presence.
	mpath := filepath.Join(dir, "msigs.duckdb")
	track = config{Msigs: true}
	if db, err = openDB(mpath, "test-v1"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err := open(mpath, config{}); err != nil {
		t.Fatal(err)
	}
	refused(mpath, config{Msigs: true})
	mpath = filepath.Join(dir, "msigs2.duckdb")
	if err := open(mpath, config{Msigs: true}); err != nil {
		t.Fatal(err)
	}
	refused(mpath, config{})
	refused(mpath, config{Lsigs: true})

	// A fees-only DB from before config, holding a block.
	legacy := filepath.Join(dir, "legacy.duckdb")
	if db, err = sql.Open("duckdb", legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE blocks(rnd UINTEGER NOT NULL, ts UINTEGER NOT NULL, tc UBIGINT NOT NULL, fees UBIGINT NOT NULL);
		CREATE TABLE meta(genesis VARCHAR NOT NULL); INSERT INTO meta VALUES ('test-v1'); INSERT INTO blocks VALUES (0, 0, 0, 0)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	refused(legacy, config{Proposers: true})
	refused(legacy, config{VoterRounds: 1})
	if err := open(legacy, config{}); err != nil {
		t.Fatal(err)
	}
	if db, err = sql.Open("duckdb", legacy); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := strs(t, db, "SELECT coalesce(prop, '-') || coalesce(pp::VARCHAR, '-') FROM blocks"); !slices.Equal(got, []string{"--", "--"}) {
		t.Errorf("legacy rows = %q", got)
	}
}

func TestProposerRoutes(t *testing.T) {
	defer func(c config) { track = c }(track)
	track = config{Proposers: true, VoterRounds: 100}
	ctx := context.Background()
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	A, B, C, D := addrOf(1), addrOf(2), addrOf(3), addrOf(4) // in string order
	const d = 1759708800                                     // 2025-10-06
	at := func(rnd, ts uint64, prop string, pp uint64, absent, voters []string) block {
		b := row(rnd, ts, 0, 0)
		b.prop, b.pp, b.absent, b.voters = prop, pp, absent, voters
		return b
	}
	if err := insertBatch(ctx, db, []block{
		at(0, 100, "", 0, nil, nil),
		at(1, d+10, A, 0, nil, []string{A, B}),
		at(2, d+20, B, 5, []string{C}, []string{A}),
		at(3, d+86400, A, 7, nil, []string{B}),
		at(4, d+86430, A, 0, []string{C, A}, nil),
		at(5, d+2*86400+10, C, 9, nil, []string{A, B, C}),
	}); err != nil {
		t.Fatal(err)
	}
	h := routes(db)
	get := func(url string) (int, string) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", url, nil))
		return w.Code, strings.TrimSpace(w.Body.String())
	}
	r := strings.NewReplacer("@A", A, "@B", B, "@C", C, "@D", D)
	ok := http.StatusOK
	for _, c := range []struct {
		url  string
		code int
		want string // @A-@D are replaced with the addresses
	}{
		{"/v0/proposers", ok, `[{"proposer":"@A","blocks":3,"payouts":7},{"proposer":"@B","blocks":1,"payouts":5},{"proposer":"@C","blocks":1,"payouts":9}]`},
		{"/v0/proposers?minRound=2&maxRound=4", ok, `[{"proposer":"@A","blocks":2,"payouts":7},{"proposer":"@B","blocks":1,"payouts":5}]`},
		{"/v0/proposers?start=2025-10-07", ok, `[{"proposer":"@A","blocks":2,"payouts":7},{"proposer":"@C","blocks":1,"payouts":9}]`},
		{fmt.Sprintf("/v0/proposers?start=%d&end=%d", d+15, d+86400), ok, `[{"proposer":"@B","blocks":1,"payouts":5}]`},
		{"/v0/proposers?minRound=4&start=2025-10-07&end=2025-10-08", ok, `[{"proposer":"@A","blocks":1,"payouts":0}]`},
		{fmt.Sprintf("/v0/proposers?start=%d&end=%d", d+21, d+86400), ok, `[]`}, // between rounds 2 and 3
		{"/v0/proposers?end=50", ok, `[]`},
		{"/v0/proposers?start=yesterday", 400, ""},
		{"/v0/proposers?minRound=-1", 400, ""},
		{"/v0/proposer/@A", ok, fmt.Sprintf(`[{"rnd":1,"ts":%d},{"rnd":3,"ts":%d,"pp":7},{"rnd":4,"ts":%d}]`, d+10, d+86400, d+86430)},
		{"/v0/proposer/@A?maxRound=3&start=2025-10-07", ok, fmt.Sprintf(`[{"rnd":3,"ts":%d,"pp":7}]`, d+86400)},
		{"/v0/proposer/@D", ok, `[]`},
		{"/v0/proposer/nope", 400, ""},
		{"/v0/proposer/@A/stats", ok, fmt.Sprintf(`{"blocks":3,"payouts":7,"suspensions":1,"firstRound":1,"lastRound":4,"avgBlocksPerDay":1.5,"range":{"firstRound":1,"lastRound":5,"firstTs":%d,"lastTs":%d}}`, d+10, d+2*86400+10)},
		{"/v0/proposer/@A/stats?start=2025-10-07&maxRound=4", ok, fmt.Sprintf(`{"blocks":2,"payouts":7,"suspensions":1,"firstRound":3,"lastRound":4,"avgBlocksPerDay":5760.0,"range":{"firstRound":3,"lastRound":4,"firstTs":%d,"lastTs":%d}}`, d+86400, d+86430)},
		{"/v0/proposer/@D/stats?minRound=9", ok, `{"blocks":0,"payouts":0,"suspensions":0,"firstRound":0,"lastRound":0,"avgBlocksPerDay":0.0,"range":{"firstRound":0,"lastRound":0,"firstTs":0,"lastTs":0}}`},
		{"/v0/payouts/highest", ok, fmt.Sprintf(`[{"proposer":"@C","rnd":5,"ts":%d,"pp":9},{"proposer":"@A","rnd":3,"ts":%d,"pp":7},{"proposer":"@B","rnd":2,"ts":%d,"pp":5},{"proposer":"@A","rnd":1,"ts":%d,"pp":0},{"proposer":"@A","rnd":4,"ts":%d,"pp":0}]`, d+2*86400+10, d+86400, d+20, d+10, d+86430)},
		{"/v0/evictions", ok, `[{"account":"@A","evictions":1,"rounds":[4]},{"account":"@C","evictions":2,"rounds":[4,2]}]`},
		{"/v0/evictions?maxRound=3", ok, `[{"account":"@C","evictions":1,"rounds":[2]}]`},
		{"/v0/evictions?minRound=5", ok, `[]`},
		{"/v0/evictions/@C", ok, `[2,4]`},
		{"/v0/evictions/@C?end=2025-10-07", ok, `[2]`},
		{"/v0/round/3", ok, fmt.Sprintf(`{"proposer":"@A","rnd":3,"ts":%d,"pp":7}`, d+86400)},
		{"/v0/round/1", ok, fmt.Sprintf(`{"proposer":"@A","rnd":1,"ts":%d}`, d+10)},
		{"/v0/round/0", ok, `{"proposer":null,"rnd":0,"ts":100}`},
		{"/v0/round/99", 404, ""},
		{"/v0/round/x", 400, ""},
		{"/v0/voters", ok, `[{"voter":"@A","blocks":3},{"voter":"@B","blocks":3},{"voter":"@C","blocks":1}]`},
		{"/v0/voters?start=2025-10-07", ok, `[{"voter":"@B","blocks":2},{"voter":"@A","blocks":1},{"voter":"@C","blocks":1}]`},
		{"/v0/voter/@B", ok, `[1,3,5]`},
		{"/v0/voter/@B?end=2025-10-08", ok, `[1,3]`},
		{"/v0/voter/@D", ok, `[]`},
		{"/v0/exists/@C", ok, `{"exists":true}`},
		{"/v0/exists/@D", ok, `{"exists":false}`},
		{"/v0/exists/nope", 400, ""},
		{"/v0/status", ok, `{"ok":1,"maxRound":5,"records":6,"proposers":true,"voterRounds":100,"lsigs":false,"msigs":false}`},
	} {
		code, body := get(r.Replace(c.url))
		if code != c.code || (c.code == ok && body != r.Replace(c.want)) {
			t.Errorf("%s = %d %s, want %d %s", c.url, code, body, c.code, c.want)
		}
	}

	track = config{}
	h = routes(db)
	for _, url := range []string{"/v0/proposers", "/v0/round/1", "/v0/voters", "/v0/exists/" + A} {
		if code, _ := get(url); code != 404 {
			t.Errorf("%s with tracking off = %d, want 404", url, code)
		}
	}
}

func TestMergeLsigs(t *testing.T) {
	defer func(c config) { track = c }(track)
	track = config{Lsigs: true}
	ctx := context.Background()
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	at := func(rnd uint64, uses ...lsigUse) block { b := row(rnd, rnd, 0, 0); b.lsigs = uses; return b }
	U := lsigUse{prog: "P", tmpl: "t1", logic: "AQ==", n: 2}
	V := lsigUse{prog: "Q", signer: "Y", tmpl: "t1", logic: "Ag==", n: 1}
	U1 := U
	U1.n, U1.logic = 1, "Aw=="
	W := lsigUse{prog: "W", tmpl: "t2", n: 1} // used twice in one batch only
	for _, batch := range [][]block{{at(5, U, V, W), at(6), at(7, U1, W)}, {at(9, U1)}, {at(10)}} {
		if err := insertBatch(ctx, db, batch); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"P - - 4 5 9", "Q Y - 1 5 5", "W - - 2 5 7"}
	if got := strs(t, db, `SELECT concat_ws(' ', prog, coalesce(nullif(signer, ''), '-'), coalesce(nullif(sender, ''), '-'), txns, first_rnd, last_rnd)
		FROM lsigs ORDER BY prog`); !slices.Equal(got, want) {
		t.Errorf("lsigs = %q, want %q", got, want)
	}
	if got := strs(t, db, "SELECT tmpl || ' ' || logic FROM lsig_templates"); !slices.Equal(got, []string{"t1 AQ=="}) {
		t.Errorf("lsig_templates = %q, want the first logic", got)
	}
}

// TestStartRound checks that an empty DB starts at startRound, and a later sync resumes past it.
func TestStartRound(t *testing.T) {
	defer func(c config, r uint64) { track, startRound = c, r }(track, startRound)
	track, startRound = config{}, 7
	ctx := context.Background()
	var blocks []blockCert
	for r := range 10 {
		blocks = append(blocks, mk(r, false))
	}
	var headerOnly atomic.Int32
	src := algodSource(algods{fakeAlgod(t, "test-v1", &blocks, &headerOnly)}, "test-v1")
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	payouts.Store(false)
	for _, hi := range []int64{9, 10} {
		if hi == 10 {
			blocks = append(blocks, mk(10, false))
		}
		if _, err := syncToTip(ctx, db, src, 1, 5); err != nil {
			t.Fatal(err)
		}
		if n, lo, got, err := rounds(ctx, db); err != nil || n != hi-6 || lo != 7 || got != hi {
			t.Errorf("rounds = %d %d %d %v, want %d 7 %d", n, lo, got, err, hi-6, hi)
		}
	}
	// Past the tip, it stores nothing (fetching would panic) and returns the tip, not START_ROUND-1.
	startRound = 20
	edb, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer edb.Close()
	tip10 := source{tip: func(context.Context, uint64) (uint64, error) { return 10, nil }}
	if hi, err := syncToTip(ctx, edb, tip10, 1, 5); err != nil || hi != 10 {
		t.Errorf("syncToTip past the tip = %d %v, want 10", hi, err)
	}
}

func TestLsigRoutes(t *testing.T) {
	defer func(c config) { track = c }(track)
	track = config{Lsigs: true}
	ctx := context.Background()
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	A, B, C, D, E, F, G, H := addrOf(1), addrOf(2), addrOf(3), addrOf(4), addrOf(5), addrOf(6), addrOf(7), addrOf(8)
	const t1, t2, t3 = "00000000000000a1", "00000000000000a2", "00000000000000a3"
	at := func(rnd uint64, uses ...lsigUse) block { b := row(rnd, rnd, 0, 0); b.lsigs = uses; return b }
	// A and B are escrows of template t1; D delegated C (t2), also sending for E rekeyed to D; G and H
	// delegated F (t3).
	if err := insertBatch(ctx, db, []block{
		at(1, lsigUse{A, "", "", t1, "AQ==", 1}),
		at(2, lsigUse{B, "", "", t1, "", 2}, lsigUse{C, D, "", t2, "Ag==", 1}),
		at(3, lsigUse{C, D, E, t2, "", 1}),
		at(4, lsigUse{F, G, "", t3, "Aw==", 1}, lsigUse{F, H, "", t3, "", 1}),
	}); err != nil {
		t.Fatal(err)
	}
	use := func(prog, tmpl, signer, sender string, txns, first, last int) string {
		q := func(s string) string {
			if s == "" {
				return "null"
			}
			return `"` + s + `"`
		}
		return fmt.Sprintf(`{"lsig":"%s","template":"%s","signer":%s,"sender":%s,"txns":%d,"firstRound":%d,"lastRound":%d}`,
			prog, tmpl, q(signer), q(sender), txns, first, last)
	}
	uA, uB, uC, uCE := use(A, t1, "", "", 1, 1, 1), use(B, t1, "", "", 2, 2, 2), use(C, t2, D, "", 1, 2, 2), use(C, t2, D, E, 1, 3, 3)
	h := routes(db)
	get := func(url string) (int, string) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", url, nil))
		return w.Code, strings.TrimSpace(w.Body.String())
	}
	ok := http.StatusOK
	for _, c := range []struct {
		url  string
		code int
		want string
	}{
		{"/v0/lsig/" + A, ok, `{"template":"` + t1 + `","name":null,"programs":2,"uses":[` + uA + `]}`},
		{"/v0/lsig/" + C, ok, `{"template":"` + t2 + `","name":null,"programs":1,"uses":[` + uCE + "," + uC + `]}`},
		{"/v0/lsig/" + C + "?limit=1", ok, `{"template":"` + t2 + `","name":null,"programs":1,"uses":[` + uCE + `]}`},
		{"/v0/lsig/" + D, 404, ""}, // a delegator, not a program
		{"/v0/lsig/nope", 400, ""},
		{"/v0/lsig/" + A + "?limit=0", 400, ""},
		{"/v0/delegations/" + D, ok, "[" + uCE + "," + uC + "]"},
		{"/v0/delegations/" + E, ok, "[" + uCE + "]"},
		{"/v0/delegations/" + A, ok, "[]"},
		// t2 is one program with one delegator: not listed.
		{"/v0/templates", ok, `[{"template":"` + t1 + `","name":null,"programs":2,"delegators":0,"txns":3,"firstRound":1,"lastRound":2},` +
			`{"template":"` + t3 + `","name":null,"programs":1,"delegators":2,"txns":2,"firstRound":4,"lastRound":4}]`},
		{"/v0/templates?limit=x", 400, ""},
		{"/v0/templates?limit=100001", 400, ""},
		{"/v0/template/" + t1, ok, `{"template":"` + t1 + `","name":null,"logic":"AQ==","programs":2,"uses":[` + uB + "," + uA + `]}`},
		{"/v0/template/" + t2 + "?limit=1", ok, `{"template":"` + t2 + `","name":null,"logic":"Ag==","programs":1,"uses":[` + uCE + `]}`},
		{"/v0/template/0000000000000000", 404, ""},
		{"/v0/template/00000000000000A1", 400, ""},
		{"/v0/template/a1", 400, ""},
	} {
		code, body := get(c.url)
		if code != c.code || (c.code == ok && body != c.want) {
			t.Errorf("%s = %d %s, want %d %s", c.url, code, body, c.code, c.want)
		}
	}

	track = config{}
	h = routes(db)
	for _, url := range []string{"/v0/lsig/" + A, "/v0/delegations/" + D, "/v0/templates", "/v0/template/" + t1} {
		if code, _ := get(url); code != 404 {
			t.Errorf("%s with lsigs off = %d, want 404", url, code)
		}
	}
}
