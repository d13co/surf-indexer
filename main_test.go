package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
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
func fakeAlgod(t *testing.T, genesisID string, blocks *[]types.Block, headerOnly *atomic.Int32) *algod.Client {
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
			b.Payset = nil
		}
		w.Write(msgpack.Encode(models.BlockResponse{Block: b}))
	}))
	t.Cleanup(srv.Close)
	c, err := algod.MakeClient(srv.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// mk builds round r with payset fees r*1000 + 1. With payouts on it has a proposer and
// fc = r*7, so the stored fees show which path was taken.
func mk(r int, payouts bool) types.Block {
	var b types.Block
	b.Round, b.TimeStamp, b.TxnCounter = types.Round(r), int64(1000+r), uint64(2*r)
	b.FeeSink[0] = 'S'
	b.Payset = []types.SignedTxnInBlock{{SignedTxnWithAD: stxn('A', uint64(r)*1000, stxn('B', 1))}}
	if payouts {
		b.Proposer[0], b.FeesCollected = 'P', types.MicroAlgos(r*7)
	}
	return b
}

// checkFees checks that rounds 0-10 hold mk's blocks, with payouts on for rounds 5-9.
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
}

func TestSyncToTip(t *testing.T) {
	ctx := context.Background()
	var blocks []types.Block
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
	// Batches of 5, fetched one at a time: 0-4 full, 5 full (flips payouts), 6-9 header-only.
	if hi, err := syncToTip(ctx, db, algodSource(algods{good}, "test-v1"), 1, 5); err != nil || hi != 9 {
		t.Fatal(hi, err)
	}
	if got := headerOnly.Load(); got != 4 {
		t.Errorf("header-only fetches = %d, want 4", got)
	}
	// A header-only block without prp is refetched in full; an algod on another network is skipped.
	blocks = append(blocks, mk(10, false))
	other := make([]types.Block, len(blocks)) // zero fees: would show if used
	wrong := fakeAlgod(t, "other-v1", &other, &headerOnly)
	if hi, err := syncToTip(ctx, db, algodSource(algods{wrong, good}, "test-v1"), 1, 5); err != nil || hi != 10 {
		t.Fatal(hi, err)
	}
	checkFees(t, db)
}

func TestLedgerSource(t *testing.T) {
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
		blk := msgpack.Encode(&b)
		if r >= 6 && r <= 9 {
			blk = nil // must be read header-only: a full fetch fails to decode
		}
		if _, err := w.Exec("INSERT INTO blocks VALUES (?, '', ?, ?, NULL)", r, msgpack.Encode(&b.BlockHeader), blk); err != nil {
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
	// Same path as TestSyncToTip: 0-5 full (5 flips payouts), 6-9 header-only, 10 refetched in full.
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
		{0, 100, 1000, 0}, // genesis counter starts at 1000
		{1, 110, 1000, 0}, // empty block, repeated ts
		{2, 110, 1003, 3000},
		{3, 110, 1005, 2500},
		{4, 120, 1005, 0},
		{5, 125, 1010, 7000},
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
	if err := insertBatch(ctx, db, []block{{0, 0, 1000, 0}, {1, m + 3599, 1004, 100}, {2, m + 3600, 1010, 200},
		{3, m + 3*3600, 1011, 300}, {4, m + 4*3600, 1013, 400}}); err != nil {
		t.Fatal(err)
	}
	h := func(ts int64, fees, txns, blocks uint64) bucket {
		return bucket{ts, fees, txns, float64(txns) / 3600, blocks}
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
		{"day", 0, math.MaxUint32, []bucket{{0, 0, 0, 0, 1}, {m, 1000, 13, 13.0 / 86400, 4}}},
		{"week", 0, math.MaxUint32, []bucket{{-3 * 86400, 0, 0, 0, 1}, {m, 1000, 13, 13.0 / (7 * 86400), 4}}},    // Mondays 1969-12-29, 2025-10-06
		{"month", 0, math.MaxUint32, []bucket{{0, 0, 0, 0, 1}, {m - 5*86400, 1000, 13, 13.0 / (31 * 86400), 4}}}, // 2025-10-01, 31 days
		{"hour", m + 5*3600, math.MaxUint32, []bucket{}},
	} {
		// got must be non-nil to encode as [], not null.
		if got, err := series(ctx, db, intervals[c.interval], c.start, c.end); err != nil || got == nil || !slices.Equal(got, c.want) {
			t.Errorf("series(%s, %d, %d) = %+v %v, want %+v", c.interval, c.start, c.end, got, err, c.want)
		}
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
	src := source{get: func(_ context.Context, rnd uint64, _ bool) (b types.Block, _ error) {
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
