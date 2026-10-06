package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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

func TestSyncToTip(t *testing.T) {
	ctx := context.Background()
	var sink, prop types.Address
	sink[0], prop[0] = 'S', 'P'
	// Payset fees are r*1000 + 1; rounds 5-9 have payouts on, with fc = r*7 so the path taken shows.
	mk := func(r int, payouts bool) types.Block {
		var b types.Block
		b.Round, b.TimeStamp, b.TxnCounter, b.FeeSink = types.Round(r), int64(1000+r), uint64(2*r), sink
		b.Payset = []types.SignedTxnInBlock{{SignedTxnWithAD: stxn('A', uint64(r)*1000, stxn('B', 1))}}
		if payouts {
			b.Proposer, b.FeesCollected = prop, types.MicroAlgos(r*7)
		}
		return b
	}
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
	if err := syncToTip(ctx, db, algods{good}, "test-v1", 1, 5); err != nil {
		t.Fatal(err)
	}
	if got := headerOnly.Load(); got != 4 {
		t.Errorf("header-only fetches = %d, want 4", got)
	}
	// A header-only block without prp is refetched in full; an algod on another network is skipped.
	blocks = append(blocks, mk(10, false))
	other := make([]types.Block, len(blocks)) // zero fees: would show if used
	wrong := fakeAlgod(t, "other-v1", &other, &headerOnly)
	if err := syncToTip(ctx, db, algods{wrong, good}, "test-v1", 1, 5); err != nil {
		t.Fatal(err)
	}
	for r := range 11 {
		want := uint64(r*1000 + 1)
		if r >= 5 && r <= 9 {
			want = uint64(r * 7)
		}
		b, err := queryBlock(ctx, db, cols+"WHERE rnd = ?", uint64(r))
		if err != nil || b == nil || b.Fees != want || b.Ts != uint64(1000+r) || b.Tc != uint64(2*r) {
			t.Errorf("round %d = %+v %v, want fees %d", r, b, err, want)
		}
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
