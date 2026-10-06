package main

import (
	"context"
	"math"
	"testing"
	"time"

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

func TestQueries(t *testing.T) {
	ctx := context.Background()
	db, err := openDB("")
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
