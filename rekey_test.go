package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/crypto"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

// tx builds a txn from sender, set up by each of mods.
func tx(sender types.Address, mods ...func(*types.SignedTxnWithAD)) types.SignedTxnWithAD {
	var t types.SignedTxnWithAD
	t.Txn.Type, t.Txn.Sender = types.PaymentTx, sender
	for _, m := range mods {
		m(&t)
	}
	return t
}

func rekeyTo(a types.Address) func(*types.SignedTxnWithAD) {
	return func(t *types.SignedTxnWithAD) { t.Txn.RekeyTo = a }
}
func closeTo(a types.Address) func(*types.SignedTxnWithAD) {
	return func(t *types.SignedTxnWithAD) { t.Txn.CloseRemainderTo = a }
}
func signedBy(a types.Address) func(*types.SignedTxnWithAD) {
	return func(t *types.SignedTxnWithAD) { t.AuthAddr = a }
}

// appCall calls app (0 creates created), making inner.
func appCall(app, created uint64, inner ...types.SignedTxnWithAD) func(*types.SignedTxnWithAD) {
	return func(t *types.SignedTxnWithAD) {
		t.Txn.Type, t.Txn.ApplicationID, t.ApplyData.ApplicationID = types.ApplicationCallTx, types.AppIndex(app), types.AppIndex(created)
		t.EvalDelta.InnerTxns = inner
	}
}

func payset(txns ...types.SignedTxnWithAD) []types.SignedTxnInBlock {
	out := make([]types.SignedTxnInBlock, len(txns))
	for i, t := range txns {
		out[i].SignedTxnWithAD = t
	}
	return out
}

func TestRekeyUses(t *testing.T) {
	a := func(c byte) types.Address { return types.Address{c} }
	A, B, C, D, E, F, G, H := a('A'), a('B'), a('C'), a('D'), a('E'), a('F'), a('G'), a('H')
	J, K, L, M, N, P, Q, R, S, T, U := a('J'), a('K'), a('L'), a('M'), a('N'), a('P'), a('Q'), a('R'), a('S'), a('T'), a('U')
	app7, app9, app11, app13 := crypto.GetApplicationAddress(7), crypto.GetApplicationAddress(9), crypto.GetApplicationAddress(11), crypto.GetApplicationAddress(13)
	V := a('V')
	var b types.Block
	b.Payset = payset(
		tx(A, rekeyTo(B)),
		tx(A, rekeyTo(A)), // a rekey back
		// An inner rekey, and one two levels down.
		tx(C, appCall(7, 0, tx(E, rekeyTo(F)), tx(app7, appCall(9, 0, tx(G, rekeyTo(H)))))),
		tx(A, rekeyTo(B)), // folds into the first
		tx(D),             // no rekey
		// Close-outs: by a rekeyed sender; sgnr equal to the sender, or unset, isn't rekeyed.
		tx(J, closeTo(K), signedBy(K)),
		tx(L, closeTo(K), signedBy(L)),
		tx(M, closeTo(K)),
		// Inner: an account rekeyed to the app, and the app's own (not rekeyed).
		tx(C, appCall(7, 0, tx(N, closeTo(K)), tx(app7, closeTo(K)))),
		// Under an app created in the same txn.
		tx(C, appCall(0, 11, tx(P, closeTo(K)), tx(app11, closeTo(K)))),
		// Rekeyed then closed in one txn: both.
		tx(Q, rekeyTo(R), closeTo(K)),
		// An asset close-out closes a holding, not the account.
		tx(S, signedBy(K), func(t *types.SignedTxnWithAD) { t.Txn.Type, t.Txn.AssetCloseTo = types.AssetTransferTx, K }),
		// Rekeyed back, then closed: the close clears nothing.
		tx(T, signedBy(U), rekeyTo(T), closeTo(K)),
		// An inner txn of app 9 called from an outer appl's inner: sent by app9, not rekeyed.
		tx(C, appCall(7, 0, tx(app7, appCall(9, 0, tx(app9, closeTo(K)))))),
		// An app created by an inner appl, whose own inners close an account rekeyed to it (V) and its
		// own account (app13).
		tx(C, appCall(7, 0, tx(app7, appCall(0, 13, tx(V, closeTo(K)), tx(app13, closeTo(K)))))),
	)
	var got, want []string
	for _, u := range rekeyUses(b) {
		got = append(got, fmt.Sprintf("%s>%s %d", u.acct, u.auth, u.n))
	}
	for _, w := range []struct {
		acct, auth types.Address
		n          int
	}{{A, B, 2}, {A, A, 1}, {E, F, 1}, {G, H, 1}, {J, J, 1}, {N, N, 1}, {P, P, 1}, {Q, R, 1}, {Q, Q, 1}, {T, T, 1}, {V, V, 1}} {
		want = append(want, fmt.Sprintf("%s>%s %d", w.acct, w.auth, w.n))
	}
	if !slices.Equal(got, want) {
		t.Errorf("rekeyUses =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// rekeyRows returns the rekeys table, one string per row.
func rekeyRows(t *testing.T, db *sql.DB) []string {
	t.Helper()
	return strs(t, db, "SELECT concat_ws(' ', acct, auth, txns, first_rnd, last_rnd) FROM rekeys ORDER BY acct, auth")
}

// The same rounds merged in any order and batching give the same rows.
func TestMergeRekeysOrderFree(t *testing.T) {
	defer func(c config) { track = c }(track)
	track = config{Rekeys: true}
	at := func(rnd uint64, uses ...rekeyUse) block { return block{Rnd: rnd, rekeys: uses} }
	u := func(acct, auth string, n uint64) rekeyUse { return rekeyUse{acct, auth, n} }
	rounds := []block{
		at(1, u("A", "B", 1)), at(2, u("A", "B", 2), u("C", "A", 1)), at(3), at(4, u("A", "A", 1)),
		at(5, u("A", "B", 1), u("D", "A", 1)), at(6, u("C", "A", 1)),
	}
	asc := [][]block{rounds[:2], rounds[2:5], rounds[5:]}
	desc := [][]block{{rounds[5], rounds[4]}, {rounds[3]}, {rounds[2], rounds[1], rounds[0]}}
	mixed := [][]block{{rounds[3], rounds[0]}, {rounds[5]}, {rounds[1], rounds[4], rounds[2]}}
	var first []string
	for i, batches := range [][][]block{asc, desc, mixed} {
		db, err := openDB("", "test-v1")
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range batches {
			tx, err := db.Begin()
			if err == nil {
				if err = mergeRekeys(context.Background(), tx, b); err == nil {
					err = tx.Commit()
				}
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		got := rekeyRows(t, db)
		db.Close()
		if i == 0 {
			first = got
			if want := []string{"A A 1 4 4", "A B 4 1 5", "C A 2 2 6", "D A 1 5 5"}; !slices.Equal(got, want) {
				t.Errorf("rekeys = %q, want %q", got, want)
			}
		} else if !slices.Equal(got, first) {
			t.Errorf("order %d: rekeys = %q, want %q", i, got, first)
		}
	}
}

// openWith opens path with track set to c.
func openWith(t *testing.T, path string, c config) (*sql.DB, error) {
	t.Helper()
	track = c
	return openDB(path, "test-v1")
}

// gaps returns feature_sync and backfill_gaps for rekeys, as "next n" and "lo-hi" strings.
func gaps(t *testing.T, db *sql.DB) []string {
	t.Helper()
	return strs(t, db, `SELECT 'next ' || next FROM feature_sync WHERE feature = 'rekeys'
		UNION ALL (SELECT lo || '-' || hi FROM backfill_gaps WHERE feature = 'rekeys' ORDER BY lo)`)
}

func insertRounds(t *testing.T, db *sql.DB, lo, hi uint64) {
	t.Helper()
	var rows []block
	for r := lo; r <= hi; r++ {
		rows = append(rows, row(r, r, 0, 0))
	}
	if err := insertBatch(context.Background(), db, rows); err != nil {
		t.Fatal(err)
	}
}

func TestRekeySetup(t *testing.T) {
	defer func(c config, r uint64) { track, startRound = c, r }(track, startRound)
	dir := "data" // drop-feature opens data/<dbID>.duckdb
	t.Chdir(t.TempDir())
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Turned on for a DB with blocks: a gap of every stored round but genesis, routes 404 until
	// backfilled.
	path := filepath.Join(dir, "on.duckdb")
	db, err := openWith(t, path, config{})
	if err != nil {
		t.Fatal(err)
	}
	insertRounds(t, db, 0, 9)
	db.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 10", "1-9"}) {
		t.Errorf("gaps = %q", got)
	}
	h := routes(db)
	get := func(url string) (int, string) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", url, nil))
		return w.Code, strings.TrimSpace(w.Body.String())
	}
	if code, _ := get("/v0/rekeys/" + addrOf(1)); code != 404 {
		t.Errorf("rekeys while backfilling = %d, want 404", code)
	}
	if _, body := get("/v0/status"); !strings.Contains(body, `"rekeys":false,"backfill":{"rekeys":{"roundsLeft":9}}`) {
		t.Errorf("status while backfilling = %s", body)
	}
	// Ingest past the gap moves next; reopening adds nothing.
	insertRounds(t, db, 10, 11)
	db.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 12", "1-9"}) {
		t.Errorf("gaps after ingest = %q", got)
	}
	db.Close()

	// An older binary ingested 12-14 without rekeys: reopening adds them.
	raw, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("INSERT INTO blocks (rnd, ts, tc, fees, txns) VALUES (12, 12, 0, 0, 0), (13, 13, 0, 0, 0), (14, 14, 0, 0, 0)"); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 15", "1-9", "12-14"}) {
		t.Errorf("gaps after an older binary = %q", got)
	}
	if roundsLeft()["rekeys"] != 12 {
		t.Errorf("roundsLeft = %v, want 12", roundsLeft())
	}
	db.Close()

	// A table made before its indexes were added gets them on the next start.
	indexes := func(db *sql.DB) []string {
		t.Helper()
		return strs(t, db, "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'rekeys' ORDER BY 1")
	}
	if raw, err = sql.Open("duckdb", path); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("DROP INDEX rekeys_acct; DROP INDEX rekeys_auth; INSERT INTO rekeys SELECT 'a' || i, 'b' || (i % 7), 1, i, i FROM range(5000) t(i)"); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got, n := indexes(db), strs(t, db, "SELECT count(*) FROM rekeys"); !slices.Equal(got, []string{"rekeys_acct", "rekeys_auth"}) || !slices.Equal(n, []string{"5000"}) {
		t.Errorf("indexes after reopening = %q, rows %q", got, n)
	}
	if _, err := db.Exec("DELETE FROM rekeys"); err != nil { // the rest of the test expects none
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 15", "1-9", "12-14"}) {
		t.Errorf("gaps after building indexes = %q", got)
	}
	db.Close()

	// Turned off with the table present: refused, the file unchanged.
	before, _ := os.ReadFile(path)
	if _, err := openWith(t, path, config{}); !errors.As(err, new(configErr)) || !strings.Contains(err.Error(), "drop-feature rekeys on ") {
		t.Errorf("rekeys off = %v, want a configErr naming drop-feature", err)
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Error("the refusal changed the file")
	}
	// drop-feature drops it, after which it opens with rekeys off.
	if code := dropFeature([]string{"rekeys", "on"}); code != 0 {
		t.Fatalf("drop-feature = %d", code)
	}
	if db, err = openWith(t, path, config{}); err != nil {
		t.Fatal(err)
	}
	if got := strs(t, db, `SELECT 'x' FROM duckdb_tables() WHERE table_name = 'rekeys'
		UNION ALL SELECT index_name FROM duckdb_indexes() WHERE table_name = 'rekeys'
		UNION ALL SELECT feature FROM feature_sync UNION ALL SELECT feature FROM backfill_gaps`); len(got) > 0 {
		t.Errorf("after drop-feature: %q", got)
	}
	db.Close()
	if code := dropFeature([]string{"nope", "on"}); code != 2 {
		t.Errorf("drop-feature of an unknown feature = %d, want 2", code)
	}
	if code := dropFeature([]string{"rekeys", "missing"}); code != 1 {
		t.Errorf("drop-feature of a missing file = %d, want 1", code)
	}
	for _, id := range []string{"../on", "data/on", "..", ""} {
		if code := dropFeature([]string{"rekeys", id}); code != 2 {
			t.Errorf("drop-feature of dbID %q = %d, want 2", id, code)
		}
	}
	// Another DuckDB file in data/ is refused and left as it is.
	other := filepath.Join(dir, "other.duckdb")
	if raw, err = sql.Open("duckdb", other); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("CREATE TABLE rekeys(x INT)"); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	if code := dropFeature([]string{"rekeys", "other"}); code != 1 {
		t.Errorf("drop-feature of a DB that isn't surf-indexer's = %d, want 1", code)
	}
	if raw, err = sql.Open("duckdb", other); err != nil {
		t.Fatal(err)
	}
	if got := strs(t, raw, "SELECT table_name FROM duckdb_tables() ORDER BY 1"); !slices.Equal(got, []string{"rekeys"}) {
		t.Errorf("tables after a refused drop-feature = %q", got)
	}
	raw.Close()

	// The gap starts no earlier than the network's first rekey round.
	defer func(m map[string]uint64) { rekeyFrom = m }(rekeyFrom)
	rekeyFrom = map[string]uint64{"test-v1": 6}
	path = filepath.Join(dir, "from.duckdb")
	if db, err = openWith(t, path, config{}); err != nil {
		t.Fatal(err)
	}
	insertRounds(t, db, 0, 9)
	db.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 10", "6-9"}) {
		t.Errorf("gaps from round 6 = %q", got)
	}
	db.Close()
	// So does one an older binary leaves behind.
	path = filepath.Join(dir, "older.duckdb")
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	insertRounds(t, db, 0, 3)
	db.Close()
	if raw, err = sql.Open("duckdb", path); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("INSERT INTO blocks (rnd, ts, tc, fees, txns) SELECT r, r, 0, 0, 0 FROM range(4, 10) t(r)"); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 10", "6-9"}) {
		t.Errorf("older binary's gap from round 6 = %q", got)
	}
	db.Close()
	// All rounds before it: no gap, ready at once.
	rekeyFrom = map[string]uint64{"test-v1": 100}
	path = filepath.Join(dir, "early.duckdb")
	if db, err = openWith(t, path, config{}); err != nil {
		t.Fatal(err)
	}
	insertRounds(t, db, 0, 9)
	db.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 10"}) || !ready("rekeys") {
		t.Errorf("gaps before the first rekey round = %q, ready %t", got, ready("rekeys"))
	}
	db.Close()
	rekeyFrom = map[string]uint64{}

	// A table out of step with its gaps (a hand-edited DB: feature_sync's row lost) is emptied and
	// backfilled whole, so no round is merged twice.
	path = filepath.Join(dir, "lost.duckdb")
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	b := row(0, 0, 0, 0)
	b.rekeys = []rekeyUse{{addrOf(1), addrOf(2), 1}}
	if err := insertBatch(context.Background(), db, []block{b, row(1, 1, 0, 0)}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("DELETE FROM feature_sync"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got, rows := gaps(t, db), rekeyRows(t, db); !slices.Equal(got, []string{"next 2", "1-1"}) || len(rows) > 0 {
		t.Errorf("lost next: gaps %q, rows %q, want the table emptied and one gap", got, rows)
	}
	db.Close()

	// So is one with a gap: the gap is deleted and the same one added again, in one transaction.
	path = filepath.Join(dir, "lostgap.duckdb")
	if db, err = openWith(t, path, config{}); err != nil {
		t.Fatal(err)
	}
	insertRounds(t, db, 0, 9)
	db.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("DELETE FROM feature_sync"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 10", "1-9"}) {
		t.Errorf("lost next with a gap: gaps %q", got)
	}
	db.Close()

	// An empty DB gets no gap and is ready at once; with no blocks, it may be turned off again.
	path = filepath.Join(dir, "empty.duckdb")
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 0"}) || !ready("rekeys") || roundsLeft() != nil {
		t.Errorf("empty: gaps %q, ready %t, left %v", got, ready("rekeys"), roundsLeft())
	}
	insertRounds(t, db, 0, 2)
	if got := gaps(t, db); !slices.Equal(got, []string{"next 3"}) {
		t.Errorf("empty after ingest: gaps %q", got)
	}
	db.Close()
	if _, err := openWith(t, path, config{}); err == nil {
		t.Error("rekeys off after ingest: want refused")
	}
	path = filepath.Join(dir, "empty2.duckdb")
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if db, err = openWith(t, path, config{}); err != nil {
		t.Fatalf("rekeys off on an empty DB: %v", err)
	}
	db.Close()
	// An empty trial DB's ingest starts at START_ROUND.
	startRound = 7
	if db, err = openWith(t, filepath.Join(dir, "trial.duckdb"), config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 7"}) {
		t.Errorf("empty trial: gaps %q", got)
	}
	db.Close()
	// A DB with blocks takes next from its tip, not a stale START_ROUND past it: openData refuses
	// such a start only after setup has committed.
	startRound = 0
	path = filepath.Join(dir, "stale.duckdb")
	if db, err = openWith(t, path, config{}); err != nil {
		t.Fatal(err)
	}
	insertRounds(t, db, 0, 20)
	db.Close()
	startRound = 30
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 21", "1-20"}) {
		t.Errorf("stale START_ROUND: gaps %q", got)
	}
	db.Close()
	startRound = 0
	// An empty DB turned on (next 0) that an older binary then filled from START_ROUND 5: the gap
	// starts at the DB's first round, not below it.
	path = filepath.Join(dir, "later.duckdb")
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if raw, err = sql.Open("duckdb", path); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("INSERT INTO blocks (rnd, ts, tc, fees, txns) SELECT r, r, 0, 0, 0 FROM range(5, 10) t(r)"); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 10", "5-9"}) {
		t.Errorf("DB from round 5: gaps %q", got)
	}
	db.Close()

	// Rekeys doesn't count in the fixed config: other changes are still refused.
	if db, err = openWith(t, filepath.Join(dir, "fixed.duckdb"), config{Proposers: true}); err != nil {
		t.Fatal(err)
	}
	insertRounds(t, db, 0, 0)
	db.Close()
	if _, err := openWith(t, filepath.Join(dir, "fixed.duckdb"), config{Rekeys: true}); err == nil {
		t.Error("PROPOSERS changed with REKEYS turned on: want refused")
	}
}

// memSource serves blocks from memory, counting fetches per round. Its tip is the last block, or tip
// if set. fail, if set, fails a fetch of a round it returns true for.
type memSource struct {
	mu      sync.Mutex
	blocks  []blockCert
	tip     uint64
	fetched map[uint64]int
	fail    func(rnd uint64) bool
}

func (m *memSource) source() source {
	m.fetched = map[uint64]int{}
	return source{
		get: func(ctx context.Context, rnd uint64, headerOnly, cert bool) (blockCert, error) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.fetched[rnd]++
			if m.fail != nil && m.fail(rnd) {
				return blockCert{}, fmt.Errorf("round %d: failed", rnd)
			}
			if rnd >= uint64(len(m.blocks)) {
				return blockCert{}, fmt.Errorf("round %d: no such round", rnd)
			}
			b := m.blocks[rnd]
			b.GenesisID = "test-v1"
			if headerOnly {
				b.Payset = nil
			}
			return b, nil
		},
		tip: func(context.Context, uint64) (uint64, error) {
			m.mu.Lock()
			defer m.mu.Unlock()
			if m.tip > 0 {
				return m.tip, nil
			}
			return uint64(len(m.blocks) - 1), nil
		},
	}
}

func (m *memSource) set(f func(m *memSource)) {
	m.mu.Lock()
	f(m)
	m.mu.Unlock()
}

func (m *memSource) count(rnd uint64) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fetched[rnd]
}

// rekeyChain is rounds 0 to n: every fourth (3, 7, …) and genesis empty, the others rekeying an
// account among 5 to one of 3 authorizers, and every sixth also closing out a rekeyed account. tc
// counts the txns.
func rekeyChain(n int) []blockCert {
	var out []blockCert
	tc := uint64(0)
	for r := range n + 1 {
		var b blockCert
		b.Round, b.TimeStamp = types.Round(r), int64(1000+r)
		if r > 0 && r%4 != 3 {
			acct := types.Address{byte(r%5 + 1)}
			txns := []types.SignedTxnWithAD{tx(acct, rekeyTo(types.Address{byte(r%3 + 10)}))}
			if r%6 == 0 {
				txns = append(txns, tx(types.Address{byte(r%5 + 1)}, signedBy(types.Address{10}), closeTo(types.Address{99})))
			}
			b.Payset = payset(txns...)
			tc += uint64(len(txns))
		}
		b.TxnCounter = tc
		out = append(out, b)
	}
	return out
}

// turnOn ingests chain[:k+1] into a new DB at path with rekeys off, then reopens it with rekeys on,
// leaving a gap of rounds 1 to k.
func turnOn(t *testing.T, path string, chain []blockCert, k int) *sql.DB {
	t.Helper()
	payouts.Store(false)
	db, err := openWith(t, path, config{})
	if err != nil {
		t.Fatal(err)
	}
	m := &memSource{blocks: chain[:k+1]}
	if _, err := syncToTip(context.Background(), db, m.source(), 2, 7); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if db, err = openWith(t, path, config{Rekeys: true}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if got := gaps(t, db); !slices.Equal(got, []string{fmt.Sprintf("next %d", k+1), fmt.Sprintf("1-%d", k)}) {
		t.Fatalf("gaps after turning on = %q", got)
	}
	return db
}

// freshRekeys is the rekeys rows of chain ingested with rekeys on from the start.
func freshRekeys(t *testing.T, chain []blockCert) []string {
	t.Helper()
	payouts.Store(false)
	db, err := openWith(t, filepath.Join(t.TempDir(), "fresh.duckdb"), config{Rekeys: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m := &memSource{blocks: chain}
	if _, err := syncToTip(context.Background(), db, m.source(), 2, 7); err != nil {
		t.Fatal(err)
	}
	return rekeyRows(t, db)
}

// A DB backfilled while live ingest runs ends up with the rows of one ingested with rekeys from the
// start, with no duplicate pairs; each round in the gap is fetched once, and the routes switch on
// when the last gap closes.
func TestBackfill(t *testing.T) {
	defer func(c config, d time.Duration) { track, retryWait = c, d }(track, retryWait)
	retryWait = 0
	ctx := context.Background()
	const n, k = 40, 25 // the chain's last round, and the last before rekeys is turned on
	chain := rekeyChain(n)
	want := freshRekeys(t, chain)
	if len(want) < 10 {
		t.Fatalf("fresh rekeys = %q: too few to test with", want)
	}

	db := turnOn(t, filepath.Join(t.TempDir(), "x.duckdb"), chain, k)
	h := routes(db)
	get := func(url string) (int, string) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", url, nil))
		return w.Code, strings.TrimSpace(w.Body.String())
	}
	A := types.Address{1}.String()
	if code, _ := get("/v0/rekeys/" + A); code != 404 {
		t.Errorf("rekeys before the backfill = %d, want 404", code)
	}

	back := &memSource{blocks: chain}
	bf := backfiller{src: back.source(), concurrency: 3, chunk: 4}
	live := &memSource{blocks: chain}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := syncToTip(ctx, db, live.source(), 2, 3); err != nil {
			t.Error(err)
		}
	}()
	runBackfill(ctx, db, bf)
	wg.Wait()

	if got := rekeyRows(t, db); !slices.Equal(got, want) {
		t.Errorf("backfilled rekeys =\n%q\nwant\n%q", got, want)
	}
	if got := strs(t, db, "SELECT acct || auth FROM rekeys GROUP BY acct, auth HAVING count(*) > 1"); len(got) > 0 {
		t.Errorf("duplicate pairs: %q", got)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{fmt.Sprintf("next %d", n+1)}) {
		t.Errorf("gaps after the backfill = %q", got)
	}
	for r := range uint64(n + 1) {
		if got, want := back.count(r), 0; r >= 1 && r <= k {
			want = 1
			if got != want {
				t.Errorf("round %d fetched %d times by the backfill, want %d", r, got, want)
			}
		} else if got != want {
			t.Errorf("round %d fetched %d times by the backfill, want %d", r, got, want)
		}
	}
	if code, body := get("/v0/rekeys/" + A); code != 200 || !strings.HasPrefix(body, `{"to":[{`) {
		t.Errorf("rekeys after the backfill = %d %s", code, body)
	}
	if _, body := get("/v0/status"); !strings.Contains(body, `"rekeys":true}`) {
		t.Errorf("status after the backfill = %s", body)
	}
}

// A fetch that fails partway writes nothing for its chunk, which is fetched again on retry. A source
// whose tip is below the gap waits without erroring, and continues once it catches up.
func TestBackfillFailure(t *testing.T) {
	defer func(c config, d time.Duration) { track, backfillRetry = c, d }(track, backfillRetry)
	ctx := context.Background()
	chain := rekeyChain(10)
	db := turnOn(t, filepath.Join(t.TempDir(), "x.duckdb"), chain, 10)
	m := &memSource{blocks: chain}
	var failing atomic.Bool
	failing.Store(true)
	m.fail = func(rnd uint64) bool { return rnd == 5 && failing.Load() }
	bf := backfiller{src: m.source(), concurrency: 2, chunk: 8}
	if _, _, _, err := bf.step(ctx, db, rekeysJob); err == nil {
		t.Fatal("step with a failing fetch: want an error")
	}
	if got := rekeyRows(t, db); len(got) > 0 {
		t.Errorf("rekeys after a failed step = %q, want none", got)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 11", "1-10"}) {
		t.Errorf("gaps after a failed step = %q", got)
	}
	// runBackfill retries after backfillRetry.
	backfillRetry = 10 * time.Millisecond
	time.AfterFunc(50*time.Millisecond, func() { failing.Store(false) })
	runBackfill(ctx, db, bf)
	if m.count(5) < 2 || !ready("rekeys") {
		t.Errorf("round 5 fetched %d times, ready %t: want a retry and done", m.count(5), ready("rekeys"))
	}
	if got, want := rekeyRows(t, db), freshRekeys(t, chain); !slices.Equal(got, want) {
		t.Errorf("rekeys after the retry = %q, want %q", got, want)
	}

	// Behind: a source at round 4 fills 1-4, then waits, logging once and not at ERROR.
	db = turnOn(t, filepath.Join(t.TempDir(), "y.duckdb"), chain, 10)
	m = &memSource{blocks: chain, tip: 4}
	bf = backfiller{src: m.source(), concurrency: 2, chunk: 8}
	var buf syncBuf
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	time.AfterFunc(80*time.Millisecond, func() { m.set(func(m *memSource) { m.tip = 0 }) })
	runBackfill(ctx, db, bf)
	logs := buf.String()
	if strings.Count(logs, `"msg":"backfill waiting"`) != 1 || strings.Contains(logs, `"level":"ERROR"`) || !ready("rekeys") {
		t.Errorf("behind: ready %t, logs:\n%s", ready("rekeys"), logs)
	}
	if m.count(5) != 1 {
		t.Errorf("behind: round 5 fetched %d times, want once (after catching up)", m.count(5))
	}
}

// syncBuf is a bytes.Buffer safe for concurrent writes.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// A commit that fails after the merge, or is cut short by shutdown, leaves the gap and the rows as
// they were.
func TestBackfillCommitFails(t *testing.T) {
	defer func(c config) { track = c }(track)
	chain := rekeyChain(10)
	db := turnOn(t, filepath.Join(t.TempDir(), "x.duckdb"), chain, 10)
	m := &memSource{blocks: chain}
	bf := backfiller{src: m.source(), concurrency: 2, chunk: 4}
	failing := rekeysJob
	failing.merge = func(ctx context.Context, tx *sql.Tx, rows []block) error {
		if err := mergeRekeys(ctx, tx, rows); err != nil {
			return err
		}
		return errors.New("disk full")
	}
	ctx, cancel := context.WithCancel(context.Background())
	shutdown := rekeysJob
	shutdown.merge = func(c context.Context, tx *sql.Tx, rows []block) error {
		err := mergeRekeys(c, tx, rows)
		cancel() // SIGTERM mid-commit
		return err
	}
	for _, j := range []job{failing, shutdown} {
		if _, _, _, err := bf.step(ctx, db, j); err == nil {
			t.Errorf("step with a failing commit: want an error")
		}
		if got := gaps(t, db); !slices.Equal(got, []string{"next 11", "1-10"}) || len(rekeyRows(t, db)) > 0 {
			t.Errorf("after a failed commit: gaps %q, rows %q", got, rekeyRows(t, db))
		}
	}
	// The next start picks up from the gaps.
	runBackfill(context.Background(), db, bf)
	if got, want := rekeyRows(t, db), freshRekeys(t, chain); !slices.Equal(got, want) {
		t.Errorf("rekeys after resuming = %q, want %q", got, want)
	}
}

// A LEDGER_DB import drains the gaps before it exits.
func TestImportDrainsBackfill(t *testing.T) {
	defer func(c config) { track = c }(track)
	chain := rekeyChain(30)
	db := turnOn(t, filepath.Join(t.TempDir(), "x.duckdb"), chain, 20)
	m := &memSource{blocks: chain}
	if err := runImport(context.Background(), db, m.source(), 2, 6); err != nil {
		t.Fatal(err)
	}
	if got := gaps(t, db); !slices.Equal(got, []string{"next 31"}) || !ready("rekeys") {
		t.Errorf("gaps after the import = %q, ready %t", got, ready("rekeys"))
	}
	if got, want := rekeyRows(t, db), freshRekeys(t, chain); !slices.Equal(got, want) {
		t.Errorf("rekeys after the import = %q, want %q", got, want)
	}
}

// A LEDGER_DB import from a ledger behind the DB fills the gaps up to the ledger's tip and leaves the
// rest to the service, without failing, on a rerun too.
func TestImportBehindLedger(t *testing.T) {
	defer func(c config) { track = c }(track)
	chain := rekeyChain(30)
	db := turnOn(t, filepath.Join(t.TempDir(), "x.duckdb"), chain, 20)
	want := freshRekeys(t, chain[:16]) // rounds 0-15
	m := &memSource{blocks: chain, tip: 15}
	for run := range 2 {
		if err := runImport(context.Background(), db, m.source(), 2, 6); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if got := gaps(t, db); !slices.Equal(got, []string{"next 21", "16-20"}) || ready("rekeys") || roundsLeft()["rekeys"] != 5 {
			t.Errorf("run %d: gaps after the import = %q, ready %t, roundsLeft %v", run, got, ready("rekeys"), roundsLeft())
		}
		if got := rekeyRows(t, db); !slices.Equal(got, want) {
			t.Errorf("run %d: rekeys after the import = %q, want rounds 1-15's %q", run, got, want)
		}
	}
}

// BACKFILL_RATE caps fetch attempts per second, retries included, and BACKFILL_CONCURRENCY those in
// flight, as an algod sees them.
func TestBackfillLimits(t *testing.T) {
	defer func(c config, d time.Duration) { track, retryWait = c, d }(track, retryWait)
	retryWait = 0
	const n, conc, perSec = 20, 3, 40
	var chain []blockCert
	for r := range n + 1 { // every round has a txn
		var b blockCert
		b.Round, b.TxnCounter, b.Payset = types.Round(r), uint64(r+1), payset(tx(types.Address{1}))
		chain = append(chain, b)
	}
	db := turnOn(t, filepath.Join(t.TempDir(), "x.duckdb"), chain, n)
	var reqs, inFlight, peak atomic.Int32
	var mu sync.Mutex
	tried := map[int]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v2/status/wait-for-block-after/") {
			json.NewEncoder(w).Encode(models.NodeStatus{LastRound: n})
			return
		}
		reqs.Add(1)
		c := inFlight.Add(1)
		defer inFlight.Add(-1)
		for p := peak.Load(); c > p && !peak.CompareAndSwap(p, c); p = peak.Load() {
		}
		time.Sleep(5 * time.Millisecond)
		var rnd int
		fmt.Sscanf(r.URL.Path, "/v2/blocks/%d", &rnd)
		mu.Lock()
		first := !tried[rnd]
		tried[rnd] = true
		mu.Unlock()
		if first { // every round's first attempt fails, so each is fetched twice
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		b := chain[rnd]
		b.GenesisID = "test-v1"
		w.Write(msgpack.Encode(b))
	}))
	defer srv.Close()
	clearEnv(t)
	i := strings.LastIndex(srv.URL, ":")
	setEnv(t, map[string]string{"BACKFILL_ALGOD_HOST": srv.URL[:i], "BACKFILL_ALGOD_PORT": srv.URL[i+1:]})
	a, err := algodClients(true, conc)
	if err != nil {
		t.Fatal(err)
	}
	// Twice the workers: the transport's per-host cap must hold on its own.
	bf := backfiller{src: algodSource(a, "test-v1", newRateLimiter(perSec, conc)), concurrency: 2 * conc, chunk: 100}
	start := time.Now()
	runBackfill(context.Background(), db, bf)
	took := time.Since(start)
	if got := reqs.Load(); got != 2*n || !ready("rekeys") {
		t.Fatalf("%d block requests, ready %t: want %d (each retried once) and done", got, ready("rekeys"), 2*n)
	}
	// A burst of conc, then perSec.
	if min := time.Duration(2*n-conc) * time.Second / perSec; took < min*9/10 {
		t.Errorf("%d attempts took %s, want at least %s at %d/s", 2*n, took, min, perSec)
	}
	if p := peak.Load(); p > conc {
		t.Errorf("%d requests in flight, want at most %d", p, conc)
	}
}

func TestRekeyRoutes(t *testing.T) {
	defer func(c config) { track = c }(track)
	db, err := openWith(t, "", config{Rekeys: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	A, B, C, D, E := addrOf(1), addrOf(2), addrOf(3), addrOf(4), addrOf(5)
	at := func(rnd uint64, uses ...rekeyUse) block { b := row(rnd, rnd, 0, 0); b.rekeys = uses; return b }
	// A to B at 1 and 3, back to itself at 4; C to A at 2, D to A at 5.
	if err := insertBatch(context.Background(), db, []block{
		at(0), at(1, rekeyUse{A, B, 1}), at(2, rekeyUse{C, A, 1}), at(3, rekeyUse{A, B, 1}), at(4, rekeyUse{A, A, 1}), at(5, rekeyUse{D, A, 1}),
	}); err != nil {
		t.Fatal(err)
	}
	h := routes(db)
	get := func(url string) (int, string) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", url, nil))
		return w.Code, strings.TrimSpace(w.Body.String())
	}
	e := func(a string, txns, first, last int) string {
		return fmt.Sprintf(`{"address":"%s","txns":%d,"firstRound":%d,"lastRound":%d}`, a, txns, first, last)
	}
	ok := http.StatusOK
	for _, c := range []struct {
		url  string
		code int
		want string
	}{
		{"/v0/rekeys/" + A, ok, `{"to":[` + e(A, 1, 4, 4) + "," + e(B, 2, 1, 3) + `],"from":[` + e(D, 1, 5, 5) + "," + e(C, 1, 2, 2) + `]}`},
		{"/v0/rekeys/" + A + "?limit=1", ok, `{"to":[` + e(A, 1, 4, 4) + `],"from":[` + e(D, 1, 5, 5) + `]}`},
		// The self row (round 4) is newer than C's: the limit counts from's rows after leaving it out.
		{"/v0/rekeys/" + A + "?limit=2", ok, `{"to":[` + e(A, 1, 4, 4) + "," + e(B, 2, 1, 3) + `],"from":[` + e(D, 1, 5, 5) + "," + e(C, 1, 2, 2) + `]}`},
		{"/v0/rekeys/" + B, ok, `{"to":[],"from":[` + e(A, 2, 1, 3) + `]}`},
		{"/v0/rekeys/" + E, ok, `{"to":[],"from":[]}`},
		{"/v0/rekeys/nope", 400, ""},
		{"/v0/rekeys/" + A + "?limit=0", 400, ""},
		{"/v0/account/" + A, ok, `{"rekeysTo":2,"rekeysFrom":2}`},
		{"/v0/account/" + E, ok, `{"rekeysTo":0,"rekeysFrom":0}`},
	} {
		code, body := get(c.url)
		if code != c.code || (c.code == ok && body != c.want) {
			t.Errorf("%s = %d %s, want %d %s", c.url, code, body, c.code, c.want)
		}
	}
	// Each account count is the length of its list.
	for _, a := range []string{A, B, C, D, E} {
		var acc struct{ RekeysTo, RekeysFrom int }
		var list struct{ To, From []json.RawMessage }
		_, body := get("/v0/account/" + a)
		_, lbody := get("/v0/rekeys/" + a)
		if json.Unmarshal([]byte(body), &acc) != nil || json.Unmarshal([]byte(lbody), &list) != nil ||
			acc.RekeysTo != len(list.To) || acc.RekeysFrom != len(list.From) {
			t.Errorf("%s: account %s, list %s", a, body, lbody)
		}
	}

	// While backfilling: the routes and the account keys are off; with only rekeys tracked, the
	// account route 404s.
	setLeft("rekeys", 5)
	for _, url := range []string{"/v0/rekeys/" + A, "/v0/account/" + A} {
		if code, _ := get(url); code != 404 {
			t.Errorf("%s while backfilling = %d, want 404", url, code)
		}
	}
	track = config{Rekeys: true, VoterRounds: 10}
	h = routes(db)
	if code, body := get("/v0/account/" + A); code != ok || body != `{"voted":false}` {
		t.Errorf("account while backfilling = %d %s", code, body)
	}
	setLeft("rekeys", 0)
	if code, body := get("/v0/account/" + A); code != ok || body != `{"voted":false,"rekeysTo":2,"rekeysFrom":2}` {
		t.Errorf("account once backfilled = %d %s", code, body)
	}

	track = config{}
	h = routes(db)
	if code, _ := get("/v0/rekeys/" + A); code != 404 {
		t.Errorf("rekeys with the feature off = %d, want 404", code)
	}
}

// The rekeys routes look up acct and auth through their indexes, not a scan of rekeys. Plain EXPLAIN
// prints a sequential scan even when the index runs, so this reads EXPLAIN ANALYZE.
func TestRekeyQueriesUseIndex(t *testing.T) {
	defer func(c config) { track = c }(track)
	db, err := openWith(t, "", config{Rekeys: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	A, B, C := addrOf(1), addrOf(2), addrOf(3)
	if _, err := db.Exec("INSERT INTO rekeys VALUES (?, ?, 1, 1, 1), (?, ?, 1, 2, 2), (?, ?, 1, 3, 3)", A, B, C, A, A, A); err != nil {
		t.Fatal(err)
	}
	// scans counts the plan's index and sequential scans.
	scans := func(q string, args ...any) (index, seq int) {
		t.Helper()
		rows, err := db.Query("EXPLAIN ANALYZE "+q, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var k, plan string
			if err := rows.Scan(&k, &plan); err != nil {
				t.Fatal(err)
			}
			index, seq = index+strings.Count(plan, "Type: Index Scan"), seq+strings.Count(plan, "Type: Sequential Scan")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return index, seq
	}
	for name, q := range map[string]struct {
		sql  string
		args []any
	}{
		"/v0/rekeys":  {rekeysSQL, []any{10, A}},
		"/v0/account": {"SELECT to_json({" + strings.Join(rekeyCounts, ", ") + "})::VARCHAR", []any{A}},
	} {
		if index, seq := scans(q.sql, q.args...); index != 2 || seq != 0 {
			t.Errorf("%s: %d index scans, %d sequential, want 2 and 0", name, index, seq)
		}
	}
	// The check can see a scan: an OR of the two lookups is one.
	if index, seq := scans("SELECT count(*) FROM rekeys WHERE acct = $1 OR auth = $1", A); index != 0 || seq != 1 {
		t.Errorf("OR: %d index scans, %d sequential, want 0 and 1", index, seq)
	}
}

// check fails a backfill source that lacks the first round the backfill may need.
func TestCheckBackfillArchival(t *testing.T) {
	defer func(d time.Duration, m map[string]uint64) { retryWait, rekeyFrom = d, m }(retryWait, rekeyFrom)
	retryWait = 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/transactions/params" {
			fmt.Fprint(w, `{"genesis-id":"test-v1","genesis-hash":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","last-round":10,"fee":0,"min-fee":1000,"consensus-version":"v"}`)
			return
		}
		var rnd int
		fmt.Sscanf(r.URL.Path, "/v2/blocks/%d", &rnd)
		if rnd < 5 && r.URL.Query().Get("header-only") != "true" { // pruned, but serves headers: check must fetch what the backfill does
			http.Error(w, `{"message":"ledger does not have entry"}`, http.StatusNotFound)
			return
		}
		var b types.Block
		b.Round, b.GenesisID = types.Round(rnd), "test-v1"
		w.Write(msgpack.Encode(models.BlockResponse{Block: b}))
	}))
	defer srv.Close()
	clearEnv(t)
	setEnv(t, map[string]string{"ALGOD_HOST": srv.URL[:strings.LastIndex(srv.URL, ":")], "ALGOD_PORT": srv.URL[strings.LastIndex(srv.URL, ":")+1:]})
	for _, c := range []struct {
		rekeys      string
		from, start uint64
		want        int
	}{
		{"", 0, 0, 0}, {"1", 5, 0, 0}, {"1", 3, 0, 1}, {"1", 0, 0, 1}, // from 0: unknown, so round 1
		{"1", 3, 5, 0}, {"1", 0, 5, 0}, // the DB starts at START_ROUND: no round below it is needed
	} {
		t.Setenv("REKEYS", c.rekeys)
		t.Setenv("START_ROUND", fmt.Sprint(c.start))
		rekeyFrom = map[string]uint64{"test-v1": c.from}
		if got := check([]string{"test-v1"}); got != c.want {
			t.Errorf("REKEYS=%q from round %d START_ROUND=%d: check = %d, want %d", c.rekeys, c.from, c.start, got, c.want)
		}
	}
}
