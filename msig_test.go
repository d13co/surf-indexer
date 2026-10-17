package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/algorand/go-algorand-sdk/v2/crypto"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

// msigOf is a v1 multisig of keys, with a sig from each key whose signs entry is set.
func msigOf(thr uint8, keys []types.Address, signs ...bool) types.MultisigSig {
	m := types.MultisigSig{Version: 1, Threshold: thr}
	for i, k := range keys {
		s := types.MultisigSubsig{Key: k[:]}
		if i < len(signs) && signs[i] {
			s.Sig[0] = 1
		}
		m.Subsigs = append(m.Subsigs, s)
	}
	return m
}

// msigAddr is keys' multisig address, as go-algorand derives it.
func msigAddr(t *testing.T, thr uint8, keys ...types.Address) string {
	t.Helper()
	ma, err := crypto.MultisigAccountWithParams(1, thr, keys)
	if err != nil {
		t.Fatal(err)
	}
	a, err := ma.Address()
	if err != nil {
		t.Fatal(err)
	}
	return a.String()
}

func TestMsigUses(t *testing.T) {
	K1, K2, K3, X := types.Address{'1'}, types.Address{'2'}, types.Address{'3'}, types.Address{'X'}
	keys := []types.Address{K1, K2, K3}
	m23, m12, m11 := msigAddr(t, 2, keys...), msigAddr(t, 1, K1, K2), msigAddr(t, 1, K1, K1)

	var plain, direct, rekeyed, delegated, lmsig, orphan, dup types.SignedTxnInBlock
	plain.Sig[0] = 1
	direct.Msig = msigOf(2, keys, true, false, true)
	rekeyed.Txn.Sender, rekeyed.AuthAddr = X, X // the address comes from the preimage, not these
	rekeyed.Msig = msigOf(2, keys, false, true, true)
	delegated.Lsig.Logic, delegated.Lsig.Msig = []byte{6, 0x81, 1}, msigOf(1, keys[:2], false, true)
	lmsig.Lsig.Logic, lmsig.Lsig.LMsig = []byte{6, 0x81, 1}, msigOf(1, keys[:2], true)
	orphan.Lsig.Msig = types.MultisigSig{Version: 9, Subsigs: []types.MultisigSubsig{{Key: K1[:]}}} // no program: ignored
	dup.Msig = msigOf(1, []types.Address{K1, K1}, false, true)
	var b types.Block
	b.Payset = []types.SignedTxnInBlock{plain, direct, rekeyed, delegated, lmsig, orphan, dup}

	u := func(k types.Address, msig string, signed bool, pos uint32) msigUse {
		return msigUse{k.String(), msig, signed, pos}
	}
	want := []msigUse{
		u(K1, m23, true, 1), u(K2, m23, false, 1), u(K3, m23, true, 1),
		u(K1, m23, false, 2), u(K2, m23, true, 2), u(K3, m23, true, 2),
		u(K1, m12, false, 3), u(K2, m12, true, 3),
		u(K1, m12, true, 4), u(K2, m12, false, 4),
		u(K1, m11, false, 6), u(K1, m11, true, 6),
	}
	if got := msigUses(b); !slices.Equal(got, want) {
		t.Errorf("msigUses = %+v\nwant %+v", got, want)
	}

	// A preimage this build can't read is skipped, and the block's other msigs still count.
	for name, m := range map[string]types.MultisigSig{
		"version":   {Version: 2, Threshold: 1, Subsigs: []types.MultisigSubsig{{Key: K1[:]}}},
		"threshold": {Version: 1, Threshold: 2, Subsigs: []types.MultisigSubsig{{Key: K1[:]}}},
		"pk length": {Version: 1, Threshold: 1, Subsigs: []types.MultisigSubsig{{Key: K1[:31]}}},
	} {
		var bad types.Block
		bad.Payset = []types.SignedTxnInBlock{{}, direct}
		bad.Payset[0].Msig = m
		if got := msigUses(bad); len(got) != 3 || got[0].pos != 1 {
			t.Errorf("%s: msigUses = %+v, want direct's 3 uses only", name, got)
		}
	}
}

func TestMergeMsigs(t *testing.T) {
	defer func(c config) { track = c }(track)
	track = config{Msigs: true}
	ctx := context.Background()
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	at := func(rnd uint64, uses ...msigUse) block { b := row(rnd, rnd, 0, 0); b.msigs = uses; return b }
	u := func(signer string, signed bool, pos uint32) msigUse { return msigUse{signer, "M", signed, pos} }
	// A signs first at 6 pos 1 (not 6 pos 3, nor 9); B signs at its first use; C only in the second
	// batch; D never signs and keeps its first use; F is a duplicate pk, one subsig signed.
	// The last batch replays older rounds: a better pointer (signed, then earlier) still wins.
	for _, batch := range [][]block{
		{at(5, u("A", false, 0), u("B", true, 0), u("D", false, 0)), at(6, u("A", false, 0), u("A", true, 1), u("B", true, 2), u("A", true, 3)), at(7, u("C", false, 4))},
		{at(8), at(9, u("C", true, 1), u("A", true, 0), u("D", false, 2), u("E", false, 0), u("F", false, 3), u("F", true, 3))},
		{at(10)},
		{at(3, u("A", false, 0), u("E", false, 1)), at(4, u("C", true, 0), u("B", false, 0))},
	} {
		if err := insertBatch(ctx, db, batch); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"A M true 6 1", "B M true 5 0", "C M true 4 0", "D M false 5 0", "E M false 3 1", "F M true 9 3"}
	if got := strs(t, db, "SELECT concat_ws(' ', signer, msig, signed, rnd, pos) FROM msig_signers ORDER BY signer"); !slices.Equal(got, want) {
		t.Errorf("msig_signers = %q, want %q", got, want)
	}
}

func TestMsigRoutes(t *testing.T) {
	defer func(c config) { track = c }(track)
	track = config{Msigs: true}
	ctx := context.Background()
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	A, B, C, M, N := addrOf(1), addrOf(2), addrOf(3), addrOf(4), addrOf(5)
	at := func(rnd uint64, uses ...msigUse) block { b := row(rnd, rnd, 0, 0); b.msigs = uses; return b }
	if err := insertBatch(ctx, db, []block{
		at(1, msigUse{A, M, true, 0}, msigUse{B, M, false, 0}),
		at(2, msigUse{A, N, false, 3}, msigUse{C, N, true, 3}),
	}); err != nil {
		t.Fatal(err)
	}
	h := routes(db)
	get := func(url string) (int, string) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", url, nil))
		return w.Code, strings.TrimSpace(w.Body.String())
	}
	of := func(key, a string, signed bool, rnd, pos int) string {
		return fmt.Sprintf(`{"%s":"%s","signed":%t,"round":%d,"index":%d}`, key, a, signed, rnd, pos)
	}
	ok := http.StatusOK
	for _, c := range []struct {
		url  string
		code int
		want string
	}{
		{"/v0/subsigner/" + A, ok, "[" + of("msig", N, false, 2, 3) + "," + of("msig", M, true, 1, 0) + "]"},
		{"/v0/subsigner/" + A + "?limit=1", ok, "[" + of("msig", N, false, 2, 3) + "]"},
		{"/v0/subsigner/" + M, ok, "[]"}, // a multisig, not a subsigner
		{"/v0/subsigner/nope", 400, ""},
		{"/v0/msig/" + M, ok, "[" + of("signer", A, true, 1, 0) + "," + of("signer", B, false, 1, 0) + "]"},
		{"/v0/msig/" + N + "?limit=0", 400, ""},
		{"/v0/msig/nope", 400, ""},
		{"/v0/msig/" + A, ok, "[]"},
	} {
		code, body := get(c.url)
		if code != c.code || (c.code == ok && body != c.want) {
			t.Errorf("%s = %d %s, want %d %s", c.url, code, body, c.code, c.want)
		}
	}

	track = config{}
	h = routes(db)
	for _, url := range []string{"/v0/subsigner/" + A, "/v0/msig/" + M} {
		if code, _ := get(url); code != 404 {
			t.Errorf("%s with msigs off = %d, want 404", url, code)
		}
	}
}
