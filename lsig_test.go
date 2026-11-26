package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/algorand/go-algorand-sdk/v2/crypto"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

// v6 assembles: intcblock 1 x; bytecblock b; txn <field>; bytec_0; ==; txn TypeEnum; intc_0; ==; &&
func v6(x uint64, b []byte, field byte) []byte {
	p := binary.AppendUvarint([]byte{6, 0x20, 2, 1}, x)
	p = append(binary.AppendUvarint(append(p, 0x26, 1), uint64(len(b))), b...)
	return append(p, 0x31, field, 0x28, 0x12, 0x31, 0x10, 0x22, 0x12, 0x10)
}

func TestTemplate(t *testing.T) {
	addr := bytes.Repeat([]byte{'a'}, 32)
	base := template(v6(5, addr, 7))
	same := func(name string, a, b []byte) {
		t.Helper()
		if template(a) != template(b) {
			t.Errorf("%s: want the same template", name)
		}
	}
	differ := func(name string, a, b []byte) {
		t.Helper()
		if template(a) == template(b) {
			t.Errorf("%s: want different templates", name)
		}
	}
	if len(base) != 16 {
		t.Errorf("template id %q: want 16 hex digits", base)
	}
	same("block values of other lengths", v6(5, addr, 7), v6(300000, []byte("xyz"), 7))
	// As goal assembles one source: a filled-in value that equals another constant merges into it,
	// changing the block's count and order, its loads' indexes, and pushint to intc (mainnet ids
	// cc42304cb3da6aec, 045a3215de861249).
	// intcblock 7 9; intc_1; intc_0; pushint 9; bytecblock ""; bytec 0 vs intcblock 9; intc_0; pushint 7; intc_0; pushbytes ""
	same("constant blocks and loads", []byte{8, 0x20, 2, 7, 9, 0x23, 0x22, 0x81, 9, 0x26, 1, 0, 0x27, 0},
		[]byte{8, 0x20, 1, 9, 0x22, 0x81, 7, 0x22, 0x80, 0})
	differ("int vs bytes constant", []byte{8, 0x81, 1}, []byte{8, 0x80, 1, 1})
	same("pushints values", []byte{8, 0x83, 2, 1, 2}, []byte{8, 0x83, 2, 0x80, 0x01, 9})
	// Puya combines single-use pushes, so a multi-push is a push per value. pushints 1 2 vs pushint 1; intc_0
	same("pushints as pushes", []byte{8, 0x83, 2, 1, 2}, []byte{8, 0x81, 1, 0x22})
	differ("pushints count", []byte{8, 0x83, 2, 1, 2}, []byte{8, 0x83, 3, 1, 2, 3})
	// pushbytess "a" "abc" vs "xy" "": walked as ints, the values would be read as ops.
	same("pushbytess values", []byte{8, 0x82, 2, 1, 'a', 3, 'a', 'b', 'c'}, []byte{8, 0x82, 2, 2, 'x', 'y', 0})
	// From v13 goal autosalts a program whose address is an Ed25519 point: a last entry in its own
	// intcblock, or else a trailing intcblock 1 s. Salted or not, it's one template.
	same("autosalt in the block", []byte{13, 0x20, 1, 5, 0x22, 0x22}, []byte{13, 0x20, 2, 5, 3, 0x22, 0x22})
	same("autosalt trailing", []byte{13, 0x81, 5}, []byte{13, 0x81, 5, 0x20, 1, 2})
	differ("txn field", v6(5, addr, 7), v6(5, addr, 8))
	other := v6(5, addr, 7)
	other[0] = 5
	differ("version", v6(5, addr, 7), other)
	// b over (pushbytes s; pop); pushint 1: a longer s shifts the b offset.
	pushB := func(s string) []byte {
		p := append([]byte{8, 0x42, 0, byte(3 + len(s)), 0x80, byte(len(s))}, s...)
		return append(p, 0x48, 0x81, 1)
	}
	same("pushbytes and branch offset", pushB("a"), pushB("abc"))
	differ("op after", pushB("a"), append(pushB("a"), 0x48))
	// bnz, bz, b, callsub: an int16 offset (0 1 vs 0x12 0x34, which walked as ops differ) to v12, a
	// zigzag varint (1 or 2 bytes) from v13; err; pushint x.
	for _, op := range []byte{0x40, 0x41, 0x42, 0x88} {
		br := func(ver byte, off []byte, x byte) []byte { return append(append([]byte{ver, op}, off...), 0, 0x81, x) }
		same(fmt.Sprintf("v12 branch %#x", op), br(12, []byte{0, 1}, 5), br(12, []byte{0x12, 0x34}, 7))
		same(fmt.Sprintf("v13 branch %#x", op), br(13, []byte{2}, 5), br(13, []byte{0x90, 0x03}, 7))
	}
	// switch, match with 200 labels (a count > 127, so not a varuint), each 0x80 0x80, which walked
	// as ops would fail; pushint x. Built fresh each time, so the two don't share a backing array.
	for _, op := range []byte{0x8d, 0x8e} {
		sw := func(x byte) []byte {
			return append(append([]byte{8, op, 200}, bytes.Repeat([]byte{0x80}, 400)...), 0x81, x)
		}
		same(fmt.Sprintf("%#x 200 labels", op), sw(5), sw(7))
	}
	// Every other op reads exactly its fixed immediates (langspec v1..v13, sig mode), here 0x80s, then
	// pushbytes x x x: reading one too few leaves a pushbytes whose length runs past the end, and
	// reading too many walks x as ops, so either changes the id with x.
	imm := map[byte]int{0x37: 3}
	for _, op := range []byte{0x33, 0x36, 0x39, 0x51, 0x57, 0x8a, 0xc1} {
		imm[op] = 2
	}
	for _, op := range []byte{0x05, 0x06, 0x07, 0x21, 0x27, 0x2c, 0x31, 0x32, 0x34, 0x35, 0x38, 0x45, 0x46, 0x47,
		0x4b, 0x4e, 0x4f, 0x5c, 0x5e, 0x5f, 0x8b, 0x8c, 0xc0, 0xc2, 0xd0, 0xd1, 0xe0, 0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7} {
		imm[op] = 1
	}
	for i := range 256 {
		op := byte(i)
		switch op {
		case 0x20, 0x26, 0x40, 0x41, 0x42, 0x80, 0x81, 0x82, 0x83, 0x88, 0x8d, 0x8e:
			continue
		}
		p := func(x byte) []byte {
			return append(append([]byte{13, op}, bytes.Repeat([]byte{0x80}, imm[op])...), 0x80, 3, x, x, x)
		}
		same(fmt.Sprintf("op %#x", op), p(1), p(2))
	}
	// falcon_verify has no immediate through v13, and a FalconConfigs field from v14; rsa_verify (v14) a field too.
	same("falcon_verify v13", []byte{13, 0x85, 0x81, 5}, []byte{13, 0x85, 0x81, 7})
	same("falcon_verify v14", []byte{14, 0x85, 0, 0x81, 5}, []byte{14, 0x85, 0, 0x81, 7})
	differ("falcon_verify v14 field", []byte{14, 0x85, 0, 0x81, 5}, []byte{14, 0x85, 1, 0x81, 5})
	same("rsa_verify v14", []byte{14, 0xe8, 0, 0x80, 1, 'a'}, []byte{14, 0xe8, 0, 0x80, 2, 'b', 'c'})
	same("v14", []byte{14, 0x81, 5}, []byte{14, 0x81, 7})
	// A truncated program, or a version past the table, is its own template.
	full := v6(5, addr, 7)
	differ("truncated in bytecblock", full, full[:12])
	differ("truncated, same prefix", full[:12], full[:13])
	differ("v15", []byte{15, 0x81, 5}, []byte{15, 0x81, 7})
	// A fallback hashes the program, prefixed so it can't equal a skeleton: version 83 ('S') vs pushint.
	differ("fallback vs skeleton", []byte{'S', 8, 0x81}, []byte{8, 0x81, 5})
	differ("varint past the end", []byte{6, 0x81, 0x80}, []byte{6, 0x81, 0x81})
}

func TestLsigUses(t *testing.T) {
	addr := bytes.Repeat([]byte{'a'}, 32)
	progA, progA2, progB := v6(5, addr, 7), v6(6, addr, 7), []byte{6, 0x81, 1}
	at := func(p []byte) types.Address { return crypto.LogicSigAddress(types.LogicSig{Logic: p}) }
	tx := func(p []byte, sender, auth types.Address) types.SignedTxnInBlock {
		var t types.SignedTxnInBlock
		t.Lsig.Logic, t.Txn.Sender, t.AuthAddr = p, sender, auth
		return t
	}
	X, Y, M := types.Address{'X'}, types.Address{'Y'}, types.Address{'M'}
	plain := tx(nil, Y, types.Address{})
	plain.Sig[0] = 1
	orphan := tx(nil, Y, types.Address{})
	orphan.Lsig.Args = [][]byte{{1}}
	delegated := tx(progB, Y, types.Address{})
	delegated.Lsig.Sig[0] = 1
	viaMsig := tx(progB, X, M)
	viaMsig.Lsig.Msig.Version = 1
	selfAuth := tx(progB, Y, Y) // AuthAddr == Sender, allowed before v42: not rekeyed
	selfAuth.Lsig.Sig[0] = 1
	var b types.Block
	b.Payset = []types.SignedTxnInBlock{
		plain,
		tx(progA, at(progA), types.Address{}),
		tx(progA, at(progA), types.Address{}),
		tx(progA2, at(progA2), types.Address{}), // same template: no logic
		tx(progB, X, at(progB)),                 // escrow, X rekeyed to it
		delegated,
		viaMsig,
		orphan,
		selfAuth,
	}
	b64 := base64.StdEncoding.EncodeToString
	tA, tB := template(progA), template(progB)
	want := []lsigUse{
		{at(progA).String(), "", "", tA, b64(progA), 2},
		{at(progA2).String(), "", "", tA, "", 1},
		{at(progB).String(), "", X.String(), tB, b64(progB), 1},
		{at(progB).String(), Y.String(), "", tB, "", 2},
		{at(progB).String(), M.String(), X.String(), tB, "", 1},
	}
	if got := lsigUses(b); !slices.Equal(got, want) {
		t.Errorf("lsigUses = %+v\nwant %+v", got, want)
	}
}
