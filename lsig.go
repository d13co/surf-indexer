package main

import (
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"

	"github.com/algorand/go-algorand-sdk/v2/crypto"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

// lsigUse is a block's txns authorized by one lsig program for one (signer, sender).
type lsigUse struct {
	prog, signer, sender, tmpl string
	logic                      string // base64; set only on the block's first use of tmpl
	n                          uint64 // uses in the block
}

// lsigUses returns the block's top-level txns authorized by a lsig (inner txns carry no signatures),
// folded per (prog, signer, sender) to bound a batch's memory. The block was verified, so for auth,
// the txn's authorizer: an escrow has auth == hash(logic), and any delegation sig (sig, msig, lmsig,
// pqsig) was checked against auth (go-algorand verify/txn.go logicSigSanityCheckBatchPrep).
func lsigUses(b types.Block) (uses []lsigUse) {
	idx, tseen := map[[3]string]int{}, map[string]bool{}
	for _, t := range b.Payset {
		// Program-less lsig content was accepted unchecked before v42: not a lsig.
		if len(t.Lsig.Logic) == 0 {
			continue
		}
		auth := t.Txn.Sender
		if !t.AuthAddr.IsZero() {
			auth = t.AuthAddr
		}
		prog := crypto.LogicSigAddress(t.Lsig)
		u := lsigUse{prog: prog.String()}
		if auth != prog {
			u.signer = auth.String()
		}
		if t.Txn.Sender != auth {
			u.sender = t.Txn.Sender.String()
		}
		k := [3]string{u.prog, u.signer, u.sender}
		if i, ok := idx[k]; ok {
			uses[i].n++
			continue
		}
		u.tmpl, u.n = template(t.Lsig.Logic), 1
		if !tseen[u.tmpl] {
			tseen[u.tmpl] = true
			u.logic = base64.StdEncoding.EncodeToString(t.Lsig.Logic)
		}
		idx[k] = len(uses)
		uses = append(uses, u)
	}
	return uses
}

// immBytes is the fixed immediate size of each opcode a lsig may use, from go-algorand's
// langspec_v1..v13.json (app-only ops fail the static check in a lsig); the rest take none.
// The variable-length ops, and branches (whose offsets changed encoding in v13), are in template's switch.
var immBytes = func() (t [256]uint8) {
	for _, op := range []byte{0x05, 0x06, 0x07, 0x21, 0x27, 0x2c, 0x31, 0x32, 0x34, 0x35, 0x38, 0x45, 0x46, 0x47,
		0x4b, 0x4e, 0x4f, 0x5c, 0x5e, 0x5f, 0x8b, 0x8c, 0xc0, 0xc2, 0xd0, 0xd1, 0xe0, 0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7} {
		t[op] = 1
	}
	for _, op := range []byte{0x33, 0x36, 0x39, 0x51, 0x57, 0x8a, 0xc1} {
		t[op] = 2
	}
	t[0x37] = 3
	return t
}()

// template ids a lsig program's template: the hash of its skeleton, which keeps opcodes and fixed
// immediates and leaves out constants (blocks, and which value each load reads) and branch offsets
// (an inline constant of another length shifts them). Programs instantiated from one template with
// other values (TMPL_ vars, per-user escrows) share it. It says nothing about the values or branch
// targets, so it never makes a lsig genuine: anyone can fill a template with their own.
func template(logic []byte) string {
	ver, pc := binary.Uvarint(logic)
	// ponytail: immBytes knows versions up to 13 (consensus v42); v14 is still vFuture, where
	// falcon_verify gains a field and rsa_verify is added. A newer version is its own template until
	// added. Any change here changes stored ids, so it needs a LEDGER_DB rebuild.
	skel, ok := []byte("S"), pc > 0 && ver <= 13
	if ok {
		skel = append(skel, logic[:pc]...)
	}
	// uvarint reads a varuint at pc; ok goes false if there is none.
	uvarint := func() uint64 {
		v, n := binary.Uvarint(logic[pc:])
		if n <= 0 {
			ok = false
			return 0
		}
		pc += n
		return v
	}
	// skip moves pc past n bytes, if there are that many.
	skip := func(n uint64) {
		if n > uint64(len(logic)-pc) {
			ok = false
			return
		}
		pc += int(n)
	}
	// value skips a varuint, or a (length, bytes), and returns the push op that loads it.
	value := func(isBytes bool) byte {
		l := uvarint()
		if !isBytes {
			return 0x81
		}
		if ok {
			skip(l)
		}
		return 0x80
	}
	for ok && pc < len(logic) {
		op := logic[pc]
		skel = append(skel, op)
		pc++
		// Constant blocks are left out, every int load is kept as pushint and every bytes load as
		// pushbytes: assemblers merge equal values and order a block by use count (goal), or combine
		// single-use pushes (Puya), so instances of one template differ in their blocks, in intc
		// indexes, and in intc vs pushint (seen on mainnet). That covers goal's autosalt too.
		switch op {
		case 0x20, 0x26, 0x83, 0x82: // intcblock, bytecblock, pushints, pushbytess: count, then values
			skel = skel[:len(skel)-1] // a block is left out, and a multi-push kept as a push per value
			for i, n := uint64(0), uvarint(); ok && i < n; i++ {
				if push := value(op == 0x26 || op == 0x82); op == 0x83 || op == 0x82 {
					skel = append(skel, push)
				}
			}
		case 0x80, 0x81: // pushbytes, pushint
			value(op == 0x80)
		case 0x21, 0x22, 0x23, 0x24, 0x25: // intc, intc_0..3
			skel[len(skel)-1] = 0x81
			skip(uint64(immBytes[op]))
		case 0x27, 0x28, 0x29, 0x2a, 0x2b: // bytec, bytec_0..3
			skel[len(skel)-1] = 0x80
			skip(uint64(immBytes[op]))
		case 0x40, 0x41, 0x42, 0x88: // bnz, bz, b, callsub: int16 offset, a zigzag varint from v13
			if ver < 13 {
				skip(2)
			} else if _, n := binary.Varint(logic[pc:]); n > 0 {
				pc += n
			} else {
				ok = false
			}
		case 0x8d, 0x8e: // switch, match: a 1-byte count (not a varuint, despite the langspec), then int16s
			if skip(1); ok {
				skel = append(skel, logic[pc-1])
				skip(2 * uint64(logic[pc-1]))
			}
		default:
			start := pc
			if skip(uint64(immBytes[op])); ok {
				skel = append(skel, logic[start:pc]...)
			}
		}
	}
	if !ok { // the program is its own template; the prefix keeps it from equalling a skeleton
		skel = append([]byte("P"), logic...)
	}
	h := sha512.Sum512_256(skel)
	return hex.EncodeToString(h[:8])
}
