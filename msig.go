package main

import (
	"fmt"
	"log/slog"

	"github.com/algorand/go-algorand-sdk/v2/crypto"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

// msigUse is one subsigner key of a multisig used by a block's top-level txn.
type msigUse struct {
	signer, msig string
	signed       bool
	pos          uint32 // index in the block's payset
}

// msigUses returns every subsigner of each multisig that authorized one of the block's top-level txns
// (inner txns carry no signatures): the txn's own msig, or one that signed its delegated lsig. The
// multisig address comes from the preimage, as a rekeyed sender's authorizer is the multisig. A
// duplicate pk (allowed on chain) yields a use per subsig here, folded into one row by mergeMsigs.
// A preimage this build can't read (a new msig version, a key that isn't 32 bytes) is skipped with a
// warning, leaving a gap rather than stalling ingest.
// An old-style Lsig.Msig (until v41) signs only the program, not bound to the multisig, so its sig can
// be copied into any multisig holding the key: there signed means the key signed the program.
func msigUses(b types.Block) (uses []msigUse) {
	for i, t := range b.Payset {
		ms := []types.MultisigSig{t.Msig}
		// Program-less lsig content was accepted unchecked before v42: not a delegation.
		if len(t.Lsig.Logic) > 0 {
			ms = append(ms, t.Lsig.Msig, t.Lsig.LMsig)
		}
		for _, m := range ms {
			if m.Blank() {
				continue
			}
			addr, err := msigAddress(m)
			if err != nil {
				slog.Warn("msig skipped", "round", uint64(b.Round), "txn", i, "err", err)
				continue
			}
			for _, s := range m.Subsigs {
				var pk types.Address
				copy(pk[:], s.Key)
				uses = append(uses, msigUse{signer: pk.String(), msig: addr.String(), signed: s.Sig != (types.Signature{}), pos: uint32(i)})
			}
		}
	}
	return uses
}

// msigAddress is m's multisig address, Hash("MultisigAddr" || v || thr || pk1 || pk2 …).
func msigAddress(m types.MultisigSig) (types.Address, error) {
	for _, s := range m.Subsigs { // the SDK hashes keys of any length into the address
		if len(s.Key) != len(types.Address{}) {
			return types.Address{}, fmt.Errorf("a %d-byte pk", len(s.Key))
		}
	}
	ma, err := crypto.MultisigAccountFromSig(m)
	if err != nil {
		return types.Address{}, err
	}
	return ma.Address()
}
