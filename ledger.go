package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	"github.com/algorand/go-algorand-sdk/v2/types"
	_ "github.com/mattn/go-sqlite3"
)

// ledgerSource reads an archival algod's ledger.block.sqlite directly, for a bulk load
// far faster than REST. algod keeps it in WAL mode, so a read-only reader can run beside
// a live algod: it never blocks algod's writes and never writes the DB or its WAL.
// hdrdata/blkdata are the same msgpack /v2/blocks serves, decoded as leniently as the SDK does.
// A round missing from the file (a non-archival node) fails its fetch.
func ledgerSource(path, genesisID string, concurrency int) (source, *sql.DB, error) {
	// Escaped: a '?' or '#' in the path would otherwise cut the filename and drop mode=ro.
	ldb, err := sql.Open("sqlite3", "file:"+url.PathEscape(path)+"?mode=ro")
	if err != nil {
		return source{}, nil, err
	}
	// Idle conns too: the default 2 would make the readers reopen sqlite connections constantly.
	ldb.SetMaxOpenConns(concurrency)
	ldb.SetMaxIdleConns(concurrency)
	src := source{
		get: func(ctx context.Context, rnd uint64, headerOnly bool) (b types.Block, err error) {
			col, dst := "blkdata", any(&b)
			if headerOnly {
				col, dst = "hdrdata", &b.BlockHeader
			}
			var buf []byte
			if err = ldb.QueryRowContext(ctx, "SELECT "+col+" FROM blocks WHERE rnd = ?", rnd).Scan(&buf); err != nil {
				return b, fmt.Errorf("ledger round %d: %w", rnd, err)
			}
			if err = msgpack.NewLenientDecoder(bytes.NewReader(buf)).Decode(dst); err != nil {
				return b, fmt.Errorf("ledger round %d: %w", rnd, err)
			}
			if b.GenesisID != genesisID {
				err = fmt.Errorf("round %d: %s is on %q, want %q", rnd, path, b.GenesisID, genesisID)
			}
			return b, err
		},
		// The tip as of now: the import stops there, and algod ingest picks up from it.
		tip: func(ctx context.Context, _ uint64) (tip uint64, err error) {
			err = ldb.QueryRowContext(ctx, "SELECT max(rnd) FROM blocks").Scan(&tip)
			return tip, err
		},
	}
	return src, ldb, nil
}
