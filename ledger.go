package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	_ "github.com/mattn/go-sqlite3"
)

// ledgerSource reads an archival algod's ledger.block.sqlite directly, for a bulk load
// far faster than REST. algod keeps it in WAL mode, so a read-only reader can run beside
// a live algod: it never blocks algod's writes and never writes the DB or its WAL.
// hdrdata/blkdata/certdata are the same msgpack /v2/blocks serves, decoded as leniently as the SDK does.
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
		get: func(ctx context.Context, rnd uint64, headerOnly, cert bool) (b blockCert, err error) {
			col, dst, certCol := "blkdata", any(&b.Block), "NULL"
			if headerOnly {
				col, dst = "hdrdata", &b.Block.BlockHeader
			}
			// ponytail: with proposers on, pre-incentive rounds read the whole certdata (~60 KB/round,
			// ~2.8 TB over mainnet's 46.5M), so the rebuild is I/O-bound. Throttle with CONCURRENCY.
			if cert {
				certCol = "certdata"
			}
			var buf, cbuf []byte
			if err = ldb.QueryRowContext(ctx, "SELECT "+col+", "+certCol+" FROM blocks WHERE rnd = ?", rnd).Scan(&buf, &cbuf); err != nil {
				return b, fmt.Errorf("ledger round %d: %w", rnd, err)
			}
			err = msgpack.NewLenientDecoder(bytes.NewReader(buf)).Decode(dst)
			if err == nil && len(cbuf) > 0 { // a bare agreement.Certificate: decoding it into &b would yield nothing
				err = msgpack.NewLenientDecoder(bytes.NewReader(cbuf)).Decode(&b.Cert)
			}
			if err != nil {
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
