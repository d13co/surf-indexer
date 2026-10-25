package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

// version is set by the release build.
var version = "dev"

func envStr(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

// fatal logs at ERROR and exits, like log.Fatal.
func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

// algodClients builds one client per ';'-separated ALGOD_HOST/PORT/TOKEN entry.
// A single entry is shared by all algods.
func algodClients(concurrency int) (algods, error) {
	parts := [][]string{
		strings.Split(envStr("ALGOD_HOST", "http://localhost"), ";"),
		strings.Split(envStr("ALGOD_PORT", "8080"), ";"),
		strings.Split(os.Getenv("ALGOD_TOKEN"), ";"),
	}
	n := max(len(parts[0]), len(parts[1]), len(parts[2]))
	for _, p := range parts {
		if len(p) != 1 && len(p) != n {
			return nil, fmt.Errorf("ALGOD_HOST/ALGOD_PORT/ALGOD_TOKEN: expected 1 or %d ';'-separated entries", n)
		}
	}
	pick := func(p []string, i int) string { return p[min(i, len(p)-1)] }
	// The default transport has no response timeout (a stuck algod would stall ingest forever)
	// and keeps only 2 idle conns per host (concurrent fetchers would churn connections).
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = 90 * time.Second // > algod's 60s wait-for-block
	tr.MaxIdleConnsPerHost = concurrency
	var a algods
	for i := range n {
		host, port, token := pick(parts[0], i), pick(parts[1], i), pick(parts[2], i)
		c, err := algod.MakeClientWithTransport(host+":"+port, token, nil, tr)
		if err != nil {
			return nil, err
		}
		safe := host // a basic-auth password in the URL must not reach the log
		if u, err := url.Parse(host); err == nil {
			safe = u.Redacted()
		}
		slog.Info("algod", "i", i, "host", safe, "port", port, "token", token != "")
		a = append(a, c)
	}
	return a, nil
}

// startRound is where ingest into an empty DB begins: 0 but for trials (START_ROUND), whose DB is partial.
var startRound uint64

// openData opens data/<dbID>.duckdb, refusing one that isn't contiguous from startRound.
func openData(ctx context.Context, dbID, genesisID string) *sql.DB {
	if err := os.MkdirAll("data", 0o755); err != nil {
		fatal("data dir", "err", err)
	}
	// DuckDB's file lock also stops a second instance from opening the same DB.
	db, err := openDB("data/"+dbID+".duckdb", genesisID)
	if errors.As(err, new(configErr)) {
		fatalConfig("open db", "err", err)
	} else if err != nil {
		fatal("open db", "err", err)
	}
	if n, lo, hi, err := rounds(ctx, db); err != nil {
		db.Close()
		fatal("db rounds", "err", err)
	} else if n > 0 && (lo != int64(startRound) || hi+1-lo != n) {
		db.Close()
		fatalConfig("db not contiguous from START_ROUND", "startRound", startRound, "records", n, "minRound", lo, "maxRound", hi)
	}
	return db
}

// importLedger bulk-loads from an archival ledger.block.sqlite up to its current tip, then returns.
func importLedger(ctx context.Context, path, genesisID, dbID string, concurrency, chunk int) {
	src, ldb, err := ledgerSource(path, genesisID, concurrency)
	if err != nil {
		fatal("ledger source", "err", err)
	}
	defer ldb.Close()
	db := openData(ctx, dbID, genesisID)
	hi, err := syncToTip(ctx, db, src, concurrency, uint64(chunk))
	if err == nil {
		err = syncUpgrades(ctx, db, src, concurrency, hi)
	}
	if cerr := db.Close(); err == nil { // checkpoints, so the .duckdb file alone is complete
		err = cerr
	}
	if err != nil {
		fatal("ledger import", "err", err)
	}
	slog.Info("ledger import done, db closed")
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil))) // also routes any log package output
	if len(os.Args) < 2 {
		fatal("usage: surf-indexer <genesisID> [dbID] | check [-env-file FILE] [-offline] <genesisID> | version")
	}
	switch os.Args[1] {
	case "version":
		fmt.Println("surf-indexer", version)
		return
	case "check":
		os.Exit(check(os.Args[2:]))
	}
	genesisID, dbID := os.Args[1], os.Args[1]
	if len(os.Args) > 2 {
		dbID = os.Args[2]
	}

	// Before either path: importLedger opens the DB and ingests with it too.
	s, err := loadConfig()
	if err != nil {
		fatalConfig("config", "err", err)
	}
	track, adminToken, limits, startRound, dbOptions = s.track, s.adminToken, s.limits, s.startRound, s.dbOptions

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	concurrency, chunk := s.concurrency, s.chunk
	slog.Info("config", "version", version, "genesisID", genesisID, "dbID", dbID, "concurrency", concurrency, "chunk", chunk, "ledgerDB", s.ledger, "listen", s.listen,
		"proposers", track.Proposers, "voterRounds", track.VoterRounds, "lsigs", track.Lsigs, "msigs", track.Msigs, "startRound", startRound, "admin", adminToken != "",
		"rateLimit", limits.Rate, "heavyConcurrency", limits.Heavy, "realIPHeader", limits.RealIPHeader, "duckdb", dbOptions.Encode())
	if s.ledger != "" {
		importLedger(ctx, s.ledger, genesisID, dbID, concurrency, chunk)
		return
	}

	a, err := algodClients(concurrency)
	if err != nil {
		fatalConfig("algod config", "err", err)
	}
	params, err := call(ctx, a, func(c *algod.Client) (types.SuggestedParams, error) { return c.SuggestedParams().Do(ctx) })
	if err != nil {
		fatal("algod", "err", err)
	}
	if params.GenesisID != genesisID {
		fatalConfig("genesis ID mismatch", "want", genesisID, "got", params.GenesisID)
	}

	// Listen before opening the DB so a busy port fails fast with a non-zero exit.
	ln, err := net.Listen("tcp", s.listen)
	if err != nil {
		fatal("listen", "err", err)
	}

	db := openData(ctx, dbID, genesisID)

	done := make(chan struct{})
	go func() {
		ingest(ctx, db, algodSource(a, genesisID), concurrency, chunk)
		close(done)
	}()

	// ErrorLog: net/http's own errors (e.g. handler panics) at ERROR, not the rerouted log package's INFO.
	// WriteTimeout bounds a request from its headers to its last byte, so it must exceed a heavy
	// request's slot wait plus query time (40 s), with room to send a large reply.
	srv := &http.Server{Handler: routes(db), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute,
		ErrorLog: slog.NewLogLogger(slog.Default().Handler(), slog.LevelError)}
	go func() {
		slog.Info("listening", "addr", ln.Addr().String())
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("serve", "err", err)
			stop()
		}
	}()

	sdNotify("READY=1")

	<-ctx.Done()
	slog.Info("shutting down")
	sdNotify("STOPPING=1")
	srv.Shutdown(context.Background())
	<-done
	if err := db.Close(); err != nil {
		slog.Error("db close", "err", err)
	}
	slog.Info("db closed")
}
