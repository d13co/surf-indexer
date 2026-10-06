package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

func envStr(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		log.Fatalf("%s: expected a positive integer, got %q", name, v)
	}
	return n
}

// algodClients builds one client per ';'-separated ALGOD_HOST/PORT/TOKEN entry.
// A single entry is shared by all algods.
func algodClients() (algods, error) {
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
	var a algods
	for i := range n {
		host, port, token := pick(parts[0], i), pick(parts[1], i), pick(parts[2], i)
		c, err := algod.MakeClient(host+":"+port, token)
		if err != nil {
			return nil, err
		}
		log.Printf("algod %d: %s:%s token=%t", i, host, port, token != "")
		a = append(a, c)
	}
	return a, nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: fees-api <genesisID> [dbID]")
	}
	genesisID, dbID := os.Args[1], os.Args[1]
	if len(os.Args) > 2 {
		dbID = os.Args[2]
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := algodClients()
	if err != nil {
		log.Fatal(err)
	}
	params, err := call(ctx, a, func(c *algod.Client) (types.SuggestedParams, error) { return c.SuggestedParams().Do(ctx) })
	if err != nil {
		log.Fatal(err)
	}
	if params.GenesisID != genesisID {
		log.Fatalf("genesis ID mismatch, expected %s found %s", genesisID, params.GenesisID)
	}

	if err := os.MkdirAll("data", 0o755); err != nil {
		log.Fatal(err)
	}
	// DuckDB's file lock also stops a second instance from opening the same DB.
	db, err := openDB("data/" + dbID + ".duckdb")
	if err != nil {
		log.Fatal(err)
	}
	if n, lo, hi, err := rounds(ctx, db); err != nil || lo != 0 || hi+1 != n {
		db.Close()
		log.Fatalf("db not contiguous from round 0: records=%d minRound=%d maxRound=%d err=%v", n, lo, hi, err)
	}

	done := make(chan struct{})
	go func() {
		ingest(ctx, db, a, envInt("CONCURRENCY", 10), envInt("DB_CHUNKS", 1000))
		close(done)
	}()

	srv := &http.Server{Addr: ":" + envStr("PORT", "8118"), Handler: routes(db)}
	go func() {
		log.Printf("listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Print(err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Print("shutting down")
	srv.Shutdown(context.Background())
	<-done
	if err := db.Close(); err != nil {
		log.Print(err)
	}
	log.Print("db closed")
}
