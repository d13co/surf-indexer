package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// exitConfig (EX_CONFIG) is the exit code for errors a restart can't fix: the config or the DB
// must change first. The systemd unit doesn't restart on it.
const exitConfig = 78

// fatalConfig logs at ERROR and exits with exitConfig.
func fatalConfig(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(exitConfig)
}

// configErr marks an error as one a restart can't fix (see exitConfig).
type configErr struct{ error }

func (e configErr) Unwrap() error { return e.error }

// knownEnv is every variable the binary or its systemd unit reads. check rejects any other in an
// env file, where a typo would otherwise be silently ignored. Tests keep deploy/example.env and
// the README's table listing all of them.
var knownEnv = []string{
	"PORT", "LISTEN_ADDR", "ALGOD_HOST", "ALGOD_PORT", "ALGOD_TOKEN", "CONCURRENCY", "DB_CHUNKS",
	"LEDGER_DB", "PROPOSERS", "VOTER_ROUNDS", "LSIGS", "MSIGS", "ADMIN_TOKEN", "RATE_LIMIT",
	"HEAVY_CONCURRENCY", "REAL_IP_HEADER", "START_ROUND", "DUCKDB_MEMORY_LIMIT", "DUCKDB_THREADS",
	"DB_ID", // read by the unit only, which passes it as the dbID argument
}

// settings is the startup config from the environment, but for the algods (algodClients).
type settings struct {
	track              config
	adminToken         string
	limits             limitConfig
	startRound         uint64
	concurrency, chunk int
	ledger             string
	listen             string     // host:port
	dbOptions          url.Values // DuckDB config options
}

// envReader reads typed variables, collecting every error rather than stopping at the first.
type envReader struct{ errs []error }

func (r *envReader) int(name string, def, min, max int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max {
		r.errs = append(r.errs, fmt.Errorf("%s: expected an integer from %d to %d, got %q", name, min, max, v))
		return def
	}
	return n
}

func (r *envReader) bool(name string, def bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: expected a boolean, got %q", name, v))
		return def
	}
	return b
}

// loadConfig reads and checks every setting but the algods, returning all errors at once.
func loadConfig() (s settings, err error) {
	var r envReader
	s.track = config{Proposers: r.bool("PROPOSERS", true), VoterRounds: uint64(r.int("VOTER_ROUNDS", 100000, 0, math.MaxUint32)),
		Lsigs: r.bool("LSIGS", false), Msigs: r.bool("MSIGS", false)}
	s.adminToken = os.Getenv("ADMIN_TOKEN")
	if err := checkAdminToken(s.adminToken, s.track.Lsigs); err != nil {
		r.errs = append(r.errs, fmt.Errorf("ADMIN_TOKEN: %w", err))
	}
	s.limits = limitConfig{Rate: r.int("RATE_LIMIT", 10, 0, math.MaxInt), Heavy: r.int("HEAVY_CONCURRENCY", 4, 0, math.MaxInt),
		RealIPHeader: os.Getenv("REAL_IP_HEADER")}
	s.startRound = uint64(r.int("START_ROUND", 0, 0, math.MaxUint32))
	s.concurrency, s.chunk = r.int("CONCURRENCY", 10, 1, math.MaxInt), r.int("DB_CHUNKS", 1000, 1, math.MaxInt)
	s.ledger = os.Getenv("LEDGER_DB")
	host := strings.TrimSuffix(strings.TrimPrefix(os.Getenv("LISTEN_ADDR"), "["), "]") // JoinHostPort adds an IPv6's brackets
	s.listen = net.JoinHostPort(host, strconv.Itoa(r.int("PORT", 8118, 1, 65535)))

	s.dbOptions = url.Values{}
	if v := os.Getenv("DUCKDB_MEMORY_LIMIT"); v != "" {
		s.dbOptions.Set("memory_limit", v)
	}
	if n := r.int("DUCKDB_THREADS", 0, 0, math.MaxInt); n > 0 {
		s.dbOptions.Set("threads", strconv.Itoa(n))
	}
	if len(s.dbOptions) > 0 { // DuckDB checks them: try them on an in-memory DB
		db, err := sql.Open("duckdb", "?"+s.dbOptions.Encode())
		if err == nil {
			err = db.Ping()
			db.Close()
		}
		if err != nil {
			r.errs = append(r.errs, fmt.Errorf("DUCKDB_MEMORY_LIMIT/DUCKDB_THREADS: %w", err))
		}
	}
	return s, errors.Join(r.errs...)
}

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// readEnvFile reads the subset of systemd's EnvironmentFile syntax that env files need:
// NAME=value lines, # and ; comments, and values in matching single or double quotes.
// Unknown names are errors.
func readEnvFile(path string) (map[string]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	known := map[string]bool{}
	for _, k := range knownEnv {
		known[k] = true
	}
	env := map[string]string{}
	var errs []error
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		switch {
		case !ok || !envName.MatchString(k):
			errs = append(errs, fmt.Errorf("%s:%d: expected NAME=value", path, i+1))
		case !known[k]:
			errs = append(errs, fmt.Errorf("%s:%d: unknown variable %s", path, i+1, k))
		default:
			if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
				v = v[1 : len(v)-1]
			}
			env[k] = v
		}
	}
	return env, errors.Join(errs...)
}

// check validates an instance's config, and unless offline, that every algod answers for
// genesisID. It returns the exit code: 0, exitConfig, or 1 for an algod that doesn't answer.
func check(args []string) int {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	envFile := fs.String("env-file", "", "read the config from this env file (as systemd's EnvironmentFile), not the environment")
	offline := fs.Bool("offline", false, "check only the config, not the algods")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: surf-indexer check [-env-file FILE] [-offline] <genesisID>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}
	genesisID := fs.Arg(0)
	fail := func(code int, msg string, err error) int {
		fmt.Fprintf(os.Stderr, "FAIL %s:\n  %s\n", msg, strings.ReplaceAll(err.Error(), "\n", "\n  "))
		return code
	}

	if *envFile != "" {
		env, err := readEnvFile(*envFile)
		if err != nil {
			return fail(exitConfig, "env file", err)
		}
		for _, k := range knownEnv { // the file alone, as the unit sees it
			os.Unsetenv(k)
		}
		for k, v := range env {
			os.Setenv(k, v)
		}
	}
	s, err := loadConfig()
	if err != nil {
		return fail(exitConfig, "config", err)
	}
	fmt.Println("ok   config")
	if *offline {
		return 0
	}

	a, err := algodClients(s.concurrency)
	if err != nil {
		return fail(exitConfig, "algod config", err)
	}
	code := 0
	for i, c := range a { // each on its own, so a dead failover algod shows too
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		p, err := c.SuggestedParams().Do(ctx)
		cancel()
		switch {
		case err != nil:
			code = max(code, fail(1, fmt.Sprintf("algod %d", i), err))
		case p.GenesisID != genesisID:
			code = fail(exitConfig, fmt.Sprintf("algod %d", i), fmt.Errorf("genesis ID is %s, not %s", p.GenesisID, genesisID))
		default:
			fmt.Printf("ok   algod %d: %s at round %d\n", i, p.GenesisID, p.FirstRoundValid)
		}
	}
	return code
}
