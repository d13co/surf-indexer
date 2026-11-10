package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
)

// clearEnv unsets every known variable for the test, as an env file alone would set them.
func clearEnv(t *testing.T) {
	for _, k := range knownEnv {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
}

func setEnv(t *testing.T, env map[string]string) {
	for k, v := range env {
		t.Setenv(k, v)
	}
}

// The shipped example loads as is and with every commented option uncommented, and lists
// every known variable.
func TestExampleEnv(t *testing.T) {
	clearEnv(t)
	env, err := readEnvFile("deploy/example.env")
	if err != nil {
		t.Fatal(err)
	}
	setEnv(t, env)
	s, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if s.listen != "127.0.0.1:8118" || s.dbOptions.Get("memory_limit") != "2GB" || s.dbOptions.Get("threads") != "4" {
		t.Errorf("listen %q, dbOptions %v", s.listen, s.dbOptions)
	}

	b, _ := os.ReadFile("deploy/example.env")
	all := regexp.MustCompile(`(?m)^#([A-Z_]+=)`).ReplaceAllString(string(b), "$1")
	path := filepath.Join(t.TempDir(), "all.env")
	os.WriteFile(path, []byte(all), 0o600)
	clearEnv(t)
	if env, err = readEnvFile(path); err != nil {
		t.Fatal(err)
	}
	setEnv(t, env)
	if _, err := loadConfig(); err != nil {
		t.Errorf("every option uncommented: %v", err)
	}
	for _, k := range knownEnv {
		if _, ok := env[k]; !ok {
			t.Errorf("%s missing from deploy/example.env", k)
		}
	}
}

func TestReadmeListsEnv(t *testing.T) {
	b, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range knownEnv {
		if !regexp.MustCompile(`(?m)^\| ` + k + ` +\|`).Match(b) {
			t.Errorf("%s missing from the README's Environment Variables table", k)
		}
	}
}

// knownEnv is exactly what the sources read, so a new variable can't skip the docs.
func TestSourcesReadKnownEnv(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	read := regexp.MustCompile(`\b(?:envStr|int|bool|Getenv|LookupEnv)\("([A-Z][A-Z0-9_]*)"`)
	found := map[string]bool{"DB_ID": true} // read by the unit, not the binary
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, _ := os.ReadFile(f)
		for _, m := range read.FindAllSubmatch(b, -1) {
			found[string(m[1])] = true
		}
	}
	delete(found, "NOTIFY_SOCKET") // systemd's, not config
	for k := range found {
		if !slices.Contains(knownEnv, k) {
			t.Errorf("%s is read but not in knownEnv", k)
		}
	}
	for _, k := range knownEnv {
		if !found[k] {
			t.Errorf("%s is in knownEnv but never read", k)
		}
	}
}

func TestReadEnvFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.env")
	os.WriteFile(path, []byte("# comment\n; comment\n\n  PORT = 9000 \nALGOD_TOKEN=\"a b\"\nREAL_IP_HEADER='X-Real-IP'\nLSIGS=\"1'\n"), 0o600)
	env, err := readEnvFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"PORT": "9000", "ALGOD_TOKEN": "a b", "REAL_IP_HEADER": "X-Real-IP", "LSIGS": `"1'`}
	if len(env) != len(want) {
		t.Errorf("got %v", env)
	}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("%s: got %q, want %q", k, env[k], v)
		}
	}

	os.WriteFile(path, []byte("LSIG=1\nexport PORT=1\nPORT\n"), 0o600)
	_, err = readEnvFile(path)
	for _, s := range []string{":1: unknown variable LSIG", ":2: expected NAME=value", ":3: expected NAME=value"} {
		if err == nil || !strings.Contains(err.Error(), s) {
			t.Errorf("want %q in %v", s, err)
		}
	}
}

// loadConfig reports every bad setting at once.
func TestLoadConfigErrors(t *testing.T) {
	clearEnv(t)
	setEnv(t, map[string]string{"PORT": "70000", "LSIGS": "maybe", "VOTER_ROUNDS": "-1", "ADMIN_TOKEN": "short", "DUCKDB_MEMORY_LIMIT": "lots",
		"REKEYS": "maybe", "BACKFILL_CONCURRENCY": "0", "BACKFILL_RATE": "-1"})
	_, err := loadConfig()
	for _, s := range []string{"PORT", "LSIGS", "VOTER_ROUNDS", "ADMIN_TOKEN", "DUCKDB_MEMORY_LIMIT", "REKEYS", "BACKFILL_CONCURRENCY", "BACKFILL_RATE"} {
		if err == nil || !strings.Contains(err.Error(), s) {
			t.Errorf("want %s in %v", s, err)
		}
	}

	clearEnv(t)
	setEnv(t, map[string]string{"LISTEN_ADDR": "[::1]", "PORT": "9"})
	if s, err := loadConfig(); err != nil || s.listen != "[::1]:9" || len(s.dbOptions) != 0 || s.backfill != (backfillConfig{concurrency: 4, rate: 100}) {
		t.Errorf("got %q %v %+v, %v", s.listen, s.dbOptions, s.backfill, err)
	}
}

// Backfill algods on other hosts don't get the ALGOD_* token (or port).
func TestBackfillAlgodToken(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("X-Algo-API-Token"))
		http.Error(w, "no", http.StatusTeapot)
	}))
	defer srv.Close()
	host, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	for _, c := range []struct {
		env  map[string]string
		want string
	}{
		{map[string]string{"ALGOD_HOST": "http://" + host, "ALGOD_PORT": port, "ALGOD_TOKEN": "tip"}, "tip"}, // the ALGOD_* algods
		{map[string]string{"ALGOD_TOKEN": "tip", "BACKFILL_ALGOD_HOST": "http://" + host, "BACKFILL_ALGOD_PORT": port}, ""},
		{map[string]string{"ALGOD_TOKEN": "tip", "BACKFILL_ALGOD_HOST": "http://" + host, "BACKFILL_ALGOD_PORT": port, "BACKFILL_ALGOD_TOKEN": "bf"}, "bf"},
	} {
		clearEnv(t)
		setEnv(t, c.env)
		a, err := algodClients(true, 1)
		if err != nil {
			t.Fatal(err)
		}
		got.Store("none")
		a[0].Status().Do(context.Background())
		if g := got.Load(); g != c.want {
			t.Errorf("%v: token %q, want %q", c.env, g, c.want)
		}
	}
}

// Backfill algods take each ALGOD_* value they don't set, and keep their own transport.
func TestBackfillAlgods(t *testing.T) {
	clearEnv(t)
	setEnv(t, map[string]string{"ALGOD_HOST": "http://a;http://b", "ALGOD_PORT": "1", "ALGOD_TOKEN": "t", "BACKFILL_ALGOD_PORT": "2"})
	live, err := algodClients(false, 3)
	if err != nil || len(live) != 2 {
		t.Fatal(live, err)
	}
	bf, err := algodClients(true, 3)
	if err != nil || len(bf) != 2 {
		t.Fatal(bf, err)
	}
	setEnv(t, map[string]string{"BACKFILL_ALGOD_HOST": "http://c;http://d;http://e", "BACKFILL_ALGOD_PORT": "2;3"})
	if _, err := algodClients(true, 3); err == nil || !strings.Contains(err.Error(), "BACKFILL_ALGOD_HOST") {
		t.Errorf("3 hosts, 2 ports = %v, want a BACKFILL_ALGOD_* error", err)
	}
	if _, err := algodClients(false, 3); err != nil {
		t.Errorf("live algods with 3 backfill hosts: %v", err)
	}
}

func TestSdNotify(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify")
	c, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	t.Setenv("NOTIFY_SOCKET", path)
	sdNotify("READY=1")
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	if err != nil || string(buf[:n]) != "READY=1" {
		t.Errorf("got %q, %v", buf[:n], err)
	}
}
