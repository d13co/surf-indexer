package main

import (
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
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
	setEnv(t, map[string]string{"PORT": "70000", "LSIGS": "maybe", "VOTER_ROUNDS": "-1", "ADMIN_TOKEN": "short", "DUCKDB_MEMORY_LIMIT": "lots"})
	_, err := loadConfig()
	for _, s := range []string{"PORT", "LSIGS", "VOTER_ROUNDS", "ADMIN_TOKEN", "DUCKDB_MEMORY_LIMIT"} {
		if err == nil || !strings.Contains(err.Error(), s) {
			t.Errorf("want %s in %v", s, err)
		}
	}

	clearEnv(t)
	setEnv(t, map[string]string{"LISTEN_ADDR": "[::1]", "PORT": "9"})
	if s, err := loadConfig(); err != nil || s.listen != "[::1]:9" || len(s.dbOptions) != 0 {
		t.Errorf("got %q %v, %v", s.listen, s.dbOptions, err)
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
