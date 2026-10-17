package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminRoutes(t *testing.T) {
	defer func(c config, tok string) { track, adminToken = c, tok }(track, adminToken)
	track, adminToken = config{Lsigs: true}, "0123456789abcdef"
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	A, B := addrOf(1), addrOf(2)
	const t1, t2 = "00000000000000a1", "00000000000000a2"
	b := row(1, 1, 0, 0)
	b.lsigs = []lsigUse{{A, "", "", t1, "AQ==", 1}, {B, "", "", t1, "", 1}}
	if err := insertBatch(context.Background(), db, []block{b}); err != nil {
		t.Fatal(err)
	}
	h := routes(db)
	do := func(method, url, auth, body string) (int, string) {
		t.Helper()
		r := httptest.NewRequest(method, url, strings.NewReader(body))
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code, strings.TrimSpace(w.Body.String())
	}
	const ok, bearer = 200, "Bearer 0123456789abcdef"
	for _, c := range []struct {
		method, url, auth, body string
		code                    int
		want                    string
	}{
		{"PUT", "/v0/admin/template/" + t1, "", `{"name":"x"}`, 401, ""},
		{"PUT", "/v0/admin/template/" + t1, "Bearer wrong", `{"name":"x"}`, 401, ""},
		{"PUT", "/v0/admin/template/" + t1, "0123456789abcdef", `{"name":"x"}`, 401, ""},
		{"DELETE", "/v0/admin/template/" + t1, "", "", 401, ""},
		{"PUT", "/v0/admin/template/" + t2, bearer, `{"name":"x"}`, 404, ""}, // never seen
		{"PUT", "/v0/admin/template/A1", bearer, `{"name":"x"}`, 400, ""},
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"name":"  "}`, 400, ""},
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"name":"a\nb"}`, 400, ""},
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"name":"` + strings.Repeat("é", 101) + `"}`, 400, ""},
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"name":"a\u202eb"}`, 400, ""}, // bidi override
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"name":"a\u200bb"}`, 400, ""}, // zero-width space
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"name":"a\u2028b"}`, 400, ""}, // line separator
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"name":"y"} junk`, 400, ""},
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"name":"` + strings.Repeat("x", 5000) + `"}`, 413, ""},
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"nom":"x"}`, 400, ""},
		{"PUT", "/v0/admin/template/" + t1, bearer, `nope`, 400, ""},
		{"GET", "/v0/template/" + t1 + "?limit=1", "", "", ok, `{"template":"` + t1 + `","name":null,`},
		{"GET", "/v0/templates", "", "", ok, `[{"template":"` + t1 + `","name":null,`}, // cached: a rename must clear it
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"name":" Escrow v1 "}`, ok, `{"name":"Escrow v1","template":"` + t1 + `"}`},
		{"GET", "/v0/template/" + t1 + "?limit=1", "", "", ok, `{"template":"` + t1 + `","name":"Escrow v1",`},
		{"PUT", "/v0/admin/template/" + t1, bearer, `{"name":"Escrow v2"}`, ok, `{"name":"Escrow v2","template":"` + t1 + `"}`},
		{"GET", "/v0/lsig/" + A, "", "", ok, `{"template":"` + t1 + `","name":"Escrow v2",`},
		{"GET", "/v0/templates", "", "", ok, `[{"template":"` + t1 + `","name":"Escrow v2",`},
		{"DELETE", "/v0/admin/template/" + t1, bearer, "", 204, ""},
		{"DELETE", "/v0/admin/template/" + t1, bearer, "", 204, ""},
		{"GET", "/v0/template/" + t1 + "?limit=1", "", "", ok, `{"template":"` + t1 + `","name":null,`},
	} {
		code, body := do(c.method, c.url, c.auth, c.body)
		if code != c.code || !strings.HasPrefix(body, c.want) {
			t.Errorf("%s %s (%q) = %d %s, want %d %s…", c.method, c.url, c.auth, code, body, c.code, c.want)
		}
	}

	// No token, no admin routes.
	adminToken = ""
	h = routes(db)
	if code, _ := do("PUT", "/v0/admin/template/"+t1, "Bearer ", `{"name":"x"}`); code != 404 {
		t.Errorf("admin route without ADMIN_TOKEN = %d, want 404", code)
	}
}

func TestCheckAdminToken(t *testing.T) {
	for _, c := range []struct {
		tok   string
		lsigs bool
		ok    bool
	}{
		{"", false, true},
		{"0123456789abcdef", true, true},
		{"0123456789abcdef", false, false}, // routes need LSIGS
		{"0123456789abcde", true, false},   // short
		{"0123456789abcdef ", true, false}, // trimmed from the header
		{"01234567 89abcdef", true, false},
		{"0123456789abcdeé", true, false},
	} {
		if err := checkAdminToken(c.tok, c.lsigs); (err == nil) != c.ok {
			t.Errorf("checkAdminToken(%q, %t) = %v, want ok %t", c.tok, c.lsigs, err, c.ok)
		}
	}
}
