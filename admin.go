package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"
)

// adminToken guards the /v0/admin routes, which exist only when it is set (ADMIN_TOKEN).
var adminToken string

const maxNameLen = 100 // characters

// checkAdminToken refuses a token that is short, or that can't arrive intact in a header (Go trims
// a header value's spaces), and one set without LSIGS, whose routes all admin routes are.
func checkAdminToken(tok string, lsigs bool) error {
	switch {
	case tok == "":
		return nil
	case len(tok) < 16:
		return errors.New("expected at least 16 characters")
	case strings.ContainsFunc(tok, func(r rune) bool { return r <= ' ' || r > '~' }):
		return errors.New("expected printable ASCII without spaces")
	case !lsigs:
		return errors.New("its routes need LSIGS on")
	}
	return nil
}

// authorized requires "Authorization: Bearer <adminToken>". Both sides are hashed first, so the
// compare is constant-time whatever the header's length.
func authorized(h http.HandlerFunc) http.HandlerFunc {
	want := sha256.Sum256([]byte("Bearer " + adminToken))
	return func(w http.ResponseWriter, r *http.Request) {
		got := sha256.Sum256([]byte(r.Header.Get("Authorization")))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h(w, r)
	}
}

// templateName parses a {"name": …} body: trimmed, 1 to maxNameLen visible characters or spaces.
// Format, zero-width and bidi-override characters are refused: a name is shown as is to explorer users.
func templateName(w http.ResponseWriter, r *http.Request) (string, bool) {
	var body struct {
		Name string `json:"name"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	dec.DisallowUnknownFields()
	err := dec.Decode(&body)
	if err == nil && dec.Decode(&struct{}{}) != io.EOF {
		err = errors.New("trailing data")
	}
	if mbe := new(http.MaxBytesError); errors.As(err, &mbe) {
		http.Error(w, "body: too large", http.StatusRequestEntityTooLarge)
		return "", false
	}
	if err != nil {
		http.Error(w, `body: want {"name": "<name>"}: `+err.Error(), http.StatusBadRequest)
		return "", false
	}
	name := strings.TrimSpace(body.Name)
	if name == "" || utf8.RuneCountInString(name) > maxNameLen || strings.ContainsFunc(name, func(r rune) bool { return !unicode.IsGraphic(r) }) {
		http.Error(w, "name: want 1 to 100 visible characters or spaces", http.StatusBadRequest)
		return "", false
	}
	return name, true
}

func adminRoutes(mux *http.ServeMux, db *sql.DB) {
	// Names a template seen by ingest (404 otherwise, which catches a mistyped id), replacing any name.
	mux.HandleFunc("PUT /v0/admin/template/{id}", authorized(func(w http.ResponseWriter, r *http.Request) {
		id, ok := tmplID(w, r)
		if !ok {
			return
		}
		name, ok := templateName(w, r)
		if !ok {
			return
		}
		res, err := db.ExecContext(r.Context(), `INSERT INTO template_names SELECT tmpl, $2 FROM lsig_templates WHERE tmpl = $1
			ON CONFLICT (tmpl) DO UPDATE SET name = excluded.name`, id, name)
		var n int64
		if err == nil {
			n, err = res.RowsAffected()
		}
		if err == nil && n == 0 {
			http.Error(w, "no such template", http.StatusNotFound)
			return
		}
		if err == nil {
			slog.Info("template named", "template", id, "name", name, "remote", r.RemoteAddr)
		}
		reply(w, map[string]string{"template": id, "name": name}, err)
	}))

	// Removes a template's name. Idempotent: 204 whether or not it had one.
	mux.HandleFunc("DELETE /v0/admin/template/{id}", authorized(func(w http.ResponseWriter, r *http.Request) {
		id, ok := tmplID(w, r)
		if !ok {
			return
		}
		if _, err := db.ExecContext(r.Context(), "DELETE FROM template_names WHERE tmpl = $1", id); err != nil {
			reply(w, nil, err)
			return
		}
		slog.Info("template unnamed", "template", id, "remote", r.RemoteAddr)
		w.WriteHeader(http.StatusNoContent)
	}))
}
