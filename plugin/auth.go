package plugin

import (
	"crypto/subtle"
	"net/http"
)

// authMiddleware wraps an http.Handler and requires a Bearer token if configured.
// If token is empty, all requests are allowed (no auth) — but the write pool is
// then not opened either (see Handler.Configure), so a mutating endpoint never
// runs unauthenticated.
func authMiddleware(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	expected := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Constant-time compare: this token now also guards a write endpoint, so
		// don't leak it byte-by-byte through comparison timing.
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), expected) != 1 {
			writeErr(w, 401, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}
