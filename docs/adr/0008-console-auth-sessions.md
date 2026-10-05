---
status: accepted
date: 2026-10-05
---

# 0008: Console authentication uses server-side sessions

## Context and Problem Statement

The plan specified JWT access tokens plus refresh tokens. For a same-origin SPA, that means storing tokens in the browser (XSS exposure), handling rotation and revocation, and coping with WebSocket sessions that outlive 15-minute tokens and role changes (review finding 28).

## Decision Outcome

- **FastAPI serves the SPA, and the console authenticates with an opaque session cookie:** HttpOnly, Secure, SameSite=Strict, backed by a server-side session table.
- **Mutations require a custom header** as CSRF defence in depth.
- **The WebSocket ticket is issued from the session.** The server closes the socket when the session expires or the user's role changes, and the client resyncs.
- **Roles** (viewer, operator, admin) and the authorisation matrix test are unchanged.
- **JWT** is kept only for future machine clients.

### Consequences

- Good: no tokens in JavaScript; immediate revocation.
- Bad: sessions are stateful, which is acceptable for a single API instance.
