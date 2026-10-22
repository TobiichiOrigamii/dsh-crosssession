# Changelog

## 0.1.0

Initial public release.

- Cross-session / cross-workspace messaging with auto-wake and next-turn queueing
- Persistent session team: create, alias, roster, team status
- Task board with queued / running / blocked / done / failed / cancelled / awaiting-approval states
- Serial per-worker queue with per-worker dispatch lock
- Approval gate for sensitive workspaces (`policy.json`, hot reload)
- Session-scoped task board in the DSH native right sidebar (`sessionHubBoard` tab)
- `@` mention source for team members (editable chip pattern)
- Quiet notification format, `deliverReports: board` + `session_inbox` background reading
- Security hardening: loopback-only board with CSP, export path confinement, corrupt-state backup, input limits
