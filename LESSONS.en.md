# Development Lessons

Evidence-backed lessons for the parent and public host contracts; see the [AI workflow](docs/en/ai-development.md). Subsequent sessions read applicable entries before work. Client, plugin, and device lessons belong in their own repositories.

## Recording and Promotion

- Include rule, scope, failure/fix evidence, successful examples, and independent event dates. Use full repository references and commit SHAs/links.
- `Seen N×` counts distinct dates of independent events, not repeated reviews. Three distinct dates make promotion candidates; mark the authoritative destination. Existing hard rules and serious failures do not wait for three occurrences.
- Never invent counts, resolution status, or validation. Synchronize the [Chinese version](LESSONS.md) and redact sensitive information.

## Entries

### FLOW-001: Proactively deliver an Issue reply draft for approval after pushing its fix

- Scope: development delivery and Issue communication in the parent and child repositories.
- Failure: on 2026-10-13, the fix for songloft-org/songloft#495, [songloft-org/songloft-plugin-miot@1cb1b52](https://github.com/songloft-org/songloft-plugin-miot/commit/1cb1b5214612e9fe611e8d8c61bfa47d5fc2f579), was pushed, v2026.10.14 was published, and the parent was updated at [540bf0c](https://github.com/songloft-org/songloft/commit/540bf0c76f87f08ba54dec1573cf9922126ad68c). Delivery omitted a prepared, visible Issue reply draft. Only after the user asked was the reply explicitly arranged, causing extra exchanges.
- Fix: the current uncommitted workflow update requires AGENTS, AI workflow, and dev-flow to proactively show the complete draft, target Issue, and specific comment approval action after pushing; existing applicable comment authorization permits direct posting. Update drafts as CI/release results change, with separate authorization for comments and closure.
- Done right: after the user explicitly requested the reply, the release update was [posted and read back](https://github.com/songloft-org/songloft/issues/495#issuecomment-6147071915), stating the available version, actual validation, and device/duration acceptance limits. Subsequent practical validation of the new “proactive draft → approval → posting” workflow remains pending; the late reply does not establish that complete flow.
- Seen 1× (2026-10-13). Status: incorporated immediately at the user's explicit request into [Linked Issue Replies](docs/en/ai-development.md#linked-issue-replies), without waiting for the occurrence threshold.

### TEST-001: Check background file writes when temporary directory cleanup fails

- Scope: Go plugin lifecycle, asynchronous bytecode caching, and test temporary directories.
- Rule: do not dismiss `TempDir RemoveAll: directory not empty` as an environment problem or hide it with a delay. Check the ordering of test completion and background disk writes. A successful rerun establishes that attempt's result, not a lifecycle-race fix.
- Failure: on 2026-10-14, the parent gitlink commit [`405e8a0`](https://github.com/songloft-org/songloft/commit/405e8a0f2697bac53c3311567fdca025256335df) for songloft-org/songloft#518 failed during `TestManager_ReloadPlugin` cleanup in the [first CI attempt](https://github.com/songloft-org/songloft/actions/runs/38526078645/attempts/1). Logs show manager closure before `bytecode cached`. This batch changed client gitlinks without changing Go source.
- Investigation and handling: `JSService.Load` compiles and writes bytecode asynchronously, and shutdown does not join that disk write. Evidence points to a race between background writes and temporary directory cleanup. At the same SHA, local `CGO_ENABLED=0 go test -tags dev,lite ./internal/jsplugin -run '^TestManager_ReloadPlugin$' -count=30` passed; rerunning failed jobs passed Quality. Logs are in `/tmp/songloft-518-{parent-ci-failed,reload-repeat}.log`. This batch does not fix that background-task lifecycle; a deterministic regression and shutdown wait remain follow-up work.
- Done right: retain the first failure, actual SHA, and rerun result. A lifecycle fix should verify that no cache writes occur after shutdown returns, then clean the temporary directory. Do not treat one successful rerun as proof of repair.
- Seen 1× (2026-10-14). Status: lesson; lifecycle repair remains pending.

### AUTH-001: Refresh token rotation must be conditionally consumed inside a transaction; keep session identity separate from display info

- Scope: authentication module in the parent repository (`internal/services/auth_service.go`, `auth_tokens` in `internal/database`).
- Rules: (1) concurrency safety cannot rely on a check-then-act `IsRevoked`→`Revoke` sequence. Inside `RunInTx`, a guarded conditional UPDATE (`token_type='refresh' AND revoked_at IS NULL AND expires_at >= ?`) must be the **first statement** that atomically consumes the token; zero rows means reuse/invalid, after which persistent state is read and classified as consumed/revoked/expired/wrong-type. (2) Old-refresh consumption and new access/refresh inserts commit in the same transaction; any failure rolls back completely, and the uncommitted old refresh must stay retryable. (3) Session identity is a random value generated at login, persisted in its own `session_id` column and stable across rotations; `client_info` such as User-Agent is display-only and must never be the session key (same-UA independent logins are different sessions; logout revokes by session). (4) Distinguish invalid credentials (401) from storage failures (500) with sentinel errors; DB errors must never map to credential errors.
- Failure: on 2026-10-14, `RefreshToken` at baseline `6b050ff` was a non-transactional IsRevoked→Get→Revoke→two Creates sequence: concurrent refreshes of the same token could each pass the check and mint multiple still-valid token sets; a failed Create left the old refresh revoked with no new pair, deadlocking the session; after rotation the new JWT `client_id` was set to the old row's `client_info` (UA), so two independent sessions with the same UA revoked each other's refresh tokens at logout.
- Fix: uncommitted working tree (baseline `6b050ff`). Migration `0040_auth_token_session.sql` adds `session_id`; fixed SQL `ConsumeRefreshToken`/`RevokeActiveTokensBySession`; login and rotation commit in a single transaction; sentinel errors `ErrInvalidRefreshToken`/`ErrRefreshTokenConsumed` etc., with the handler mapping 401/500 via `errors.Is`; legacy rows with empty `session_id` keep the old logout path and start a fresh chain on next rotation.
- Done right: `TestRefreshToken_ConcurrentSameRefresh` (file DB, 24 goroutines — exactly one succeeds, all others consumed), `TestRefreshToken_ConsumedStateSurvivesRestart` (reopened DB + new AuthService still rejects the consumed token while the chain head rotates), `TestRefreshToken_RollbackKeepsRefreshRetryable`, and `TestLogout_SessionIsolationSameUA` pass, including under `go test -race`; handler tests confirm 401 with an explicit "already used" message for reuse and 500 for a closed database.
- Seen 1× (2026-10-14). Status: lesson.

## Entry Format

```text
## Stable ID: one-sentence rule
Scope: repository/module/platform
Failure: owner/repo#number, commit SHA, date, behavior and impact
Fix: commit SHA, validation object and observed outcome
Done right: successful example and evidence; pending if unavailable
Seen N× (YYYY-MM-DD, YYYY-MM-DD)
Status: lesson / promotion candidate / promoted (authoritative destination)
```
