-- name: CreateToken :execlastid
INSERT INTO auth_tokens (
    token_id, token_type, client_info, session_id, expires_at, revoked_at,
    revoked_by, created_at, revoked_reason
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetTokenByID :one
SELECT id, token_id, token_type, client_info, session_id, expires_at, revoked_at,
    revoked_by, created_at, revoked_reason
FROM auth_tokens WHERE token_id = ?;

-- name: RevokeToken :execrows
UPDATE auth_tokens
SET revoked_at = ?, revoked_by = ?, revoked_reason = ?
WHERE token_id = ?;

-- name: CleanExpiredTokens :execrows
DELETE FROM auth_tokens WHERE expires_at < ?;

-- name: IsTokenRevoked :one
SELECT EXISTS(
    SELECT 1 FROM auth_tokens
    WHERE token_id = ? AND (revoked_at IS NOT NULL OR expires_at < ?)
);

-- ConsumeRefreshToken atomically consumes one active refresh token.
-- The guard conditions make concurrent refreshes race-free: only one
-- statement can match the still-valid row; every later call matches zero
-- rows and the caller reports reuse instead of issuing another pair.
-- name: ConsumeRefreshToken :execrows
UPDATE auth_tokens
SET revoked_at = ?, revoked_by = ?, revoked_reason = ?
WHERE token_id = ?
  AND token_type = 'refresh'
  AND revoked_at IS NULL
  AND expires_at >= ?;

-- name: GetActiveTokenIDsBySession :many
SELECT token_id FROM auth_tokens
WHERE session_id = ?
  AND session_id != ''
  AND revoked_at IS NULL
  AND expires_at > ?;

-- RevokeActiveTokensBySession revokes every still-active token of one
-- session (current access token + refresh chain head). Empty session_id
-- never matches, so pre-migration rows can't be swept in bulk.
-- name: RevokeActiveTokensBySession :execrows
UPDATE auth_tokens
SET revoked_at = ?, revoked_by = ?, revoked_reason = ?
WHERE session_id = ?
  AND session_id != ''
  AND revoked_at IS NULL;
