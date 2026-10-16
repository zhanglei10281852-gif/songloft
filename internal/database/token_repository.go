package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"

	"songloft/internal/database/sqlc"
	"songloft/internal/models"
)

// RefreshConsumedReason 是 refresh token 被成功轮换消费时写入的撤销原因，
// 也是复用检测（refresh token reuse）区分「已消费」与普通撤销的依据。
const RefreshConsumedReason = "token refreshed"

// TokenRepository 认证令牌仓储。
// 固定 SQL 走 sqlc.Queries，动态过滤/排序的 ListActive 走 squirrel。
type TokenRepository struct {
	db      sqlc.DBTX
	queries *sqlc.Queries
}

// NewTokenRepository 用 *sql.DB 或 *sql.Tx 构造仓储。
func NewTokenRepository(db sqlc.DBTX) *TokenRepository {
	return &TokenRepository{db: db, queries: sqlc.New(db)}
}

// Create 写入新令牌并回填自增 ID。
func (r *TokenRepository) Create(ctx context.Context, token *models.AuthToken) error {
	var revokedAt sql.NullTime
	if !token.RevokedAt.IsZero() {
		revokedAt = sql.NullTime{Time: token.RevokedAt, Valid: true}
	}
	id, err := r.queries.CreateToken(ctx, sqlc.CreateTokenParams{
		TokenID:       token.TokenID,
		TokenType:     token.TokenType,
		ClientInfo:    token.ClientInfo,
		SessionID:     token.SessionID,
		ExpiresAt:     token.ExpiresAt,
		RevokedAt:     revokedAt,
		RevokedBy:     token.RevokedBy,
		CreatedAt:     token.CreatedAt,
		RevokedReason: token.RevokedReason,
	})
	if err != nil {
		return fmt.Errorf("create token: %w", err)
	}
	token.ID = id
	return nil
}

// GetByID 按 token_id 取记录，找不到返回 ErrNotFound。
func (r *TokenRepository) GetByID(ctx context.Context, tokenID string) (*models.AuthToken, error) {
	row, err := r.queries.GetTokenByID(ctx, tokenID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get token: %w", err)
	}
	return tokenRowToModel(row.ID, row.TokenID, row.TokenType, row.ClientInfo, row.SessionID,
		row.ExpiresAt, row.RevokedAt, row.RevokedBy, row.CreatedAt, row.RevokedReason), nil
}

// Revoke 把指定令牌标记为已撤销，找不到返回 ErrNotFound。
func (r *TokenRepository) Revoke(ctx context.Context, tokenID, revokedBy, reason string) error {
	rows, err := r.queries.RevokeToken(ctx, sqlc.RevokeTokenParams{
		RevokedAt:     sql.NullTime{Time: time.Now(), Valid: true},
		RevokedBy:     revokedBy,
		RevokedReason: reason,
		TokenID:       tokenID,
	})
	if err != nil {
		return fmt.Errorf("revoke token: %w", err)
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

// ConsumeRefreshToken 原子消费一个仍然有效的 refresh token。
// 仅当记录存在、类型为 refresh、未撤销且未过期时才会命中一行；
// 并发轮换时只有一个调用方命中，其余命中 0 行，由调用方按复用处理。
// 必须在事务内、且在任何读操作之前作为第一条语句执行，
// 以借助条件 UPDATE 直接抢占写锁，避免读后写竞态。
func (r *TokenRepository) ConsumeRefreshToken(ctx context.Context, tokenID, revokedBy string, now time.Time) (int64, error) {
	rows, err := r.queries.ConsumeRefreshToken(ctx, sqlc.ConsumeRefreshTokenParams{
		RevokedAt:     sql.NullTime{Time: now, Valid: true},
		RevokedBy:     revokedBy,
		RevokedReason: RefreshConsumedReason,
		TokenID:       tokenID,
		ExpiresAt:     now,
	})
	if err != nil {
		return 0, fmt.Errorf("consume refresh token: %w", err)
	}
	return rows, nil
}

// GetActiveTokenIDsBySession 返回某会话当前仍活跃（未撤销、未过期）的全部令牌 ID。
func (r *TokenRepository) GetActiveTokenIDsBySession(ctx context.Context, sessionID string) ([]string, error) {
	ids, err := r.queries.GetActiveTokenIDsBySession(ctx, sqlc.GetActiveTokenIDsBySessionParams{
		SessionID: sessionID,
		ExpiresAt: time.Now(),
	})
	if err != nil {
		return nil, fmt.Errorf("get active token ids by session: %w", err)
	}
	return ids, nil
}

// RevokeActiveTokensBySession 撤销某会话所有仍活跃的令牌
// （当前 access token 与 refresh 链头），返回受影响行数。
func (r *TokenRepository) RevokeActiveTokensBySession(ctx context.Context, sessionID, revokedBy, reason string, now time.Time) (int64, error) {
	rows, err := r.queries.RevokeActiveTokensBySession(ctx, sqlc.RevokeActiveTokensBySessionParams{
		RevokedAt:     sql.NullTime{Time: now, Valid: true},
		RevokedBy:     revokedBy,
		RevokedReason: reason,
		SessionID:     sessionID,
	})
	if err != nil {
		return 0, fmt.Errorf("revoke tokens by session: %w", err)
	}
	return rows, nil
}

// ListActive 列出未撤销且未过期的令牌，支持按类型筛选 + 白名单排序 + 分页。
func (r *TokenRepository) ListActive(ctx context.Context, filter *TokenFilter) ([]*models.AuthToken, error) {
	if filter == nil {
		filter = &TokenFilter{}
	}
	sb := sq.Select("id", "token_id", "token_type", "client_info", "session_id", "expires_at",
		"revoked_at", "revoked_by", "created_at", "revoked_reason").
		From("auth_tokens").
		Where(sq.Eq{"revoked_at": nil}).
		Where(sq.Gt{"expires_at": time.Now()})

	if filter.TokenType != "" {
		sb = sb.Where(sq.Eq{"token_type": filter.TokenType})
	}

	sb = applyOrder(sb, filter.OrderBy, filter.Order, "created_at DESC", tokenOrderWhitelist, "")
	sb = applyPagination(sb, filter.Limit, filter.Offset)

	query, args, err := sb.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build list tokens sql: %w", err)
	}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list active tokens: %w", err)
	}
	defer rows.Close()

	tokens := []*models.AuthToken{}
	for rows.Next() {
		var (
			id                             int64
			tokenID, tokenType, clientInfo string
			sessionID                      string
			expiresAt, createdAt           time.Time
			revokedAt                      sql.NullTime
			revokedBy, revokedReason       string
		)
		if err := rows.Scan(&id, &tokenID, &tokenType, &clientInfo, &sessionID,
			&expiresAt, &revokedAt, &revokedBy, &createdAt, &revokedReason); err != nil {
			return nil, fmt.Errorf("scan token: %w", err)
		}
		tokens = append(tokens, tokenRowToModel(id, tokenID, tokenType, clientInfo, sessionID,
			expiresAt, revokedAt, revokedBy, createdAt, revokedReason))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tokens: %w", err)
	}
	return tokens, nil
}

// CleanExpired 删除所有已过期记录，返回删除条数。
func (r *TokenRepository) CleanExpired(ctx context.Context) (int64, error) {
	n, err := r.queries.CleanExpiredTokens(ctx, time.Now())
	if err != nil {
		return 0, fmt.Errorf("clean expired tokens: %w", err)
	}
	return n, nil
}

// IsRevoked 判断令牌是否被撤销或已过期。
func (r *TokenRepository) IsRevoked(ctx context.Context, tokenID string) (bool, error) {
	revoked, err := r.queries.IsTokenRevoked(ctx, sqlc.IsTokenRevokedParams{
		TokenID:   tokenID,
		ExpiresAt: time.Now(),
	})
	if err != nil {
		return false, fmt.Errorf("check token revoked: %w", err)
	}
	return revoked, nil
}

func tokenRowToModel(id int64, tokenID, tokenType, clientInfo, sessionID string,
	expiresAt time.Time, revokedAt sql.NullTime, revokedBy string, createdAt time.Time, revokedReason string) *models.AuthToken {
	t := &models.AuthToken{
		ID:            id,
		TokenID:       tokenID,
		TokenType:     tokenType,
		ClientInfo:    clientInfo,
		SessionID:     sessionID,
		ExpiresAt:     expiresAt,
		RevokedBy:     revokedBy,
		CreatedAt:     createdAt,
		RevokedReason: revokedReason,
	}
	if revokedAt.Valid {
		t.RevokedAt = revokedAt.Time
	}
	return t
}
