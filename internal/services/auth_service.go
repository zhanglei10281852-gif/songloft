package services

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"songloft/internal/database"
	"songloft/internal/models"
)

// 认证业务哨兵错误。凭证类错误由 handler 映射为 401；
// 数据库/存储故障必须原样上抛为 500，不得伪装成凭证无效。
var (
	// ErrInvalidCredentials 用户名或密码错误。
	ErrInvalidCredentials = errors.New("invalid credentials")

	// ErrInvalidRefreshToken 是所有 refresh 凭证类错误的基类，
	// 具体原因（过期/撤销/已消费/类型错误）都 wrap 它。
	ErrInvalidRefreshToken = errors.New("invalid refresh token")
	// ErrRefreshTokenExpired refresh token 已过期。
	ErrRefreshTokenExpired = fmt.Errorf("%w: refresh token has expired", ErrInvalidRefreshToken)
	// ErrRefreshTokenRevoked refresh token 已被主动撤销（如登出、令牌管理接口）。
	ErrRefreshTokenRevoked = fmt.Errorf("%w: refresh token has been revoked", ErrInvalidRefreshToken)
	// ErrRefreshTokenConsumed refresh token 已被轮换消费；并发刷新中的失败方
	// 以及进程重启后再次使用旧 token 都必须落到这个错误（refresh token reuse）。
	ErrRefreshTokenConsumed = fmt.Errorf("%w: refresh token has already been consumed", ErrInvalidRefreshToken)
	// ErrRefreshTokenWrongType 提交的不是 refresh 类型令牌。
	ErrRefreshTokenWrongType = fmt.Errorf("%w: token is not a refresh token", ErrInvalidRefreshToken)
)

// IsRefreshCredentialError 判断错误是否属于 refresh 凭证类错误（对应 HTTP 401），
// 其余错误（存储故障等）应由调用方按 500 处理。
func IsRefreshCredentialError(err error) bool {
	return errors.Is(err, ErrInvalidRefreshToken)
}

// TokenRepository 认证令牌仓储接口（AuthService 依赖）。
type TokenRepository interface {
	Create(ctx context.Context, token *models.AuthToken) error
	GetByID(ctx context.Context, tokenID string) (*models.AuthToken, error)
	Revoke(ctx context.Context, tokenID, revokedBy, reason string) error
	ListActive(ctx context.Context, filter *database.TokenFilter) ([]*models.AuthToken, error)
	CleanExpired(ctx context.Context) (int64, error)
	IsRevoked(ctx context.Context, tokenID string) (bool, error)
}

// tokenTx 提供令牌写入所需的事务入口。实现方为 *database.SQLiteDB。
// 旧 refresh 的消费与新令牌对的写入必须在同一个事务里共同提交。
type tokenTx interface {
	RunInTx(ctx context.Context, fn func(context.Context, *database.UnitOfWork) error) error
}

// TokenCacheEntry Token 缓存条目
type TokenCacheEntry struct {
	Claims    *Claims
	ExpiresAt time.Time
	Revoked   bool
}

// AuthService 认证服务
type AuthService struct {
	tokens   TokenRepository
	tx       tokenTx
	secret   []byte
	username string
	password string
	// Token 内存缓存，key 为 token 字符串，value 为缓存条目
	tokenCache sync.Map // map[string]*TokenCacheEntry
	done       chan struct{}
	closeOnce  sync.Once
}

// Claims JWT声明结构
type Claims struct {
	ClientID string `json:"client_id"`
	jwt.RegisteredClaims
}

// RefreshResponse 刷新Token响应
type RefreshResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

// NewAuthService 创建认证服务
func NewAuthService(configs ConfigRepository, tx tokenTx, tokens TokenRepository, username, password string) (*AuthService, error) {
	// 从数据库获取 JWT 密钥
	config, err := configs.Get(context.Background(), "jwt_secret")
	if err != nil {
		return nil, fmt.Errorf("failed to get jwt secret: %w", err)
	}

	// 解码密钥
	secret, err := hex.DecodeString(config.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode jwt secret: %w", err)
	}

	s := &AuthService{
		tokens:   tokens,
		tx:       tx,
		secret:   secret,
		username: username,
		password: password,
		done:     make(chan struct{}),
	}

	// 启动缓存清理协程
	go s.startCacheCleanup()

	return s, nil
}

// GenerateSecret 生成新的 JWT 密钥
func GenerateSecret() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// getAdminUsername 获取管理员用户名
func (s *AuthService) getAdminUsername(ctx context.Context) (string, error) {
	return s.username, nil
}

// getAdminPassword 获取管理员密码
func (s *AuthService) getAdminPassword(ctx context.Context) (string, error) {
	return s.password, nil
}

// Login 用户登录
func (s *AuthService) Login(ctx context.Context, username, password, clientInfo string) (*models.LoginResponse, error) {
	// 从数据库获取管理员账户信息
	adminUsername, err := s.getAdminUsername(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get admin username: %w", err)
	}

	adminPassword, err := s.getAdminPassword(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get admin password: %w", err)
	}

	if username != adminUsername || password != adminPassword {
		return nil, ErrInvalidCredentials
	}

	// 生成随机会话身份：一次登录一个会话，后续轮换保持不变。
	// 仅用于展示的 clientInfo（UA 等）不能充当会话键。
	sessionID, err := generateSessionID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate session id: %w", err)
	}

	now := time.Now()

	// 生成 Access Token (7 天过期)
	accessToken, accessExp, err := s.generateToken(sessionID, "access", 7*24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("failed to generate access token: %w", err)
	}

	// 生成 Refresh Token (30 天过期)
	refreshToken, refreshExp, err := s.generateToken(sessionID, "refresh", 30*24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("failed to generate refresh token: %w", err)
	}

	accessRecord := &models.AuthToken{
		TokenID:    accessToken,
		TokenType:  "access",
		ClientInfo: clientInfo,
		SessionID:  sessionID,
		ExpiresAt:  accessExp,
		CreatedAt:  now,
	}

	refreshRecord := &models.AuthToken{
		TokenID:    refreshToken,
		TokenType:  "refresh",
		ClientInfo: clientInfo,
		SessionID:  sessionID,
		ExpiresAt:  refreshExp,
		CreatedAt:  now,
	}

	// 一对令牌必须原子持久化：任一写入失败整体回滚，不留下孤立令牌。
	if err := s.tx.RunInTx(ctx, func(ctx context.Context, uow *database.UnitOfWork) error {
		if err := uow.Tokens.Create(ctx, accessRecord); err != nil {
			return err
		}
		if err := uow.Tokens.Create(ctx, refreshRecord); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to persist token pair: %w", err)
	}

	// 清理过期 Token
	_, _ = s.tokens.CleanExpired(ctx)

	return &models.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresIn:    int64(accessExp.Sub(now).Seconds()),
		TokenType:    "Bearer",
	}, nil
}

// getCachedToken 从缓存获取 Token 信息
func (s *AuthService) getCachedToken(tokenString string) (*TokenCacheEntry, bool) {
	if entry, ok := s.tokenCache.Load(tokenString); ok {
		cacheEntry := entry.(*TokenCacheEntry)
		// 检查缓存是否过期
		if time.Now().Before(cacheEntry.ExpiresAt) && !cacheEntry.Revoked {
			return cacheEntry, true
		}
		// 缓存已过期，删除它
		s.tokenCache.Delete(tokenString)
	}
	return nil, false
}

// setTokenCache 设置 Token 缓存
func (s *AuthService) setTokenCache(tokenString string, claims *Claims, expiresAt time.Time, revoked bool) {
	s.tokenCache.Store(tokenString, &TokenCacheEntry{
		Claims:    claims,
		ExpiresAt: expiresAt,
		Revoked:   revoked,
	})
}

// deleteTokenCache 删除 Token 缓存
func (s *AuthService) deleteTokenCache(tokenString string) {
	s.tokenCache.Delete(tokenString)
}

// Close 关闭 AuthService，停止缓存清理协程
func (s *AuthService) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
	})
}

// startCacheCleanup 启动缓存清理协程，每分钟清理一次过期缓存
func (s *AuthService) startCacheCleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			s.tokenCache.Range(func(key, value interface{}) bool {
				entry := value.(*TokenCacheEntry)
				// 如果缓存已过期或 token 已被撤销，删除它
				if now.After(entry.ExpiresAt) || entry.Revoked {
					s.tokenCache.Delete(key)
				}
				return true
			})
		case <-s.done:
			return
		}
	}
}

// Logout 用户登出。只撤销当前访问令牌所属会话的当前 access token 与 refresh 链，
// 不影响其他会话——即使它们的 User-Agent 等客户端描述完全相同。
func (s *AuthService) Logout(ctx context.Context, accessToken, clientID string) error {
	record, err := s.tokens.GetByID(ctx, accessToken)
	if err != nil {
		// 与历史行为一致：令牌不存在（如插件系统内存令牌）时报错，由 handler 返回 500。
		return fmt.Errorf("failed to get access token: %w", err)
	}

	// 0040 迁移前的存量行没有会话身份，走旧的按 JWT client_id 匹配路径。
	if record.SessionID == "" {
		return s.legacyLogout(ctx, accessToken, clientID)
	}

	// 先在事务内读出本会话仍活跃的令牌 ID，再一次性全部撤销，
	// 保证「列举」与「撤销」之间不会因并发轮换漏掉链头。
	var revokedIDs []string
	if err := s.tx.RunInTx(ctx, func(ctx context.Context, uow *database.UnitOfWork) error {
		ids, err := uow.Tokens.GetActiveTokenIDsBySession(ctx, record.SessionID)
		if err != nil {
			return err
		}
		if _, err := uow.Tokens.RevokeActiveTokensBySession(ctx, record.SessionID, clientID, "logout", time.Now()); err != nil {
			return err
		}
		revokedIDs = ids
		return nil
	}); err != nil {
		return fmt.Errorf("failed to revoke session tokens: %w", err)
	}

	// 提交成功后再清理进程内缓存，避免其他持有该会话令牌的请求继续命中缓存。
	for _, id := range revokedIDs {
		s.deleteTokenCache(id)
	}
	return nil
}

// legacyLogout 是 0040 会话列迁移前存量令牌的登出路径，保持原有行为：
// 显式撤销当前 access token，再按 JWT client_id 匹配并撤销同客户端的 refresh token。
func (s *AuthService) legacyLogout(ctx context.Context, accessToken, clientID string) error {
	// 撤销 Access Token
	if err := s.tokens.Revoke(ctx, accessToken, clientID, "logout"); err != nil {
		return fmt.Errorf("failed to revoke access token: %w", err)
	}
	// 清除缓存
	s.deleteTokenCache(accessToken)

	// 查找并撤销对应的 Refresh Token
	filter := &database.TokenFilter{
		TokenType: "refresh",
	}

	tokens, err := s.tokens.ListActive(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to list refresh tokens: %w", err)
	}

	for _, token := range tokens {
		// 解析 refresh token 的 JWT 获取 ClientID，按 clientID 匹配同一客户端
		parsed, parseErr := jwt.ParseWithClaims(token.TokenID, &Claims{}, func(t *jwt.Token) (interface{}, error) {
			return s.secret, nil
		})
		if parseErr != nil {
			continue
		}
		claims, ok := parsed.Claims.(*Claims)
		if !ok || claims.ClientID != clientID {
			continue
		}
		if err := s.tokens.Revoke(ctx, token.TokenID, clientID, "logout"); err != nil {
			return fmt.Errorf("failed to revoke refresh token: %w", err)
		}
		// 清除缓存
		s.deleteTokenCache(token.TokenID)
	}

	return nil
}

// RefreshToken 刷新Token。
//
// 并发安全与原子性：事务内第一条语句是带条件的消费 UPDATE
// （未撤销 + 未过期 + 类型为 refresh 才命中），因此同 refresh token 的多个
// 并发请求里只有一个能消费成功并拿到新令牌对；命中 0 行的请求会读到持久状态
// 并得到明确的「已消费」错误。旧 refresh 的消费与新 access/refresh 的写入
// 在同一事务共同提交：任一步失败（含提交失败）整体回滚，旧 refresh 仍可重试，
// 也不会留下孤立新令牌。消费结论完全由数据库状态决定，进程重启后依旧成立。
func (s *AuthService) RefreshToken(ctx context.Context, refreshToken, clientInfo string) (*RefreshResponse, error) {
	var resp *RefreshResponse

	err := s.tx.RunInTx(ctx, func(ctx context.Context, uow *database.UnitOfWork) error {
		now := time.Now()

		// 条件消费必须是事务内第一条语句：直接以写 UPDATE 抢占 SQLite 写锁，
		// 并发轮换者在 busy_timeout 后重估，最多一个命中。
		consumed, err := uow.Tokens.ConsumeRefreshToken(ctx, refreshToken, "system", now)
		if err != nil {
			return err
		}

		// 读取持久状态用于生成新对（命中时）或分类失败原因（未命中时）。
		old, err := uow.Tokens.GetByID(ctx, refreshToken)
		if err != nil {
			// 刚消费命中却读不到行属于存储不一致，按存储错误返回而不是凭证无效。
			if errors.Is(err, database.ErrNotFound) && consumed == 1 {
				return fmt.Errorf("consumed refresh token is missing after conditional update: %w", err)
			}
			if !errors.Is(err, database.ErrNotFound) {
				return err
			}
		}

		if consumed == 0 {
			return classifyRefreshFailure(old, now)
		}

		// 会话身份跨轮换保持稳定；存量行（session_id 为空）从本次轮换起开启新链。
		sessionID := old.SessionID
		if sessionID == "" {
			sessionID, err = generateSessionID()
			if err != nil {
				return fmt.Errorf("failed to generate session id: %w", err)
			}
		}

		// 生成新的 Access Token (7 天过期)
		newAccessToken, accessExp, err := s.generateToken(sessionID, "access", 7*24*time.Hour)
		if err != nil {
			return fmt.Errorf("failed to generate new access token: %w", err)
		}

		// 生成新的 Refresh Token (30 天过期)
		newRefreshToken, refreshExp, err := s.generateToken(sessionID, "refresh", 30*24*time.Hour)
		if err != nil {
			return fmt.Errorf("failed to generate new refresh token: %w", err)
		}

		// clientInfo 只写入展示字段，不参与会话身份判定。
		accessRecord := &models.AuthToken{
			TokenID:    newAccessToken,
			TokenType:  "access",
			ClientInfo: clientInfo,
			SessionID:  sessionID,
			ExpiresAt:  accessExp,
			CreatedAt:  now,
		}
		refreshRecord := &models.AuthToken{
			TokenID:    newRefreshToken,
			TokenType:  "refresh",
			ClientInfo: clientInfo,
			SessionID:  sessionID,
			ExpiresAt:  refreshExp,
			CreatedAt:  now,
		}

		if err := uow.Tokens.Create(ctx, accessRecord); err != nil {
			return err
		}
		if err := uow.Tokens.Create(ctx, refreshRecord); err != nil {
			return err
		}

		resp = &RefreshResponse{
			AccessToken:  newAccessToken,
			RefreshToken: newRefreshToken,
			ExpiresIn:    int64(accessExp.Sub(now).Seconds()),
			TokenType:    "Bearer",
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 提交成功后清理旧 refresh 的进程内缓存。
	s.deleteTokenCache(refreshToken)

	return resp, nil
}

// classifyRefreshFailure 在条件消费命中 0 行时，依据持久状态给出明确的凭证错误。
// 任何无法归类的异常状态都返回普通错误（500），而不是伪装成凭证无效。
func classifyRefreshFailure(old *models.AuthToken, now time.Time) error {
	switch {
	case old == nil:
		return fmt.Errorf("%w: refresh token not found", ErrInvalidRefreshToken)
	case old.TokenType != "refresh":
		return ErrRefreshTokenWrongType
	case !old.ExpiresAt.After(now):
		return ErrRefreshTokenExpired
	case old.RevokedReason == database.RefreshConsumedReason:
		return ErrRefreshTokenConsumed
	case !old.RevokedAt.IsZero():
		return ErrRefreshTokenRevoked
	default:
		return fmt.Errorf("refresh token consume matched no rows in an unexpected state")
	}
}

// ValidateToken 验证 Token
func (s *AuthService) ValidateToken(ctx context.Context, tokenString string) (*Claims, error) {
	// 先尝试从缓存获取
	if cacheEntry, found := s.getCachedToken(tokenString); found {
		return cacheEntry.Claims, nil
	}

	// 缓存未命中，解析 Token 获取 claims
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return s.secret, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token claims")
	}

	// 如果是插件系统的 Token，跳过数据库撤销检查
	// 因为插件 Token 不保存在数据库中
	if claims.ClientID == "plugin-system" {
		// 插件 Token 也缓存起来，使用 token 的过期时间
		s.setTokenCache(tokenString, claims, claims.ExpiresAt.Time, false)
		return claims, nil
	}

	// 对于普通用户 Token，检查是否被撤销
	isRevoked, err := s.tokens.IsRevoked(ctx, tokenString)
	if err != nil {
		return nil, fmt.Errorf("failed to check token status: %w", err)
	}

	if isRevoked {
		// 缓存撤销状态，使用 token 的过期时间
		s.setTokenCache(tokenString, claims, claims.ExpiresAt.Time, true)
		return nil, fmt.Errorf("token has been revoked")
	}

	// 缓存有效 Token，使用 token 的过期时间
	s.setTokenCache(tokenString, claims, claims.ExpiresAt.Time, false)

	return claims, nil
}

// ListActiveTokens 列出活跃Token
func (s *AuthService) ListActiveTokens(ctx context.Context, filter *database.TokenFilter) ([]*models.AuthToken, error) {
	return s.tokens.ListActive(ctx, filter)
}

// RevokeToken 撤销 Token
func (s *AuthService) RevokeToken(ctx context.Context, tokenID, revokedBy, reason string) error {
	err := s.tokens.Revoke(ctx, tokenID, revokedBy, reason)
	if err == nil {
		// 清除缓存
		s.deleteTokenCache(tokenID)
	}
	return err
}

// GeneratePluginToken 生成插件专用的永久 JWT Token
// 这个 Token 不会过期，专门用于插件内部调用主程序 API
// 注意：此 Token 不保存到数据库，仅在内存中使用，程序重启后会重新生成
func (s *AuthService) GeneratePluginToken(ctx context.Context) (string, error) {
	clientID := "plugin-system"

	// 生成一个 100 年后过期的 Token（实际上相当于永久）
	expirationTime := time.Now().Add(100 * 365 * 24 * time.Hour)

	tokenID, err := generateRandomString(32)
	if err != nil {
		return "", fmt.Errorf("failed to generate token ID: %w", err)
	}

	claims := &Claims{
		ClientID: clientID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ID:        tokenID,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(s.secret)
	if err != nil {
		return "", fmt.Errorf("failed to generate plugin token: %w", err)
	}

	// 注意：插件 Token 不保存到数据库，因为：
	// 1. 数据库的 token_type 约束只允许 'access' 和 'refresh'
	// 2. 插件 Token 是内部使用，不需要持久化
	// 3. 程序重启后会自动重新生成
	//
	// 这样做的好处：
	// - 不需要修改数据库 schema
	// - 不需要数据库迁移
	// - Token 验证仍然通过 JWT 签名保证安全性

	return tokenString, nil
}

// generateToken 生成JWT Token
func (s *AuthService) generateToken(clientID, tokenType string, expiresIn time.Duration) (string, time.Time, error) {
	expirationTime := time.Now().Add(expiresIn)

	tokenID, err := generateRandomString(32)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("generate token ID: %w", err)
	}

	claims := &Claims{
		ClientID: clientID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ID:        tokenID,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(s.secret)
	if err != nil {
		return "", time.Time{}, err
	}

	return tokenString, expirationTime, nil
}

// generateSessionID 生成随机会话身份 ID
func generateSessionID() (string, error) {
	return generateRandomString(16)
}

// generateRandomString 生成随机字符串
func generateRandomString(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(bytes)[:length], nil
}
