package services

import (
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"songloft/internal/database"
	"songloft/internal/database/testutil"
	"songloft/internal/models"
)

// parseClientID 解析令牌里的 client_id（会话身份）声明。
func parseClientID(t *testing.T, secretHex, token string) string {
	t.Helper()
	secret, err := hex.DecodeString(secretHex)
	if err != nil {
		t.Fatalf("decode secret: %v", err)
	}
	claims := &Claims{}
	if _, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (interface{}, error) {
		return secret, nil
	}); err != nil {
		t.Fatalf("parse token: %v", err)
	}
	return claims.ClientID
}

// newAuthFileEnv 在临时文件库上构造测试环境，用于多连接并发与重启持久化场景。
func newAuthFileEnv(t *testing.T) (*authTestEnv, string) {
	t.Helper()
	secret, _ := GenerateSecret()
	dbPath := filepath.Join(t.TempDir(), "auth-test.db")
	mdb, err := database.Open(dbPath)
	if err != nil {
		t.Fatalf("open file db: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	if err := mdb.ConfigRepository().Set(context.Background(), &models.Config{Key: "jwt_secret", Value: secret}); err != nil {
		t.Fatalf("seed jwt_secret: %v", err)
	}
	svc, err := NewAuthService(mdb.ConfigRepository(), mdb, mdb.TokenRepository(), "admin", "password")
	if err != nil {
		t.Fatalf("create auth service: %v", err)
	}
	t.Cleanup(svc.Close)
	return &authTestEnv{
		db:      mdb,
		configs: mdb.ConfigRepository(),
		tokens:  mdb.TokenRepository(),
	}, secret
}

func serviceFromEnv(env *authTestEnv, t *testing.T) *AuthService {
	t.Helper()
	svc, err := NewAuthService(env.configs, env.db, env.tokens, "admin", "password")
	if err != nil {
		t.Fatalf("create auth service: %v", err)
	}
	t.Cleanup(svc.Close)
	return svc
}

// TestRefreshToken_RotationHappyPath 验证无故障轮换流程不变：新对可用，
// 旧 access 仍然有效（轮换不撤销它），旧 refresh 被明确判为已消费，
// 会话身份跨连续刷新保持稳定。
func TestRefreshToken_RotationHappyPath(t *testing.T) {
	env := newAuthTestEnv(t)
	secret, _ := GenerateSecret()
	env.seedJWTSecret(t, secret)
	svc := serviceFromEnv(env, t)

	ctx := context.Background()
	login, err := svc.Login(ctx, "admin", "password", "UA/1.0")
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	sid0 := parseClientID(t, secret, login.AccessToken)

	r1, err := svc.RefreshToken(ctx, login.RefreshToken, "UA/1.0")
	if err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	sid1 := parseClientID(t, secret, r1.AccessToken)
	if sid1 != sid0 {
		t.Fatalf("session id changed after refresh: %q -> %q", sid0, sid1)
	}

	// 新 access / refresh 均有效。
	if _, err := svc.ValidateToken(ctx, r1.AccessToken); err != nil {
		t.Fatalf("new access token invalid: %v", err)
	}
	r2, err := svc.RefreshToken(ctx, r1.RefreshToken, "UA/2.0")
	if err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if sid := parseClientID(t, secret, r2.RefreshToken); sid != sid0 {
		t.Fatalf("session id not stable on refresh token claim: %q", sid)
	}

	// 旧 access 在轮换后仍然有效（保持历史行为）。
	if _, err := svc.ValidateToken(ctx, login.AccessToken); err != nil {
		t.Fatalf("previous access token should stay valid after rotation: %v", err)
	}

	// 已消费的旧 refresh（两次）都必须被明确拒绝。
	for _, used := range []string{login.RefreshToken, r1.RefreshToken} {
		_, err := svc.RefreshToken(ctx, used, "UA/1.0")
		if !errors.Is(err, ErrRefreshTokenConsumed) {
			t.Fatalf("expected ErrRefreshTokenConsumed, got %v", err)
		}
	}

	// 最新 refresh 仍可继续轮换。
	if _, err := svc.RefreshToken(ctx, r2.RefreshToken, "UA/1.0"); err != nil {
		t.Fatalf("latest refresh should rotate: %v", err)
	}
}

// TestRefreshToken_ConcurrentSameRefresh 多个请求携带同一 refresh token
// 并发刷新时，只允许一个成功，其余全部得到「已消费」，且只有一套新会话有效。
func TestRefreshToken_ConcurrentSameRefresh(t *testing.T) {
	env, _ := newAuthFileEnv(t)
	svc := serviceFromEnv(env, t)

	ctx := context.Background()
	login, err := svc.Login(ctx, "admin", "password", "UA/1.0")
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	const n = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	consumed := 0
	otherErrs := 0
	var winner string

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			resp, err := svc.RefreshToken(ctx, login.RefreshToken, "UA/1.0")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
				winner = resp.RefreshToken
			case errors.Is(err, ErrRefreshTokenConsumed):
				consumed++
			default:
				otherErrs++
				t.Errorf("unexpected refresh error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if successes != 1 {
		t.Fatalf("want exactly 1 successful refresh, got %d (consumed=%d other=%d)", successes, consumed, otherErrs)
	}
	if consumed != n-1 {
		t.Fatalf("want %d consumed errors, got %d", n-1, consumed)
	}
	if otherErrs != 0 {
		t.Fatalf("want no other errors, got %d", otherErrs)
	}

	// 赢家的新 refresh 可以继续轮换，旧 refresh 稳定地判为已消费。
	if _, err := svc.RefreshToken(ctx, winner, "UA/1.0"); err != nil {
		t.Fatalf("winning refresh chain should continue: %v", err)
	}
	if _, err := svc.RefreshToken(ctx, login.RefreshToken, "UA/1.0"); !errors.Is(err, ErrRefreshTokenConsumed) {
		t.Fatalf("old refresh must stay consumed after race, got %v", err)
	}
}

// TestRefreshToken_ConsumedStateSurvivesRestart 消费结论由持久状态决定：
// 换一个 AuthService（空内存缓存、新进程语义）后，已消费 refresh 仍被拒绝，
// 新 refresh 仍可完成轮换。
func TestRefreshToken_ConsumedStateSurvivesRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "auth-restart.db")
	secret, _ := GenerateSecret()

	openSvc := func(t *testing.T) (*AuthService, *database.SQLiteDB) {
		t.Helper()
		mdb, err := database.Open(dbPath)
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		if err := mdb.ConfigRepository().Set(context.Background(), &models.Config{Key: "jwt_secret", Value: secret}); err != nil {
			t.Fatalf("seed secret: %v", err)
		}
		svc, err := NewAuthService(mdb.ConfigRepository(), mdb, mdb.TokenRepository(), "admin", "password")
		if err != nil {
			t.Fatalf("new service: %v", err)
		}
		return svc, mdb
	}

	svc1, mdb1 := openSvc(t)
	ctx := context.Background()
	login, err := svc1.Login(ctx, "admin", "password", "UA/1.0")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	r1, err := svc1.RefreshToken(ctx, login.RefreshToken, "UA/1.0")
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	svc1.Close()
	if err := mdb1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// “重启”：新连接、新服务实例、全新内存缓存。
	svc2, mdb2 := openSvc(t)
	defer mdb2.Close()
	defer svc2.Close()

	if _, err := svc2.RefreshToken(ctx, login.RefreshToken, "UA/1.0"); !errors.Is(err, ErrRefreshTokenConsumed) {
		t.Fatalf("consumed refresh must stay rejected after restart, got %v", err)
	}
	if _, err := svc2.RefreshToken(ctx, r1.RefreshToken, "UA/1.0"); err != nil {
		t.Fatalf("unconsumed latest refresh should rotate after restart: %v", err)
	}
}

// TestRefreshToken_RollbackKeepsRefreshRetryable 事务中途失败时
// 旧 refresh 的消费随回滚撤销，之后仍能正常完成轮换，不锁死会话。
func TestRefreshToken_RollbackKeepsRefreshRetryable(t *testing.T) {
	env := newAuthTestEnv(t)
	secret, _ := GenerateSecret()
	env.seedJWTSecret(t, secret)
	svc := serviceFromEnv(env, t)
	ctx := context.Background()

	login, err := svc.Login(ctx, "admin", "password", "UA/1.0")
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	boom := errors.New("simulated failure after consume")
	err = env.db.RunInTx(ctx, func(ctx context.Context, uow *database.UnitOfWork) error {
		n, err := uow.Tokens.ConsumeRefreshToken(ctx, login.RefreshToken, "test", time.Now())
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
		if n != 1 {
			t.Fatalf("want 1 consumed row, got %d", n)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want wrapped boom, got %v", err)
	}

	// 回滚后旧 refresh 未被消费，轮换必须成功。
	r1, err := svc.RefreshToken(ctx, login.RefreshToken, "UA/1.0")
	if err != nil {
		t.Fatalf("refresh should succeed after rolled-back consume: %v", err)
	}
	if _, err := svc.ValidateToken(ctx, r1.AccessToken); err != nil {
		t.Fatalf("new access invalid: %v", err)
	}
}

// TestRefreshToken_InvalidTypeExpiredAndUnknown 类型、有效期与存在性校验语义不变。
func TestRefreshToken_InvalidTypeExpiredAndUnknown(t *testing.T) {
	env := newAuthTestEnv(t)
	secret, _ := GenerateSecret()
	env.seedJWTSecret(t, secret)
	svc := serviceFromEnv(env, t)
	ctx := context.Background()

	login, err := svc.Login(ctx, "admin", "password", "UA/1.0")
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	// 拿 access token 当 refresh 用。
	if _, err := svc.RefreshToken(ctx, login.AccessToken, "UA/1.0"); !errors.Is(err, ErrRefreshTokenWrongType) {
		t.Fatalf("want wrong type error, got %v", err)
	}

	// 不存在的 refresh。
	if _, err := svc.RefreshToken(ctx, "no-such-token", "UA/1.0"); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("want invalid refresh token, got %v", err)
	}

	// 已过期的 refresh 记录。
	expired := &models.AuthToken{
		TokenID:   "expired-refresh-token",
		TokenType: "refresh",
		SessionID: "sid-expired",
		ExpiresAt: time.Now().Add(-time.Hour),
		CreatedAt: time.Now().Add(-2 * time.Hour),
	}
	if err := env.tokens.Create(ctx, expired); err != nil {
		t.Fatalf("seed expired refresh: %v", err)
	}
	if _, err := svc.RefreshToken(ctx, expired.TokenID, "UA/1.0"); !errors.Is(err, ErrRefreshTokenExpired) {
		t.Fatalf("want expired error, got %v", err)
	}
}

// TestLogout_SessionIsolationSameUA 相同 UA 的两个独立登录是不同会话；
// 登出其一只撤销该会话当前 access 与 refresh 链，另一个会话完全不受影响。
func TestLogout_SessionIsolationSameUA(t *testing.T) {
	env, secret := newAuthFileEnv(t)
	svc := serviceFromEnv(env, t)
	ctx := context.Background()

	a, err := svc.Login(ctx, "admin", "password", "SAME-UA")
	if err != nil {
		t.Fatalf("login a: %v", err)
	}
	b, err := svc.Login(ctx, "admin", "password", "SAME-UA")
	if err != nil {
		t.Fatalf("login b: %v", err)
	}
	sidA := parseClientID(t, secret, a.AccessToken)
	sidB := parseClientID(t, secret, b.AccessToken)
	if sidA == sidB {
		t.Fatal("two independent logins must have different session ids")
	}

	// A 轮换一次，链上有新旧两条 refresh。
	ar, err := svc.RefreshToken(ctx, a.RefreshToken, "SAME-UA")
	if err != nil {
		t.Fatalf("refresh a: %v", err)
	}

	// 登出会话 A（按中间件注入的 clientID = JWT 中的会话身份）。
	if err := svc.Logout(ctx, ar.AccessToken, sidA); err != nil {
		t.Fatalf("logout a: %v", err)
	}

	// A 的当前 access、旧 access 与 refresh 链全部失效。
	if _, err := svc.ValidateToken(ctx, ar.AccessToken); err == nil {
		t.Error("session A current access must be revoked")
	}
	if _, err := svc.ValidateToken(ctx, a.AccessToken); err == nil {
		t.Error("session A previous access must be revoked")
	}
	if _, err := svc.RefreshToken(ctx, ar.RefreshToken, "SAME-UA"); !errors.Is(err, ErrRefreshTokenRevoked) {
		t.Fatalf("session A refresh chain must be revoked, got %v", err)
	}

	// 会话 B 的 access 仍有效，refresh 仍能轮换。
	if _, err := svc.ValidateToken(ctx, b.AccessToken); err != nil {
		t.Fatalf("session B access must stay valid: %v", err)
	}
	br, err := svc.RefreshToken(ctx, b.RefreshToken, "SAME-UA")
	if err != nil {
		t.Fatalf("session B refresh must work: %v", err)
	}
	if _, err := svc.ValidateToken(ctx, br.AccessToken); err != nil {
		t.Fatalf("session B new access invalid: %v", err)
	}
}

// TestRefreshToken_StorageFailureNotCredential 存储故障不得被归类为凭证错误。
func TestRefreshToken_StorageFailureNotCredential(t *testing.T) {
	mdb := testutil.OpenMemoryDB(t)
	secret, _ := GenerateSecret()
	if err := mdb.ConfigRepository().Set(context.Background(), &models.Config{Key: "jwt_secret", Value: secret}); err != nil {
		t.Fatalf("seed secret: %v", err)
	}
	svc, err := NewAuthService(mdb.ConfigRepository(), mdb, mdb.TokenRepository(), "admin", "password")
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	if err := mdb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err = svc.RefreshToken(context.Background(), "whatever", "UA/1.0")
	if err == nil {
		t.Fatal("expected error on closed db")
	}
	if IsRefreshCredentialError(err) {
		t.Fatalf("storage failure must not be classified as credential error: %v", err)
	}
}

// TestLogin_PersistsAtomicPairAndStableIdentity 登录写入的两条记录共享
// 同一随机会话身份；相同 UA 的登录相互独立；登录的存储故障不是凭证错误。
func TestLogin_PersistsAtomicPairAndStableIdentity(t *testing.T) {
	env := newAuthTestEnv(t)
	secret, _ := GenerateSecret()
	env.seedJWTSecret(t, secret)
	svc := serviceFromEnv(env, t)
	ctx := context.Background()

	a, err := svc.Login(ctx, "admin", "password", "SAME-UA")
	if err != nil {
		t.Fatalf("login a: %v", err)
	}
	b, err := svc.Login(ctx, "admin", "password", "SAME-UA")
	if err != nil {
		t.Fatalf("login b: %v", err)
	}
	if parseClientID(t, secret, a.AccessToken) == parseClientID(t, secret, b.AccessToken) {
		t.Fatal("same-UA logins must have distinct session ids")
	}

	rec, err := env.tokens.GetByID(ctx, a.RefreshToken)
	if err != nil {
		t.Fatalf("get refresh row: %v", err)
	}
	if rec.SessionID != parseClientID(t, secret, a.RefreshToken) {
		t.Fatal("persisted session id must equal JWT client_id")
	}
	if rec.ClientInfo != "SAME-UA" {
		t.Fatalf("client info stored for display only, got %q", rec.ClientInfo)
	}
}
