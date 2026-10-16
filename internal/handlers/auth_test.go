package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"songloft/internal/database/testutil"
	"songloft/internal/models"
	"songloft/internal/services"
)

// newAuthHandlerForTest 启动 :memory: SQLite + 真实 AuthService,
// 返回构造好的 handler。AuthService 是 stateful 的(token 缓存),所以每个测试独占一份。
func newAuthHandlerForTest(t *testing.T) *AuthHandler {
	t.Helper()
	mdb := testutil.OpenMemoryDB(t)

	secret, _ := services.GenerateSecret()
	if err := mdb.ConfigRepository().Set(context.Background(), &models.Config{
		Key:   "jwt_secret",
		Value: secret,
	}); err != nil {
		t.Fatalf("seed jwt_secret: %v", err)
	}

	svc, err := services.NewAuthService(mdb.ConfigRepository(), mdb, mdb.TokenRepository(), "admin", "password")
	if err != nil {
		t.Fatalf("create auth service: %v", err)
	}
	return NewAuthHandler(svc)
}

// TestAuthHandler_Login 测试登录处理器
func TestAuthHandler_Login(t *testing.T) {
	handler := newAuthHandlerForTest(t)

	loginReq := models.LoginRequest{
		Username: "admin",
		Password: "password",
	}

	jsonData, _ := json.Marshal(loginReq)
	req := httptest.NewRequest("POST", "/auth/login", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler.Login(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rr.Code)
	}

	var resp models.LoginResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.AccessToken == "" {
		t.Error("AccessToken should not be empty")
	}
	if resp.RefreshToken == "" {
		t.Error("RefreshToken should not be empty")
	}
	if resp.ExpiresIn <= 0 {
		t.Error("ExpiresIn should be positive")
	}
	if resp.TokenType != "Bearer" {
		t.Errorf("Expected TokenType Bearer, got %s", resp.TokenType)
	}
}

// TestAuthHandler_Login_InvalidRequest 测试登录处理器 - 无效请求
func TestAuthHandler_Login_InvalidRequest(t *testing.T) {
	handler := newAuthHandlerForTest(t)

	req := httptest.NewRequest("POST", "/auth/login", bytes.NewBuffer([]byte("{invalid-json}")))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler.Login(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, rr.Code)
	}
}

// TestAuthHandler_Login_ServiceError 测试登录处理器 - 服务错误（密码错误）
func TestAuthHandler_Login_ServiceError(t *testing.T) {
	handler := newAuthHandlerForTest(t)

	loginReq := models.LoginRequest{
		Username: "admin",
		Password: "wrong-password",
	}

	jsonData, _ := json.Marshal(loginReq)
	req := httptest.NewRequest("POST", "/auth/login", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler.Login(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("Expected status code %d, got %d", http.StatusUnauthorized, rr.Code)
	}
}

// TestAuthHandler_RefreshToken 测试刷新token处理器
func TestAuthHandler_RefreshToken(t *testing.T) {
	handler := newAuthHandlerForTest(t)

	// 先登录获取真实的 refresh token
	loginReq := models.LoginRequest{
		Username: "admin",
		Password: "password",
	}
	loginData, _ := json.Marshal(loginReq)
	loginRequest := httptest.NewRequest("POST", "/auth/login", bytes.NewBuffer(loginData))
	loginRequest.Header.Set("Content-Type", "application/json")
	loginRecorder := httptest.NewRecorder()
	handler.Login(loginRecorder, loginRequest)

	var loginResp models.LoginResponse
	json.Unmarshal(loginRecorder.Body.Bytes(), &loginResp)

	refreshReq := models.RefreshTokenRequest{
		RefreshToken: loginResp.RefreshToken,
	}

	jsonData, _ := json.Marshal(refreshReq)
	req := httptest.NewRequest("POST", "/auth/refresh", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler.RefreshToken(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rr.Code)
	}

	var resp services.RefreshResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.AccessToken == "" {
		t.Error("AccessToken should not be empty")
	}
	if resp.RefreshToken == "" {
		t.Error("RefreshToken should not be empty")
	}
}

// doRefresh 直接调用 RefreshToken handler 并返回 recorder。
func doRefresh(handler *AuthHandler, refreshToken string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(models.RefreshTokenRequest{RefreshToken: refreshToken})
	req := httptest.NewRequest("POST", "/auth/refresh", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.RefreshToken(rr, req)
	return rr
}

// TestAuthHandler_RefreshToken_ConsumedRejectedClearly 已被轮换消费的
// refresh token 必须返回 401，且错误信息明确告知已被使用。
func TestAuthHandler_RefreshToken_ConsumedRejectedClearly(t *testing.T) {
	handler := newAuthHandlerForTest(t)

	loginReq := models.LoginRequest{Username: "admin", Password: "password"}
	loginData, _ := json.Marshal(loginReq)
	loginRequest := httptest.NewRequest("POST", "/auth/login", bytes.NewBuffer(loginData))
	loginRequest.Header.Set("Content-Type", "application/json")
	loginRecorder := httptest.NewRecorder()
	handler.Login(loginRecorder, loginRequest)
	var loginResp models.LoginResponse
	json.Unmarshal(loginRecorder.Body.Bytes(), &loginResp)

	// 第一次轮换成功。
	if rr := doRefresh(handler, loginResp.RefreshToken); rr.Code != http.StatusOK {
		t.Fatalf("first refresh = %d, want 200", rr.Code)
	}

	// 复用旧 refresh：401 且明确提示已被使用。
	rr := doRefresh(handler, loginResp.RefreshToken)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("reused refresh status = %d, want 401", rr.Code)
	}
	var errResp map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp["error"] != "刷新令牌已被使用，请重新登录" {
		t.Fatalf("unexpected error message: %q", errResp["error"])
	}
}

// TestAuthHandler_RefreshToken_StorageFailureReturns500 存储故障必须映射为
// 500，而不是伪装成 401 凭证无效。
func TestAuthHandler_RefreshToken_StorageFailureReturns500(t *testing.T) {
	mdb := testutil.OpenMemoryDB(t)
	secret, _ := services.GenerateSecret()
	if err := mdb.ConfigRepository().Set(context.Background(), &models.Config{
		Key:   "jwt_secret",
		Value: secret,
	}); err != nil {
		t.Fatalf("seed jwt_secret: %v", err)
	}
	svc, err := services.NewAuthService(mdb.ConfigRepository(), mdb, mdb.TokenRepository(), "admin", "password")
	if err != nil {
		t.Fatalf("create auth service: %v", err)
	}
	if err := mdb.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	handler := NewAuthHandler(svc)

	rr := doRefresh(handler, "any-token")
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("storage failure status = %d, want 500", rr.Code)
	}
}

// TestAuthHandler_RefreshToken_InvalidRequest 测试刷新token处理器 - 无效请求
func TestAuthHandler_RefreshToken_InvalidRequest(t *testing.T) {
	handler := newAuthHandlerForTest(t)

	req := httptest.NewRequest("POST", "/auth/refresh", bytes.NewBuffer([]byte("{invalid-json}")))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler.RefreshToken(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, rr.Code)
	}
}

// TestAuthHandler_Logout 测试登出处理器
func TestAuthHandler_Logout(t *testing.T) {
	handler := newAuthHandlerForTest(t)

	loginReq := models.LoginRequest{
		Username: "admin",
		Password: "password",
	}
	loginData, _ := json.Marshal(loginReq)
	loginRequest := httptest.NewRequest("POST", "/auth/login", bytes.NewBuffer(loginData))
	loginRequest.Header.Set("Content-Type", "application/json")
	loginRecorder := httptest.NewRecorder()
	handler.Login(loginRecorder, loginRequest)

	var loginResp models.LoginResponse
	json.Unmarshal(loginRecorder.Body.Bytes(), &loginResp)

	req := httptest.NewRequest("POST", "/auth/logout", nil)
	req.Header.Set("Authorization", "Bearer "+loginResp.AccessToken)
	req.Header.Set("X-Client-ID", "test-client")

	rr := httptest.NewRecorder()
	handler.Logout(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rr.Code)
	}
}

// TestAuthHandler_ListTokens 测试列出token处理器
func TestAuthHandler_ListTokens(t *testing.T) {
	handler := newAuthHandlerForTest(t)

	req := httptest.NewRequest("GET", "/auth/tokens", nil)
	req.Header.Set("Authorization", "Bearer test-access-token")

	rr := httptest.NewRecorder()
	handler.ListTokens(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rr.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp["limit"] != float64(20) {
		t.Errorf("Expected limit 20, got %v", resp["limit"])
	}
}
