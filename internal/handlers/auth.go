package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

	"songloft/internal/database"
	"songloft/internal/middleware"
	"songloft/internal/models"
	"songloft/internal/services"

	"github.com/go-chi/chi/v5"
)

// AuthHandler 认证处理器
type AuthHandler struct {
	authService *services.AuthService
}

// NewAuthHandler 创建认证处理器
func NewAuthHandler(authService *services.AuthService) *AuthHandler {
	return &AuthHandler{
		authService: authService,
	}
}

// Login 用户登录
// @Summary 用户登录
// @Description 用户登录获取访问令牌
// @Tags 认证管理
// @Accept json
// @Produce json
// @Param request body models.LoginRequest true "登录请求"
// @Success 200 {object} models.LoginResponse "登录成功"
// @Failure 400 {object} models.ErrorResponse "请求数据错误"
// @Failure 401 {object} models.ErrorResponse "用户名或密码错误"
// @Failure 500 {object} models.ErrorResponse "服务器错误"
// @Router /auth/login [post]
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req models.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "无效的请求数据", err)
		return
	}

	// 获取客户端信息
	clientInfo := r.UserAgent()
	if clientInfo == "" {
		clientInfo = r.RemoteAddr
	}

	// 执行登录
	resp, err := h.authService.Login(ctx, req.Username, req.Password, clientInfo)
	if err != nil {
		// 只有凭证错误才是 401；存储故障等必须是 500，不能伪装成凭证无效。
		if errors.Is(err, services.ErrInvalidCredentials) {
			respondError(w, http.StatusUnauthorized, "用户名或密码错误", err)
			return
		}
		respondError(w, http.StatusInternalServerError, "登录失败", err)
		return
	}

	respondJSON(w, http.StatusOK, resp)
}

// Logout 用户登出
// @Summary 用户登出
// @Description 用户登出，撤销当前访问令牌
// @Tags 认证管理
// @Accept json
// @Produce json
// @Success 200 {object} models.SuccessResponse "登出成功"
// @Failure 401 {object} models.ErrorResponse "未授权"
// @Failure 500 {object} models.ErrorResponse "服务器错误"
// @Security BearerAuth
// @Router /auth/logout [post]
func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 从请求上下文中获取当前客户端 ID（由 AuthMiddleware 设置）
	clientID := middleware.ClientIDFromContext(r.Context())

	// 获取当前访问令牌
	authHeader := r.Header.Get("Authorization")
	if len(authHeader) > 7 && authHeader[:7] == "Bearer " {
		accessToken := authHeader[7:]

		// 执行登出
		if err := h.authService.Logout(ctx, accessToken, clientID); err != nil {
			respondError(w, http.StatusInternalServerError, "登出失败", err)
			return
		}
	}

	respondJSON(w, http.StatusOK, models.SuccessResponse{
		Message: "登出成功",
	})
}

// RefreshToken 刷新令牌
// @Summary 刷新令牌
// @Description 使用刷新令牌获取新的访问令牌
// @Tags 认证管理
// @Accept json
// @Produce json
// @Param request body models.RefreshTokenRequest true "刷新令牌请求"
// @Success 200 {object} services.RefreshResponse "刷新成功"
// @Failure 400 {object} models.ErrorResponse "请求数据错误"
// @Failure 401 {object} models.ErrorResponse "刷新令牌无效"
// @Failure 500 {object} models.ErrorResponse "服务器错误"
// @Router /auth/refresh [post]
func (h *AuthHandler) RefreshToken(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req models.RefreshTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "无效的请求数据", err)
		return
	}

	// 获取客户端信息
	clientInfo := r.UserAgent()
	if clientInfo == "" {
		clientInfo = r.RemoteAddr
	}

	// 执行刷新令牌
	resp, err := h.authService.RefreshToken(ctx, req.RefreshToken, clientInfo)
	if err != nil {
		// 凭证类问题（无效/过期/撤销/已消费）返回 401 并给出明确原因；
		// 数据库等存储故障返回 500，客户端可原样重试。
		if services.IsRefreshCredentialError(err) {
			respondError(w, http.StatusUnauthorized, refreshErrorMessage(err), err)
			return
		}
		respondError(w, http.StatusInternalServerError, "刷新令牌失败", err)
		return
	}

	respondJSON(w, http.StatusOK, resp)
}

// refreshErrorMessage 把 refresh 凭证类错误映射为明确的中文提示，
// 复用（已消费）必须让客户端明确知道令牌已被使用而不是笼统的无效。
func refreshErrorMessage(err error) string {
	switch {
	case errors.Is(err, services.ErrRefreshTokenConsumed):
		return "刷新令牌已被使用，请重新登录"
	case errors.Is(err, services.ErrRefreshTokenExpired):
		return "刷新令牌已过期，请重新登录"
	case errors.Is(err, services.ErrRefreshTokenRevoked):
		return "刷新令牌已被撤销，请重新登录"
	default:
		return "刷新令牌无效"
	}
}

// ListTokens 列出活跃令牌
// @Summary 列出活跃令牌
// @Description 获取当前用户的所有活跃令牌列表
// @Tags 认证管理
// @Accept json
// @Produce json
// @Param type query string false "令牌类型" Enums(access, refresh)
// @Param limit query int false "每页数量" default(20)
// @Param offset query int false "偏移量" default(0)
// @Success 200 {object} map[string]interface{} "令牌列表"
// @Failure 401 {object} models.ErrorResponse "未授权"
// @Failure 500 {object} models.ErrorResponse "服务器错误"
// @Security BearerAuth
// @Router /auth/tokens [get]
func (h *AuthHandler) ListTokens(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 解析查询参数
	tokenType := r.URL.Query().Get("type")
	limit, offset := parsePagination(r, models.DefaultPaginationLimit, 0)

	// 构建过滤条件
	filter := &database.TokenFilter{
		TokenType: tokenType,
		Limit:     limit,
		Offset:    offset,
		OrderBy:   "created_at",
		Order:     "DESC",
	}

	// 获取令牌列表
	tokens, err := h.authService.ListActiveTokens(ctx, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "获取令牌列表失败", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"tokens": tokens,
		"total":  len(tokens),
		"limit":  limit,
		"offset": offset,
	})
}

// RevokeToken 撤销令牌
// @Summary 撤销令牌
// @Description 撤销指定的令牌
// @Tags 认证管理
// @Accept json
// @Produce json
// @Param token_id path string true "令牌ID"
// @Param request body models.RevokeTokenRequest true "撤销令牌请求"
// @Success 200 {object} models.SuccessResponse "撤销成功"
// @Failure 400 {object} models.ErrorResponse "请求数据错误"
// @Failure 401 {object} models.ErrorResponse "未授权"
// @Failure 500 {object} models.ErrorResponse "服务器错误"
// @Security BearerAuth
// @Router /auth/tokens/{token_id} [delete]
func (h *AuthHandler) RevokeToken(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tokenID := chi.URLParam(r, "token_id")

	var req models.RevokeTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "无效的请求数据", err)
		return
	}

	// 获取客户端信息作为撤销者
	revokedBy := middleware.ClientIDFromContext(r.Context())
	if revokedBy == "" {
		revokedBy = "unknown"
	}

	// 执行撤销令牌
	if err := h.authService.RevokeToken(ctx, tokenID, revokedBy, req.Reason); err != nil {
		respondError(w, http.StatusInternalServerError, "撤销令牌失败", err)
		return
	}

	respondJSON(w, http.StatusOK, models.SuccessResponse{
		Message: "令牌已撤销",
	})
}
