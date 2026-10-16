package handlers

import (
	"encoding/json"
	"net/http"

	"songloft/internal/database"
	"songloft/internal/models"
	"songloft/internal/services"

	"github.com/go-chi/chi/v5"
)

// ConfigHandler 配置处理器
type ConfigHandler struct {
	configService   *services.ConfigService
	pluginRepo      tabConfigPluginSource // 插件安装状态（tab-config 的孤儿清理与限额统计用）
	onConfigChanged func(key string)      // 配置变更回调（可选，落库后异步触发）
	// beforeConfigChange 落库前拦截（可选）：返回 handled=true 表示该 key 由钩子以
	// 自定义语义完成「落库 + 副作用」（如 music_path 的配置发布边界），handler 跳过
	// 默认落库与 onConfigChanged；err 非零时按 500 返回，配置不发生变更。
	beforeConfigChange func(key, value string) (handled bool, err error)
}

// NewConfigHandler 创建配置处理器
func NewConfigHandler(configService *services.ConfigService, pluginRepo tabConfigPluginSource) *ConfigHandler {
	return &ConfigHandler{
		configService: configService,
		pluginRepo:    pluginRepo,
	}
}

// SetOnConfigChanged 设置配置变更回调
func (h *ConfigHandler) SetOnConfigChanged(callback func(key string)) {
	h.onConfigChanged = callback
}

// SetBeforeConfigChange 设置落库前拦截钩子（见 beforeConfigChange 字段说明）。
func (h *ConfigHandler) SetBeforeConfigChange(hook func(key, value string) (bool, error)) {
	h.beforeConfigChange = hook
}

// ListConfigs 获取配置列表
// @Summary 获取配置列表
// @Description 获取配置列表，支持关键词搜索和分页
// @Tags 配置管理
// @Accept json
// @Produce json
// @Param keyword query string false "搜索关键词"
// @Param limit query int false "每页数量" default(20)
// @Param offset query int false "偏移量" default(0)
// @Success 200 {object} map[string]interface{} "成功返回配置列表"
// @Failure 500 {object} map[string]string "服务器错误"
// @Security BearerAuth
// @Router /configs [get]
func (h *ConfigHandler) ListConfigs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 解析查询参数
	keyword := r.URL.Query().Get("keyword")
	limit, offset := parsePagination(r, models.DefaultPaginationLimit, 0)

	// 构建过滤条件
	filter := &database.ConfigFilter{
		Keyword: keyword,
		Limit:   limit,
		Offset:  offset,
		OrderBy: "key",
		Order:   "ASC",
	}

	// 获取配置列表
	configs, err := h.configService.ListConfigs(ctx, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "获取配置列表失败", err)
		return
	}

	// 获取总数
	total, err := h.configService.CountConfigs(ctx, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "获取配置总数失败", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"configs": configs,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

// GetConfig 获取单个配置
// @Summary 获取单个配置详情
// @Description 根据配置键获取详细信息
// @Tags 配置管理
// @Accept json
// @Produce json
// @Param key path string true "配置键"
// @Success 200 {object} models.Config "成功返回配置详情"
// @Failure 404 {object} map[string]string "配置不存在"
// @Security BearerAuth
// @Router /configs/{key} [get]
func (h *ConfigHandler) GetConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	key := chi.URLParam(r, "key")

	if key == "" {
		respondError(w, http.StatusBadRequest, "配置键不能为空", nil)
		return
	}

	config, err := h.configService.GetConfig(ctx, key)
	if err != nil {
		respondError(w, http.StatusNotFound, "配置不存在", err)
		return
	}

	respondJSON(w, http.StatusOK, config)
}

// CreateConfig 创建配置
// @Summary 创建配置
// @Description 创建一个新的配置
// @Tags 配置管理
// @Accept json
// @Produce json
// @Param request body models.CreateConfigRequest true "配置信息"
// @Success 201 {object} models.Config "创建成功"
// @Failure 400 {object} map[string]string "请求数据错误"
// @Failure 500 {object} map[string]string "创建失败"
// @Security BearerAuth
// @Router /configs [post]
func (h *ConfigHandler) CreateConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req models.CreateConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "无效的请求数据", err)
		return
	}

	// 验证请求数据
	if req.Key == "" {
		respondError(w, http.StatusBadRequest, "配置键不能为空", nil)
		return
	}
	if req.Value == "" {
		respondError(w, http.StatusBadRequest, "配置值不能为空", nil)
		return
	}

	// 创建配置
	config := &models.Config{
		Key:   req.Key,
		Value: req.Value,
	}

	if err := h.configService.CreateConfig(ctx, config); err != nil {
		respondError(w, http.StatusInternalServerError, "创建配置失败", err)
		return
	}

	respondJSON(w, http.StatusCreated, config)
}

// UpdateConfig 更新配置
// @Summary 更新配置
// @Description 更新配置信息
// @Tags 配置管理
// @Accept json
// @Produce json
// @Param key path string true "配置键"
// @Param request body models.UpdateConfigRequest true "配置信息"
// @Success 200 {object} models.Config "更新成功"
// @Failure 400 {object} map[string]string "请求数据错误"
// @Failure 404 {object} map[string]string "配置不存在"
// @Security BearerAuth
// @Router /configs/{key} [put]
func (h *ConfigHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	key := chi.URLParam(r, "key")

	if key == "" {
		respondError(w, http.StatusBadRequest, "配置键不能为空", nil)
		return
	}

	// 检查配置是否存在
	_, err := h.configService.GetConfig(ctx, key)
	if err != nil {
		respondError(w, http.StatusNotFound, "配置不存在", err)
		return
	}

	var req models.UpdateConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "无效的请求数据", err)
		return
	}

	// 验证请求数据
	if req.Value == "" {
		respondError(w, http.StatusBadRequest, "配置值不能为空", nil)
		return
	}

	// 落库前拦截：music_path 等需要发布边界语义的 key 由钩子同步完成
	// 「停旧扫描 → 清理 → 落库 → 换 Scanner」，成功或失败都在此处对客户端可观察。
	if h.beforeConfigChange != nil {
		handled, hookErr := h.beforeConfigChange(key, req.Value)
		if hookErr != nil {
			respondError(w, http.StatusInternalServerError, "应用配置失败，已保留旧配置", hookErr)
			return
		}
		if handled {
			respondJSON(w, http.StatusOK, &models.Config{Key: key, Value: req.Value})
			return
		}
	}

	// 更新配置
	config := &models.Config{
		Key:   key,
		Value: req.Value,
	}

	if err := h.configService.UpdateConfig(ctx, config); err != nil {
		respondError(w, http.StatusInternalServerError, "更新配置失败", err)
		return
	}

	// 触发配置变更回调
	if h.onConfigChanged != nil {
		go h.onConfigChanged(key)
	}

	respondJSON(w, http.StatusOK, config)
}

// DeleteConfig 删除配置
// @Summary 删除配置
// @Description 根据配置键删除配置
// @Tags 配置管理
// @Accept json
// @Produce json
// @Param key path string true "配置键"
// @Success 200 {object} map[string]string "删除成功"
// @Failure 400 {object} map[string]string "无效的配置键"
// @Failure 500 {object} map[string]string "删除失败"
// @Security BearerAuth
// @Router /configs/{key} [delete]
func (h *ConfigHandler) DeleteConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	key := chi.URLParam(r, "key")

	if key == "" {
		respondError(w, http.StatusBadRequest, "配置键不能为空", nil)
		return
	}

	if err := h.configService.DeleteConfig(ctx, key); err != nil {
		respondError(w, http.StatusInternalServerError, "删除配置失败", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": "配置已删除",
	})
}
