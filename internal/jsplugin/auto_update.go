package jsplugin

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// pluginAutoUpdateConfigKey 自动更新开关配置键（与 handlers 层保持一致）。
const pluginAutoUpdateConfigKey = "plugin_auto_update"

// githubProxyConfigKey GitHub 代理地址配置键（与 handlers/upgrade.go 保持一致）。
const githubProxyConfigKey = "github_proxy"

const (
	// autoUpdateInitialDelay 服务启动后首次自动更新检查的延迟。
	autoUpdateInitialDelay = 5 * time.Minute
	// autoUpdateInterval 自动更新检查的固定周期。
	autoUpdateInterval = 6 * time.Hour
	// autoUpdateReloadRetryInterval 因插件忙而推迟的热重载的重试周期。
	// 取远小于 autoUpdateInterval 的值：新版本已经落盘了，不该等下一个 6 小时周期才生效。
	autoUpdateReloadRetryInterval = 10 * time.Minute
)

// UpdateAllOptions 控制 RunUpdateAll 的行为。
type UpdateAllOptions struct {
	// Force 为 true 时强制重新下载安装（跳过"已是最新版"判断）。
	Force bool
	// DeferReloadWhenBusy 为 true 时，若插件报告自己正忙（onQueryBusy），
	// 就跳过本次热重载——新版本 zip 已落盘，由调用方在插件空闲后补做。
	//
	// 只给后台自动更新用。手动更新**不能**开：用户点了"更新"就是希望立刻生效，
	// 悄悄推迟会让人以为更新没成功。
	DeferReloadWhenBusy bool
}

// PluginUpdateResult 单个插件的更新结果。
type PluginUpdateResult struct {
	PluginID       int64
	PluginName     string
	EntryPath      string
	Success        bool
	HasUpdate      bool
	CurrentVersion string
	NewVersion     string
	Error          string
	// ReloadDeferred 表示新版本已安装但热重载被推迟（插件当时正忙）。
	ReloadDeferred bool
}

// UpdateAllResult 批量更新的聚合结果。
type UpdateAllResult struct {
	Total   int
	Updated int
	Failed  int
	Skipped int
	// ReloadDeferred 本次有多少插件的热重载被推迟。
	ReloadDeferred int
	// PendingReload 被推迟重载的插件 entryPath，供调用方在插件空闲后补做重载。
	PendingReload []string
	Results       []PluginUpdateResult
}

// RunUpdateAll 检查并更新所有具有远程更新源的插件。
//   - 跳过无 UpdateURL 的插件
//   - force=false 时跳过已是最新版的插件；force=true 强制重新下载安装
//   - 单个插件失败不中断其他插件
//   - active 插件更新后自动热重载
//
// 该方法同时被批量更新 HTTP 端点与后台自动更新 ticker 复用。
func (m *Manager) RunUpdateAll(ctx context.Context, githubProxy string, force bool) (UpdateAllResult, error) {
	return m.RunUpdateAllWithOptions(ctx, githubProxy, UpdateAllOptions{Force: force})
}

// RunUpdateAllWithOptions 是 RunUpdateAll 的可配置版本，语义见 UpdateAllOptions。
func (m *Manager) RunUpdateAllWithOptions(ctx context.Context, githubProxy string, opts UpdateAllOptions) (UpdateAllResult, error) {
	force := opts.Force
	plugins, err := m.repo.GetAll(ctx)
	if err != nil {
		return UpdateAllResult{}, fmt.Errorf("get all js plugins: %w", err)
	}

	result := UpdateAllResult{Total: len(plugins)}

	for _, plugin := range plugins {
		item := PluginUpdateResult{
			PluginID:       plugin.ID,
			PluginName:     plugin.Name,
			EntryPath:      plugin.EntryPath,
			CurrentVersion: plugin.Version,
		}

		// 删除流程中的插件跳过：不检查更新、不下载覆盖 ZIP、不触发重载，
		// 避免与墓碑收敛竞争。
		if m.isDeleting(plugin.EntryPath) {
			result.Skipped++
			result.Results = append(result.Results, item)
			continue
		}

		if plugin.UpdateURL == "" {
			result.Skipped++
			result.Results = append(result.Results, item)
			continue
		}

		updateInfo, err := m.packager.CheckUpdate(plugin.ID, githubProxy)
		if err != nil {
			item.Error = fmt.Sprintf("检查更新失败: %v", err)
			result.Failed++
			result.Results = append(result.Results, item)
			continue
		}

		if !force && !updateInfo.HasUpdate {
			result.Skipped++
			result.Results = append(result.Results, item)
			continue
		}

		item.HasUpdate = true

		updatedPlugin, err := m.packager.DownloadUpdate(plugin.ID, githubProxy, force)
		if err != nil {
			item.Error = fmt.Sprintf("下载更新失败: %v", err)
			result.Failed++
			result.Results = append(result.Results, item)
			continue
		}

		if updatedPlugin.Status == JSPluginStatusActive {
			// 热重载会销毁整个 JS 环境，插件维护在内存里的状态和定时器一起消失。
			// miot 正在放歌时被重载就表现为"播着播着突然停了"
			// （songloft-org/songloft-plugin-miot#96）。后台自动更新因此先问一句忙不忙，
			// 忙就把重载记成待办，等空闲了补做；新版本 zip 此刻已经落盘，不会丢。
			if opts.DeferReloadWhenBusy {
				if busy, reason := m.IsPluginBusy(updatedPlugin.EntryPath); busy {
					slog.Info("plugin busy, defer reload after update",
						"entryPath", updatedPlugin.EntryPath,
						"newVersion", updatedPlugin.Version,
						"reason", reason)
					item.ReloadDeferred = true
					result.ReloadDeferred++
					result.PendingReload = append(result.PendingReload, updatedPlugin.EntryPath)
				}
			}
			if !item.ReloadDeferred {
				reloadBaseCtx := ctx
				if !opts.DeferReloadWhenBusy {
					// 手动批量更新下载完成后，即使客户端断开也要让新版本生效。
					// 后台自动更新仍保留停机取消语义。
					reloadBaseCtx = context.WithoutCancel(ctx)
				}
				reloadCtx, cancel := context.WithTimeout(reloadBaseCtx, 30*time.Second)
				if reloadErr := m.ReloadPlugin(reloadCtx, updatedPlugin.EntryPath); reloadErr != nil {
					slog.Warn("reload plugin after update failed", "entryPath", updatedPlugin.EntryPath, "error", reloadErr)
				}
				cancel()
			}
		}

		item.Success = true
		item.NewVersion = updatedPlugin.Version
		result.Updated++
		result.Results = append(result.Results, item)
	}

	return result, nil
}

// AutoUpdater 后台定时自动更新插件的调度器。
// 复刻 HotReloader.WatchForChanges 的 ticker 模式：启动后延迟首跑，之后按固定周期触发；
// 每次触发前读取 plugin_auto_update 开关，关闭则跳过。
type AutoUpdater struct {
	manager *Manager
	// pendingReload 记录"新版本已落盘、但当时插件正忙所以没重载"的插件 entryPath。
	// 由 Run 的 goroutine（runOnce / retryPendingReloads）读写；插件删除流程
	// 会从 HTTP 请求 goroutine 调 ForgetPending，因此用 pendingMu 保护。
	pendingReload map[string]struct{}
	pendingMu     sync.Mutex
}

// NewAutoUpdater 创建自动更新调度器。
func NewAutoUpdater(m *Manager) *AutoUpdater {
	return &AutoUpdater{manager: m, pendingReload: make(map[string]struct{})}
}

// ForgetPending 清除该插件被推迟的重载待办。插件删除成功后调用，
// 避免下一个重试周期对着已删除的记录刷错误日志。
func (a *AutoUpdater) ForgetPending(entryPath string) {
	if a == nil {
		return
	}
	a.pendingMu.Lock()
	delete(a.pendingReload, entryPath)
	a.pendingMu.Unlock()
}

// Run 阻塞运行自动更新循环，直到 ctx 取消。应在独立 goroutine 中调用。
func (a *AutoUpdater) Run(ctx context.Context) {
	timer := time.NewTimer(autoUpdateInitialDelay)
	defer timer.Stop()

	// 首次延迟触发，避免与启动阶段的插件加载/同步争抢。
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		a.runOnce(ctx)
	}

	ticker := time.NewTicker(autoUpdateInterval)
	defer ticker.Stop()
	// 被推迟的重载单独用一个短周期 ticker 补做，不必等下一个 6 小时检查周期。
	retryTicker := time.NewTicker(autoUpdateReloadRetryInterval)
	defer retryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.runOnce(ctx)
		case <-retryTicker.C:
			a.retryPendingReloads(ctx)
		}
	}
}

// retryPendingReloads 重试因插件忙而推迟的热重载：仍然忙就继续等下一个周期。
func (a *AutoUpdater) retryPendingReloads(ctx context.Context) {
	m := a.manager

	a.pendingMu.Lock()
	pending := make([]string, 0, len(a.pendingReload))
	for entryPath := range a.pendingReload {
		pending = append(pending, entryPath)
	}
	a.pendingMu.Unlock()

	for _, entryPath := range pending {
		if busy, reason := m.IsPluginBusy(entryPath); busy {
			slog.Debug("deferred reload still pending, plugin busy", "entryPath", entryPath, "reason", reason)
			continue
		}

		if err := m.ReloadPlugin(ctx, entryPath); err != nil {
			slog.Warn("deferred reload after update failed", "entryPath", entryPath, "error", err)
		} else {
			slog.Info("deferred reload after update completed", "entryPath", entryPath)
		}
		// 成功失败都出队：失败通常是插件本身的问题，每 10 分钟重试只会刷日志。
		// 交给 HealthChecker 的自愈与下一轮自动更新处理。删除中的插件也在此
		// 出队（DeletePlugin 的 ForgetPending 通常已先一步清掉）。
		a.pendingMu.Lock()
		delete(a.pendingReload, entryPath)
		a.pendingMu.Unlock()
	}
}

// runOnce 执行一次自动更新检查（若开关开启）。
func (a *AutoUpdater) runOnce(ctx context.Context) {
	m := a.manager
	if m.configService == nil {
		return
	}
	if !m.configService.GetBool(pluginAutoUpdateConfigKey, false) {
		return
	}

	// github_proxy 由业务端点以 JSON {"proxy":"..."} 形式存储（handlers/upgrade.go
	// 的 UpdateGithubProxySetting via SetJSON），必须用 GetJSON 解析。旧代码用
	// GetString 会把整个 JSON 串当作代理前缀，导致自动更新走代理时 URL 拼接错误。
	var proxyCfg struct {
		Proxy string `json:"proxy"`
	}
	_ = m.configService.GetJSON(githubProxyConfigKey, &proxyCfg)
	// 自动更新是后台行为，用户无感知：正在播放时热重载会打断播放，
	// 所以推迟重载，等插件空闲后由 retryPendingReloads 补做。
	// 手动更新（用户主动点击）不走这个分支，仍然立即重载。
	result, err := m.RunUpdateAllWithOptions(ctx, proxyCfg.Proxy, UpdateAllOptions{DeferReloadWhenBusy: true})
	if err != nil {
		slog.Warn("auto-update run failed", "error", err)
		return
	}
	a.pendingMu.Lock()
	for _, entryPath := range result.PendingReload {
		a.pendingReload[entryPath] = struct{}{}
	}
	a.pendingMu.Unlock()
	slog.Info("auto-update completed",
		"total", result.Total,
		"updated", result.Updated,
		"failed", result.Failed,
		"skipped", result.Skipped,
		"reloadDeferred", result.ReloadDeferred)
}
