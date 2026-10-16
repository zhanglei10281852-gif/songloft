package jsplugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// 删除运行态阶段的超时预算。
const (
	// pluginDrainTimeout 等待插件已排队/处理中的 HTTP 请求、设备事件等消息
	// 排空的上限。插件最慢的合法调用可达 300s，但删除是管理员显式动作，过长的
	// 在途调用宁可让本次删除失败可重试，也不能把 HTTP 请求挂死——超时后服务
	// 恢复接客，调用方收到明确错误。
	pluginDrainTimeout = 10 * time.Second
	// pluginUnregisterTimeout 排空成功后等待调度 worker 退出的上限。
	// 排空已确认队列归零，正常情况下立即完成。
	pluginUnregisterTimeout = 5 * time.Second
)

// ErrPluginDeleting 插件删除流程正在进行：新的请求、懒加载、健康恢复、
// 热更新与自动更新都不得再进入该插件。
var ErrPluginDeleting = errors.New("jsplugin: plugin is being deleted")

// IsPluginUninstallingError 判断错误是否为「删除中」或「删除未收敛」。
func IsPluginUninstallingError(err error) bool {
	return errors.Is(err, ErrPluginDeleting) || errors.Is(err, ErrPluginUninstalling)
}

// DeletePlugin 删除一个 JS 插件，两阶段执行：
//
//	阶段 1（运行态收敛，全部失败都中止且不动磁盘/DB）：
//	  a. 标记 deleting —— 立即阻断新的 HTTP 请求、懒加载、健康恢复/唤醒、
//	     热更新与自动更新；
//	  b. DrainService 排空已排队与运行中的调用（超时则回滚放开、返回错误，
//	     插件继续服务）；
//	  c. 注销调度器、停止服务（定时器、onDeinit、后台进程、UDP/TCP/WebSocket、
//	     脚本环境）；任一失败返回可观察错误，绝不进入破坏性阶段；
//	  d. 撤销播放订阅、歌词/封面提供者、健康检查计划。
//	阶段 2（持久收敛，见 PackageManager.Uninstall）：
//	  先落盘删除墓碑，再删 ZIP → 数据目录（keep_data=true 保留）→ DB 记录 →
//	  墓碑。部分失败或进程崩溃由下次启动的 RecoverPendingUninstalls 继续收敛。
//
// 成功后 publicPaths 立即刷新；同一 entryPath 可正常重新安装。
// 返回被删插件元数据，供调用方清理宿主侧关联配置（tab/排序等）。
func (m *Manager) DeletePlugin(ctx context.Context, pluginID int64, keepData bool) (*JSPlugin, error) {
	plugin, err := m.repo.GetByID(ctx, pluginID)
	if err != nil {
		return nil, fmt.Errorf("get plugin by id %d: %w", pluginID, err)
	}
	entryPath := plugin.EntryPath

	// a. 阻断新入口。LoadOrStore 保证同一 entryPath 同时只有一个删除流程。
	if _, exists := m.deleting.LoadOrStore(entryPath, struct{}{}); exists {
		return nil, fmt.Errorf("%w: %s", ErrPluginDeleting, entryPath)
	}
	// 无论阶段 2 成败都放开内存阻断：阶段 2 失败时记录仍在、墓碑保证删除意图
	// 不丢（安装/更新/扫描被墓碑拦截），管理员可用同一 ID 重试 DELETE；
	// 记录已删时插件本身已不存在，标记留着只会挡住同 entryPath 的重新安装。
	defer m.deleting.Delete(entryPath)

	// b+c. 排空 + 注销 + 停服。失败即中止，包与 DB 记录原样保留。
	if err := m.quiesceForDelete(plugin); err != nil {
		return nil, err
	}

	// d. 撤销宿主侧运行态注册。
	m.playEventSubscribers.Delete(entryPath)
	m.lyricProviders.Delete(entryPath)
	m.coverProviders.Delete(entryPath)
	if m.healthChecker != nil {
		m.healthChecker.Forget(entryPath)
	}
	// 清掉自动更新里「已落盘但推迟重载」的待办，避免重试周期复活插件。
	if m.autoUpdater != nil {
		m.autoUpdater.ForgetPending(entryPath)
	}

	slog.Info("js plugin runtime quiesced for delete",
		"entryPath", entryPath, "keepData", keepData)

	// 阶段 2：墓碑 + ZIP/数据目录/DB 记录的幂等收敛。
	if err := m.packager.Uninstall(pluginID, keepData); err != nil {
		return nil, fmt.Errorf("uninstall plugin %q: %w", entryPath, err)
	}

	// DB 记录已删除，重建 publicPaths 缓存，公开路由立即失效。
	m.RefreshPublicPaths()

	slog.Info("js plugin deleted", "entryPath", entryPath, "keepData", keepData)
	return plugin, nil
}

// quiesceForDelete 在 m.mu 保护下完成删除的运行态收敛：
// 冻结入口 → 排空在途消息 → 注销调度器 → 停服 → 移出 services map。
//
// 任何步骤失败都返回错误，调用方据此中止破坏性删除；此时插件不会处于
// 「调度器已摘但 VM 还在跑」的半卸载状态——排空超时会恢复接客，注销/停服
// 失败则把已停的 service 移出 map，下次请求经懒加载完整重建。
func (m *Manager) quiesceForDelete(plugin *JSPlugin) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entryPath := plugin.EntryPath

	value, ok := m.services.Load(entryPath)
	if !ok {
		// 插件未加载（空闲驱逐/error/inactive）：回收可能残留的孤儿 JS 环境。
		// 它们已无调度器入口、不可能执行消息，销毁失败仅告警，不阻断删除。
		if err := m.jsManager.DestroyPluginEnvs(plugin.ID); err != nil {
			slog.Warn("cleanup stale plugin envs during delete failed",
				"entryPath", entryPath, "error", err)
		}
		return nil
	}
	service := value.(*JSService)

	// 1) 冻结并排空：等待已排队/运行中的调用结束。超时则 unfreeze 回滚，
	//    注册表与 worker 原封不动，插件继续正常服务。
	drainTimeout := m.drainTimeout
	if drainTimeout <= 0 {
		drainTimeout = pluginDrainTimeout
	}
	if err := m.scheduler.DrainService(entryPath, drainTimeout); err != nil {
		return fmt.Errorf("drain plugin calls before delete: %w", err)
	}

	// 2) 摘调度器并等待 worker 退出。排空已确认在途归零，这里不应触发超时；
	//    万一失败（worker 被强制取消），把已停的 service 移出 map，保证不会
	//    留下「还在跑却删了包」的状态，然后中止本次删除。
	unregisterTimeout := m.unregisterTimeout
	if unregisterTimeout <= 0 {
		unregisterTimeout = pluginUnregisterTimeout
	}
	if err := m.scheduler.UnregisterService(entryPath, unregisterTimeout); err != nil {
		m.services.Delete(entryPath)
		return fmt.Errorf("unregister plugin scheduler: %w", err)
	}

	// 3) 停服：定时器处理器、onDeinit、后台进程、UDP/TCP socket、入站
	//    WebSocket 与全部脚本环境。Stop 内部每一步都会执行，错误向上冒泡。
	if err := service.Stop(); err != nil {
		m.services.Delete(entryPath)
		return fmt.Errorf("stop plugin service: %w", err)
	}

	m.services.Delete(entryPath)
	return nil
}

// markDeleting 仅供包内测试/流程复用；正常入口是 DeletePlugin。
func (m *Manager) markDeleting(entryPath string) bool {
	_, exists := m.deleting.LoadOrStore(entryPath, struct{}{})
	return !exists
}

// clearDeleting 放开删除阻断（仅供测试/回滚使用）。
func (m *Manager) clearDeleting(entryPath string) {
	m.deleting.Delete(entryPath)
}

// isDeleting 报告插件是否处于删除流程中（阶段 1 与阶段 2 期间均为 true）。
func (m *Manager) isDeleting(entryPath string) bool {
	_, ok := m.deleting.Load(entryPath)
	return ok
}

// IsPluginDeleting 对外暴露的删除中判定，供 handler 在上传/更新等路径
// 提前给出 409，避免等包管理器落盘阶段才失败。
func (m *Manager) IsPluginDeleting(entryPath string) bool {
	return m.isDeleting(entryPath)
}
