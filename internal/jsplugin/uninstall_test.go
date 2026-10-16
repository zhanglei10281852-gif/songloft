package jsplugin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"songloft/internal/database"
)

// blockingHandler 用 gate 精确控制消息处理时机，用于调度器排空测试。
type blockingHandler struct {
	gate    chan struct{} // 关闭后 HandleMessage 才放行
	mu      sync.Mutex
	count   int
	hasGate chan struct{} // 每次开始处理时发送一个信号（缓冲 1）
}

func newBlockingHandler() *blockingHandler {
	return &blockingHandler{gate: make(chan struct{}), hasGate: make(chan struct{}, 64)}
}

func (h *blockingHandler) HandleMessage(msg *Message) *Message {
	h.hasGate <- struct{}{}
	<-h.gate
	h.mu.Lock()
	h.count++
	h.mu.Unlock()
	return &Message{ID: msg.ID, Session: msg.Session, Data: "ok"}
}

func (h *blockingHandler) calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// TestScheduler_DrainService_WaitsInflight 验证排空等待在途消息全部处理完才返回，
// 排空后新消息被拒绝。
func TestScheduler_DrainService_WaitsInflight(t *testing.T) {
	sched := NewServiceScheduler(1)
	defer sched.Close()

	h := newBlockingHandler()
	if err := sched.RegisterService("svc", h, 16); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 1 个在途同步调用 + 3 个排队消息，全部被 gate 挡住。
	callDone := make(chan error, 1)
	go func() {
		_, err := sched.Call(context.Background(), "svc", "", MsgHTTPRequest, "x", 5*time.Second)
		callDone <- err
	}()
	<-h.hasGate // 等第一条真正开始处理，确保它在排空开始前已在途

	for range 3 {
		if err := sched.Send(&Message{Type: MsgLifecycle, Target: "svc"}); err != nil {
			t.Fatalf("send queued: %v", err)
		}
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- sched.DrainService("svc", 3*time.Second) }()

	// 排空开始与本 goroutine 无同步：freeze 之前探测消息可能被正常接收（计数），
	// freeze 之后必须收到 ErrServiceUnregistered。
	probeAccepted := 0
	deadline := time.After(1 * time.Second)
	rejected := false
	for !rejected {
		select {
		case <-deadline:
			t.Fatal("drain 期间新消息仍可入队，入口未冻结")
		default:
		}
		if err := sched.Send(&Message{Type: MsgLifecycle, Target: "svc"}); err != nil {
			if errors.Is(err, ErrServiceUnregistered) {
				rejected = true
				break
			}
			t.Fatalf("drain 期间意外错误: %v", err)
		}
		probeAccepted++
		time.Sleep(2 * time.Millisecond)
	}

	// gate 未放行前排空不能成功。
	select {
	case err := <-drainDone:
		t.Fatalf("仍有在途消息时 DrainService 却返回了: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(h.gate)

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("DrainService 应在全部消息处理完后成功: %v", err)
		}
	case <-time.After(3*time.Second + 100*time.Millisecond):
		t.Fatal("DrainService 超时")
	}

	select {
	case err := <-callDone:
		if err != nil {
			t.Fatalf("在途调用应正常拿到响应: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("在途调用未返回")
	}

	if got, want := h.calls(), 4+probeAccepted; got != want {
		t.Errorf("排空应处理完 1 调用+3 排队+%d 探测 = %d 条消息，实际 %d",
			probeAccepted, want, got)
	}

	// 排空成功后摘注册（关闭队列，worker 应立刻退出）。
	if err := sched.UnregisterService("svc", time.Second); err != nil {
		t.Fatalf("unregister after drain: %v", err)
	}
}

// TestScheduler_DrainService_TimeoutRollback 验证排空超时后服务恢复接客，
// 调用方可以据此中止卸载。
func TestScheduler_DrainService_TimeoutRollback(t *testing.T) {
	sched := NewServiceScheduler(1)
	defer sched.Close()

	h := newBlockingHandler()
	if err := sched.RegisterService("svc", h, 16); err != nil {
		t.Fatalf("register: %v", err)
	}

	go func() {
		_, _ = sched.Call(context.Background(), "svc", "", MsgHTTPRequest, "x", 5*time.Second)
	}()
	<-h.hasGate

	start := time.Now()
	err := sched.DrainService("svc", 80*time.Millisecond)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("期望 ErrDrainTimeout，got %v", err)
	}
	if d := time.Since(start); d > 800*time.Millisecond {
		t.Errorf("排空超时应在 ~80ms 返回，实际 %v", d)
	}

	// 回滚后放行长任务，服务应能继续处理新消息。
	close(h.gate)
	deadline := time.After(3 * time.Second)
	for h.calls() < 1 {
		select {
		case <-deadline:
			t.Fatal("在途消息未处理完")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	if err := sched.Send(&Message{Type: MsgLifecycle, Target: "svc"}); err != nil {
		t.Fatalf("排空超时回滚后服务应恢复接客，Send 失败: %v", err)
	}
	deadline = time.After(3 * time.Second)
	for h.calls() < 2 {
		select {
		case <-deadline:
			t.Fatal("回滚后新消息未被处理")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// slowPluginJS 提供一个 ~1.2s 的慢端点与即时端点。
const slowPluginJS = `
function onInit() {}
function onDeinit() {}
async function onHTTPRequest(req) {
    if (req.path === "/slow") {
        await new Promise(function(r) { setTimeout(r, 1200); });
        return {statusCode: 200, headers: {"Content-Type": "application/json"}, body: '{"ok":"slow"}'};
    }
    return {statusCode: 200, headers: {"Content-Type": "application/json"}, body: '{"hello":"world"}'};
}
`

// setupLoadedPlugin 安装并加载一个可用的测试插件，返回 manager 与记录。
func setupLoadedPlugin(t *testing.T, entryPath, jsCode string, publicPaths []string) (*Manager, *JSPlugin, string, string) {
	t.Helper()

	pluginsDir, dataDir, repo, _ := setupTestEnv(t)
	manifest := testManifest(entryPath)
	manifest.PublicPaths = publicPaths
	zipData := createTestPluginZipWithStatic(t, manifest, jsCode, map[string]string{
		"index.html": "<head></head><body>plugin page</body>",
	})
	plugin, _, err := NewPackageManager(pluginsDir, dataDir, repo).InstallFromUpload(zipData)
	if err != nil {
		t.Skipf("InstallFromUpload failed (may need QuickJS toolchain): %v", err)
	}

	manager := NewManager(repo, pluginsDir, dataDir, "", nil, nil)
	t.Cleanup(func() { manager.Close() })

	if err := manager.EnablePlugin(context.Background(), plugin.ID); err != nil {
		t.Skipf("EnablePlugin failed (may need QuickJS runtime): %v", err)
	}
	if _, ok := manager.GetService(entryPath); !ok {
		t.Skip("plugin service not loaded, skip QuickJS-dependent test")
	}
	return manager, plugin, pluginsDir, dataDir
}

func pluginPing(t *testing.T, m *Manager, entryPath string) {
	t.Helper()
	status, _, body, err := m.InvokeHTTP(context.Background(), entryPath, "GET", "/ping", nil, nil)
	if err != nil {
		t.Fatalf("InvokeHTTP /ping: %v", err)
	}
	if status != 200 {
		t.Fatalf("期望 200，got %d body=%s", status, string(body))
	}
}

func assertPluginGone(t *testing.T, m *Manager, pm *PackageManager, plugin *JSPlugin, pluginsDir, dataDir string, keepData bool) {
	t.Helper()
	entryPath := plugin.EntryPath

	if _, err := os.Stat(filepath.Join(pluginsDir, plugin.FilePath)); !os.IsNotExist(err) {
		t.Errorf("ZIP 应已删除: %v", err)
	}
	if _, err := m.repo.GetByEntryPath(context.Background(), entryPath); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("DB 记录应已删除，got err=%v", err)
	}
	if _, ok := m.GetService(entryPath); ok {
		t.Error("services map 不应再有该插件")
	}
	if m.scheduler.HasService(entryPath) {
		t.Error("scheduler 不应再有该插件")
	}
	_, lyricOK := m.lyricProviders.Load(entryPath)
	_, coverOK := m.coverProviders.Load(entryPath)
	_, subOK := m.playEventSubscribers.Load(entryPath)
	if lyricOK || coverOK || subOK {
		t.Errorf("提供者/订阅注册应已清除: lyric=%v cover=%v sub=%v", lyricOK, coverOK, subOK)
	}
	if m.IsPublicPath("/api/v1/jsplugin/" + entryPath + "/api/callback") {
		t.Error("publicPaths 应已移除")
	}
	if m.IsPluginDeleting(entryPath) {
		t.Error("删除成功后 deleting 标记应清除")
	}
	if pm.hasUninstallState(entryPath) {
		t.Error("删除成功后墓碑应已清除")
	}
	pluginDataDir := filepath.Join(dataDir, entryPath)
	if _, err := os.Stat(pluginDataDir); keepData && os.IsNotExist(err) {
		t.Error("keep_data=true 时插件数据目录应保留")
	} else if !keepData && err == nil {
		t.Error("默认删除时插件数据目录应已删除")
	}
}

// TestDeletePlugin_FullLifecycle 端到端：运行中插件删除后，运行态注册、
// 文件、DB、墓碑、publicPaths 全部清空，且同 entryPath 可立即重装。
func TestDeletePlugin_FullLifecycle(t *testing.T) {
	entryPath := "test-del-full"
	m, plugin, pluginsDir, dataDir := setupLoadedPlugin(t, entryPath, simpleJSCode, []string{"/api/callback"})
	pm := m.packager

	pluginPing(t, m, entryPath)

	// 挂上三类运行态注册。
	m.RegisterLyricProvider(entryPath)
	m.RegisterCoverProvider(entryPath)
	m.RegisterPlayEvent(entryPath)
	m.RefreshPublicPaths()
	if !m.IsPublicPath("/api/v1/jsplugin/" + entryPath + "/api/callback") {
		t.Fatal("删除前 publicPath 应命中")
	}

	if _, err := m.DeletePlugin(context.Background(), plugin.ID, false); err != nil {
		t.Fatalf("DeletePlugin: %v", err)
	}

	assertPluginGone(t, m, pm, plugin, pluginsDir, dataDir, false)

	// 删除后公开路由立即 404（EnsureLoaded 拒绝）。
	if _, err := m.EnsureLoaded(context.Background(), entryPath); !errors.Is(err, ErrPluginNotFound) {
		t.Errorf("删除后 EnsureLoaded 应 ErrPluginNotFound，got %v", err)
	}

	// 同一 entryPath 重新安装并正常工作。
	manifest := testManifest(entryPath)
	manifest.PublicPaths = []string{"/api/callback"}
	zipData := createTestPluginZipWithStatic(t, manifest, simpleJSCode, map[string]string{
		"index.html": "<head></head><body>reinstalled</body>",
	})
	newPlugin, _, err := pm.InstallFromUpload(zipData)
	if err != nil {
		t.Fatalf("同 entryPath 重装被拒: %v", err)
	}
	if err := m.EnablePlugin(context.Background(), newPlugin.ID); err != nil {
		t.Fatalf("重装后 EnablePlugin: %v", err)
	}
	pluginPing(t, m, entryPath)
}

// TestDeletePlugin_KeepData 验证 keep_data=true 保留数据目录，默认删除。
func TestDeletePlugin_KeepData(t *testing.T) {
	entryPath := "test-del-keepdata"
	m, plugin, pluginsDir, dataDir := setupLoadedPlugin(t, entryPath, simpleJSCode, nil)

	if _, err := m.DeletePlugin(context.Background(), plugin.ID, true); err != nil {
		t.Fatalf("DeletePlugin keepData: %v", err)
	}
	assertPluginGone(t, m, m.packager, plugin, pluginsDir, dataDir, true)

	// 显式核对保留下来的数据内容。
	index := filepath.Join(dataDir, entryPath, "static", "index.html")
	if _, err := os.Stat(index); err != nil {
		t.Fatalf("keep_data=true 应保留 static/index.html: %v", err)
	}
}

// TestDeletePlugin_NotLoaded 未加载（空闲驱逐/从未加载）的插件也应能完整删除。
func TestDeletePlugin_NotLoaded(t *testing.T) {
	pluginsDir, dataDir, repo, _ := setupTestEnv(t)
	manifest := testManifest("test-del-unloaded")
	zipData := createTestPluginZipWithStatic(t, manifest, simpleJSCode, nil)
	pm := NewPackageManager(pluginsDir, dataDir, repo)
	plugin, _, err := pm.InstallFromUpload(zipData)
	if err != nil {
		t.Fatalf("install: %v", err)
	}

	manager := NewManager(repo, pluginsDir, dataDir, "", nil, nil)
	t.Cleanup(func() { manager.Close() })

	if _, err := manager.DeletePlugin(context.Background(), plugin.ID, false); err != nil {
		t.Fatalf("DeletePlugin: %v", err)
	}
	assertPluginGone(t, manager, pm, plugin, pluginsDir, dataDir, false)
}

// TestDeletePlugin_DrainTimeoutAborts 慢请求未排空时删除必须失败：
// 删除窗口内新请求被拒绝；超时后错误可观察，ZIP/记录/服务原样保留并继续可用。
func TestDeletePlugin_DrainTimeoutAborts(t *testing.T) {
	entryPath := "test-del-drain"
	m, plugin, _, _ := setupLoadedPlugin(t, entryPath, slowPluginJS, nil)
	m.drainTimeout = 200 * time.Millisecond

	// 发起 ~1.2s 的慢调用。
	slowDone := make(chan error, 1)
	go func() {
		_, _, _, err := m.InvokeHTTP(context.Background(), entryPath, "GET", "/slow", nil, nil)
		slowDone <- err
	}()
	time.Sleep(150 * time.Millisecond) // 等慢调用进入 worker

	// 发起删除：它会停在排空阶段（慢调用还有 ~1s）。
	deleteDone := make(chan error, 1)
	go func() {
		_, delErr := m.DeletePlugin(context.Background(), plugin.ID, false)
		deleteDone <- delErr
	}()
	deadline := time.After(2 * time.Second)
	for !m.IsPluginDeleting(entryPath) {
		select {
		case <-deadline:
			t.Fatal("删除未进入 deleting 态")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	// 删除窗口内新请求应立即被阻断（ErrPluginNotFound，路由层映射为 404）。
	if _, err := m.EnsureLoaded(context.Background(), entryPath); !errors.Is(err, ErrPluginNotFound) {
		t.Fatalf("删除窗口内 EnsureLoaded 应返回 ErrPluginNotFound，got %v", err)
	}

	start := time.Now()
	select {
	case err := <-deleteDone:
		if !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("期望 ErrDrainTimeout，got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("删除未在排空预算后返回")
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("应在排空预算 ~200ms 后返回，实际 %v", d)
	}

	// 文件与记录原样保留。
	if _, statErr := os.Stat(filepath.Join(m.pluginsDir, plugin.FilePath)); statErr != nil {
		t.Errorf("排空失败不应删除 ZIP: %v", statErr)
	}
	if _, err := m.repo.GetByEntryPath(context.Background(), entryPath); err != nil {
		t.Fatalf("排空失败不应删除 DB 记录: %v", err)
	}
	if m.IsPluginDeleting(entryPath) {
		t.Error("失败回滚后 deleting 标记应清除，插件恢复可用")
	}

	// 慢调用正常完成；之后插件照常服务（排空超时已回滚）。
	select {
	case err := <-slowDone:
		if err != nil {
			t.Fatalf("在途慢调用应在 worker 上跑完，不应被取消: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("在途慢调用未完成")
	}
	pluginPing(t, m, entryPath)
}

// TestDeletePlugin_ConcurrentRejects 并发删除同一插件，第二个拿到 409 语义错误。
func TestDeletePlugin_ConcurrentRejects(t *testing.T) {
	entryPath := "test-del-concurrent"
	m, plugin, _, _ := setupLoadedPlugin(t, entryPath, slowPluginJS, nil)
	m.drainTimeout = 2 * time.Second

	go func() {
		_, _, _, _ = m.InvokeHTTP(context.Background(), entryPath, "GET", "/slow", nil, nil)
	}()
	time.Sleep(150 * time.Millisecond)

	errCh := make(chan error, 1)
	go func() {
		_, err := m.DeletePlugin(context.Background(), plugin.ID, false)
		errCh <- err
	}()

	// 等第一个删除进入阻断态。
	deadline := time.After(2 * time.Second)
	for !m.IsPluginDeleting(entryPath) {
		select {
		case <-deadline:
			t.Fatal("第一个删除未进入 deleting 态")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	_, err := m.DeletePlugin(context.Background(), plugin.ID, false)
	if !errors.Is(err, ErrPluginDeleting) {
		t.Fatalf("并发删除应返回 ErrPluginDeleting，got %v", err)
	}

	// 等慢调用结束，第一个删除成功收敛。
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("第一个删除应成功: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("第一个删除未完成")
	}
}

// --- 墓碑持久化与崩溃恢复 ---

func installForRecoveryTest(t *testing.T, entryPath string) (*PackageManager, *JSPlugin, string, string) {
	t.Helper()
	pluginsDir, dataDir, repo, _ := setupTestEnv(t)
	pm := NewPackageManager(pluginsDir, dataDir, repo)
	manifest := testManifest(entryPath)
	zipData := createTestPluginZipWithStatic(t, manifest, simpleJSCode, map[string]string{
		"index.html": "<head></head><body>x</body>",
	})
	plugin, _, err := pm.InstallFromUpload(zipData)
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	return pm, plugin, pluginsDir, dataDir
}

// TestRecovery_AfterZipRemovedCrash 模拟「墓碑已写、ZIP 已删、进程中断，
// DB 记录还在」：下次启动必须收敛为完整删除（记录与数据目录都清掉）。
func TestRecovery_AfterZipRemovedCrash(t *testing.T) {
	entryPath := "test-rec-zip"
	pm, plugin, pluginsDir, dataDir := installForRecoveryTest(t, entryPath)

	if _, err := pm.beginUninstall(entryPath, plugin.FilePath, false); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := os.Remove(filepath.Join(pluginsDir, plugin.FilePath)); err != nil {
		t.Fatalf("remove zip: %v", err)
	}

	// 新 PackageManager 模拟进程重启。
	restarted := NewPackageManager(pluginsDir, dataDir, pm.repo)
	recovered, err := restarted.RecoverPendingUninstalls()
	if err != nil {
		t.Fatalf("RecoverPendingUninstalls: %v", err)
	}
	if len(recovered) != 1 || recovered[0] != entryPath {
		t.Fatalf("应收敛 1 个墓碑，got %v", recovered)
	}
	if _, err := pm.repo.GetByEntryPath(context.Background(), entryPath); !errors.Is(err, database.ErrNotFound) {
		t.Fatalf("记录应删除: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, entryPath)); !os.IsNotExist(err) {
		t.Fatal("数据目录应删除")
	}
	if restarted.hasUninstallState(entryPath) {
		t.Fatal("收敛后墓碑应删除")
	}
}

// TestRecovery_ResidualZipNotReinstalled 「记录已删、ZIP 残留 + 墓碑」场景：
// 启动扫描不得把残留 ZIP 当新插件安装；恢复后 ZIP 被删除。
func TestRecovery_ResidualZipNotReinstalled(t *testing.T) {
	entryPath := "test-rec-residual"
	pm, plugin, pluginsDir, _ := installForRecoveryTest(t, entryPath)

	// 先写墓碑、删记录，ZIP 保留——模拟在「删记录之后、删 ZIP 之前」崩溃
	// （实际实现先删 ZIP，此测试同时验证乱序场景也能收敛）。
	if _, err := pm.beginUninstall(entryPath, plugin.FilePath, false); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := pm.repo.Delete(context.Background(), plugin.ID); err != nil {
		t.Fatalf("delete record: %v", err)
	}

	// 扫描时墓碑仍在：残留 ZIP 必须被跳过，不得重装。
	synced, err := pm.SyncPluginsFromDirectory()
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	for _, p := range synced {
		if p.EntryPath == entryPath {
			t.Fatal("带墓碑的残留 ZIP 不应被启动扫描重新安装")
		}
	}
	if _, err := pm.repo.GetByEntryPath(context.Background(), entryPath); !errors.Is(err, database.ErrNotFound) {
		t.Fatal("记录不应被扫描重建")
	}

	// 恢复收敛：ZIP 与墓碑消失。
	recovered, err := pm.RecoverPendingUninstalls()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("应收敛 1 个，got %v", recovered)
	}
	if _, err := os.Stat(filepath.Join(pluginsDir, plugin.FilePath)); !os.IsNotExist(err) {
		t.Fatal("残留 ZIP 应被恢复流程删除")
	}

	// 再扫描也不会复活。
	synced2, _ := pm.SyncPluginsFromDirectory()
	for _, p := range synced2 {
		if p.EntryPath == entryPath {
			t.Fatal("收敛后扫描不应再出现该插件")
		}
	}
}

// TestRecovery_KeepData 墓碑 keep_data=true 时收敛保留数据目录。
func TestRecovery_KeepData(t *testing.T) {
	entryPath := "test-rec-keep"
	pm, plugin, _, dataDir := installForRecoveryTest(t, entryPath)

	if _, err := pm.beginUninstall(entryPath, plugin.FilePath, true); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := pm.RecoverPendingUninstalls(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, entryPath, "static", "index.html")); err != nil {
		t.Fatalf("keep_data=true 的数据目录应保留: %v", err)
	}
	if _, err := pm.repo.GetByEntryPath(context.Background(), entryPath); !errors.Is(err, database.ErrNotFound) {
		t.Fatal("记录仍应删除")
	}
}

// TestUninstall_RetryAfterFailure 模拟文件系统部分失败：ZIP 路径被目录占用导致
// 删除失败，错误必须可观察、墓碑与记录保留；排除故障后重试（同进程/重启）收敛。
func TestUninstall_RetryAfterFailure(t *testing.T) {
	entryPath := "test-del-retry"
	pm, plugin, pluginsDir, _ := installForRecoveryTest(t, entryPath)

	// 用「非空同名目录」占位 ZIP 路径：os.Remove 能删空目录，但删不掉非空目录，
	// 稳定制造文件系统失败（Windows/Linux 行为一致）。
	zipPath := filepath.Join(pluginsDir, plugin.FilePath)
	if err := os.Remove(zipPath); err != nil {
		t.Fatalf("setup remove zip: %v", err)
	}
	if err := os.MkdirAll(zipPath, 0o755); err != nil {
		t.Fatalf("mkdir blocking zip path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(zipPath, "locked.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker file: %v", err)
	}

	err := pm.Uninstall(plugin.ID, false)
	if err == nil {
		t.Fatal("ZIP 删除失败时 Uninstall 必须返回错误")
	}
	if !pm.hasUninstallState(entryPath) {
		t.Error("失败后墓碑必须保留")
	}
	if _, err := pm.repo.GetByEntryPath(context.Background(), entryPath); err != nil {
		t.Fatalf("失败后 DB 记录必须保留: %v", err)
	}

	// 排除故障后模拟重启继续收敛。
	if err := os.RemoveAll(zipPath); err != nil {
		t.Fatalf("remove blocking dir: %v", err)
	}
	restarted := NewPackageManager(pluginsDir, pm.dataDir, pm.repo)
	if _, err := restarted.RecoverPendingUninstalls(); err != nil {
		t.Fatalf("重启收敛: %v", err)
	}
	if _, err := pm.repo.GetByEntryPath(context.Background(), entryPath); !errors.Is(err, database.ErrNotFound) {
		t.Fatalf("重试后记录应删除: %v", err)
	}
}

// TestInstall_RejectedWhileTombstone 墓碑未收敛期间，安装/更新同 entryPath 被拒。
func TestInstall_RejectedWhileTombstone(t *testing.T) {
	entryPath := "test-tomb-install"
	pm, plugin, _, _ := installForRecoveryTest(t, entryPath)

	if _, err := pm.beginUninstall(entryPath, plugin.FilePath, false); err != nil {
		t.Fatalf("begin: %v", err)
	}

	manifest := testManifest(entryPath)
	zipData := createTestPluginZip(t, manifest, simpleJSCode)
	if _, _, err := pm.InstallFromUpload(zipData); !errors.Is(err, ErrPluginUninstalling) {
		t.Fatalf("墓碑存在时安装应返回 ErrPluginUninstalling，got %v", err)
	}
}

// TestPackageManager_InstallGuard 内存守卫拦截删除阶段 1（墓碑未落盘）的覆盖安装。
func TestPackageManager_InstallGuard(t *testing.T) {
	entryPath := "test-guard"
	m, plugin, _, _ := setupLoadedPlugin(t, entryPath, simpleJSCode, nil)

	manifest := testManifest(entryPath)
	zipData := createTestPluginZip(t, manifest, simpleJSCode)

	if !m.markDeleting(entryPath) {
		t.Fatal("markDeleting 应首次成功")
	}
	if _, _, err := m.packager.InstallFromUpload(zipData); !errors.Is(err, ErrPluginDeleting) {
		t.Fatalf("删除窗口内安装应被内存守卫拒绝 ErrPluginDeleting，got %v", err)
	}
	if _, err := m.packager.Update(plugin.ID, zipData); !errors.Is(err, ErrPluginDeleting) {
		t.Fatalf("删除窗口内覆盖更新应被内存守卫拒绝 ErrPluginDeleting，got %v", err)
	}
	m.clearDeleting(entryPath)

	// 守卫放开后更新路径恢复可用（更新成功后恢复插件状态）。
	if _, err := m.packager.Update(plugin.ID, zipData); err != nil {
		t.Fatalf("删除标记清除后更新应恢复正常: %v", err)
	}
}

// TestIsPublicPath_DeletingFilter 删除窗口内 publicPaths 立即失效，
// 不依赖 DB 记录删除与 RefreshPublicPaths。
func TestIsPublicPath_DeletingFilter(t *testing.T) {
	pluginsDir, dataDir, repo, _ := setupTestEnv(t)
	m := NewManager(repo, pluginsDir, dataDir, "", nil, nil)
	t.Cleanup(func() { m.Close() })

	entryPath := "test-pubpath-del"
	plugin := &JSPlugin{
		Name: "p", Version: "1.0.0", EntryPath: entryPath, Main: "main.js",
		Permissions: []string{}, PublicPaths: []string{"/api/callback"},
		Status: JSPluginStatusActive, FilePath: entryPath + ".jsplugin.zip",
	}
	if err := repo.Create(context.Background(), plugin); err != nil {
		t.Fatalf("create: %v", err)
	}
	m.RefreshPublicPaths()
	publicURL := "/api/v1/jsplugin/" + entryPath + "/api/callback"
	if !m.IsPublicPath(publicURL) {
		t.Fatal("删除前 publicPath 应命中")
	}

	if !m.markDeleting(entryPath) {
		t.Fatal("markDeleting 应首次成功")
	}
	t.Cleanup(func() { m.clearDeleting(entryPath) })
	if m.IsPublicPath(publicURL) {
		t.Fatal("删除窗口内 publicPath 必须立即失效")
	}
	if !m.isDeletingPluginPath("/api/v1/jsplugin/" + entryPath + "/static/x.js") {
		t.Fatal("删除窗口内静态子路径应被识别为删除中")
	}
	if m.isDeletingPluginPath("/api/v1/jsplugin/other/api/callback") {
		t.Fatal("不应误伤其他插件路径")
	}
}
