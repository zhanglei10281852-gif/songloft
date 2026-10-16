package jsplugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"songloft/internal/database"
)

// uninstallStateDir 是插件删除墓碑（tombstone）目录，位于 pluginsDir 下。
// 目录名以点开头，SyncPluginsFromDirectory 只认 .jsplugin.zip 文件，不会误读。
const uninstallStateDir = ".uninstall"

// ErrPluginUninstalling 插件删除流程尚未收敛（墓碑仍在），任何安装/更新/重载
// 尝试都必须拒绝，防止残留 ZIP 或新包与删除流程互相覆盖。
var ErrPluginUninstalling = errors.New("jsplugin: plugin is being uninstalled")

// pluginUninstallState 是删除流程的持久墓碑。
//
// 在任何破坏性操作（删 ZIP / 数据目录 / DB 记录）之前原子落盘；只有当三者都
// 确认不存在后才删除墓碑。进程在任一阶段崩溃，或某一步在 Windows 上因句柄
// 占用失败，下次启动由 RecoverPendingUninstalls 读到墓碑并继续收敛，保证：
//   - 不留下「active 记录却没有包」的半卸载状态；
//   - 残留 ZIP 不会被 SyncPluginsFromDirectory 当新插件重新安装；
//   - keep_data=true 时数据目录在收敛中始终被保留。
type pluginUninstallState struct {
	EntryPath string `json:"entry_path"`
	// FilePath 是删除开始时 DB 记录中的 ZIP 文件名（通常为 <entryPath>.jsplugin.zip）。
	FilePath  string `json:"file_path"`
	KeepData  bool   `json:"keep_data"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// uninstallStatePath 返回墓碑文件路径。entryPath 已由 manifest 校验限定为
// ^[a-z][a-z0-9-]*$，这里再做一层 filepath.Base 防御。
func (pm *PackageManager) uninstallStatePath(entryPath string) string {
	return filepath.Join(pm.pluginsDir, uninstallStateDir, filepath.Base(entryPath)+".json")
}

// saveUninstallState 原子写入墓碑（tmp 文件 + rename），崩溃只会出现
// 「旧墓碑 / 新墓碑」二者之一，不会留下半截 JSON。
func (pm *PackageManager) saveUninstallState(state *pluginUninstallState) error {
	dir := filepath.Join(pm.pluginsDir, uninstallStateDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create uninstall state dir: %w", err)
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal uninstall state: %w", err)
	}

	final := pm.uninstallStatePath(state.EntryPath)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write uninstall state temp file: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename uninstall state file: %w", err)
	}
	return nil
}

// removeUninstallState 删除墓碑。文件不存在视为成功（幂等收敛）。
func (pm *PackageManager) removeUninstallState(entryPath string) error {
	err := os.Remove(pm.uninstallStatePath(entryPath))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// hasUninstallState 报告指定插件是否有未收敛的删除墓碑。
func (pm *PackageManager) hasUninstallState(entryPath string) bool {
	_, err := os.Stat(pm.uninstallStatePath(entryPath))
	return err == nil
}

// uninstallStateEntries 返回所有待收敛墓碑的 entryPath 集合。
func (pm *PackageManager) uninstallStateEntries() map[string]bool {
	entries := make(map[string]bool)
	dir := filepath.Join(pm.pluginsDir, uninstallStateDir)
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return entries
		}
		slog.Warn("read uninstall state dir failed", "dir", dir, "error", err)
		return entries
	}
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		entries[strings.TrimSuffix(f.Name(), ".json")] = true
	}
	return entries
}

// loadUninstallState 读取并校验单个墓碑；损坏的墓碑返回错误，由调用方记录并
// 保留现场（不静默删除，避免丢删除意图）。
func (pm *PackageManager) loadUninstallState(entryPath string) (*pluginUninstallState, error) {
	data, err := os.ReadFile(pm.uninstallStatePath(entryPath))
	if err != nil {
		return nil, err
	}
	var state pluginUninstallState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parse uninstall state for %q: %w", entryPath, err)
	}
	if state.EntryPath == "" {
		state.EntryPath = entryPath
	}
	if state.EntryPath != entryPath {
		return nil, fmt.Errorf("uninstall state entry_path mismatch: file=%q content=%q", entryPath, state.EntryPath)
	}
	return &state, nil
}

// beginUninstall 在破坏性删除前落盘墓碑。墓碑已存在时视为幂等成功（重复删除
// 或崩溃后的同进程重试），刷新 UpdatedAt。
func (pm *PackageManager) beginUninstall(entryPath, filePath string, keepData bool) (*pluginUninstallState, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	state := &pluginUninstallState{
		EntryPath: entryPath,
		FilePath:  filePath,
		KeepData:  keepData,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if existing, err := pm.loadUninstallState(entryPath); err == nil {
		// 已存在墓碑：沿用原 CreatedAt 与 keep_data 语义（首次删除的意图优先），
		// 只刷新目标 ZIP 文件名与更新时间。
		state.CreatedAt = existing.CreatedAt
		state.KeepData = existing.KeepData
	}
	if err := pm.saveUninstallState(state); err != nil {
		return nil, err
	}
	return state, nil
}

// finishUninstall 按墓碑收敛删除，全程幂等，可在崩溃/失败后反复调用：
//  1. 删除 ZIP（不存在视为成功）；
//  2. keep_data=false 时删除插件数据目录（static/bin/cache 等，不存在视为成功）；
//  3. 删除 js_plugins DB 记录（已不存在视为成功）；
//  4. 全部确认后删除墓碑。
//
// 任一步失败立即返回错误并保留墓碑，等待下次调用（同进程重试或下次启动）。
func (pm *PackageManager) finishUninstall(ctx context.Context, state *pluginUninstallState) error {
	// [1] 删除 ZIP。墓碑记录的 FilePath 与约定文件名都尝试一遍，兼容历史数据。
	zipNames := []string{filepath.Base(state.FilePath), state.EntryPath + ".jsplugin.zip"}
	zipNames = slices.Compact(zipNames)
	for _, name := range zipNames {
		if name == "" || name == "." || filepath.IsAbs(name) {
			continue
		}
		zipPath := filepath.Join(pm.pluginsDir, name)
		if err := os.Remove(zipPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove plugin zip %q: %w", zipPath, err)
		}
	}

	// [2] 删除数据目录（keep_data=true 时保留；宿主 DB 中的 plugin_storage
	// 沿用既有卸载保留语义，从来不在此处理）。
	if !state.KeepData {
		dataPath := filepath.Join(pm.dataDir, state.EntryPath)
		if err := os.RemoveAll(dataPath); err != nil {
			return fmt.Errorf("remove plugin data dir %q: %w", dataPath, err)
		}
	}

	// [3] 删除 DB 记录（若仍存在）。
	plugin, err := pm.repo.GetByEntryPath(ctx, state.EntryPath)
	switch {
	case err == nil && plugin != nil:
		if err := pm.repo.Delete(ctx, plugin.ID); err != nil {
			return fmt.Errorf("delete plugin record %q: %w", state.EntryPath, err)
		}
	case errors.Is(err, database.ErrNotFound):
		// 记录已删，继续清理墓碑。
	default:
		return fmt.Errorf("lookup plugin record %q: %w", state.EntryPath, err)
	}

	// [4] 全部目标已不存在，最后删墓碑。删墓碑失败仅告警：所有删除目标已确认
	// 消失，下次启动的收敛流程会再次走到这里并成功。
	if err := pm.removeUninstallState(state.EntryPath); err != nil {
		slog.Warn("remove uninstall state failed, will retry on next startup",
			"entryPath", state.EntryPath, "error", err)
	}
	return nil
}

// RecoverPendingUninstalls 在启动扫描前收敛所有未完成的删除。
// 返回成功收敛的插件 entryPath 列表；单个墓碑失败不阻断其他墓碑的收敛。
func (pm *PackageManager) RecoverPendingUninstalls() ([]string, error) {
	pending := pm.uninstallStateEntries()
	if len(pending) == 0 {
		return nil, nil
	}

	ctx := context.Background()
	recovered := make([]string, 0, len(pending))
	var errs []error
	for entryPath := range pending {
		state, err := pm.loadUninstallState(entryPath)
		if err != nil {
			// 损坏墓碑：保留现场并报错，不能一删了之（删除意图可能仍需收敛）。
			errs = append(errs, err)
			slog.Error("uninstall state unreadable, skipping recovery",
				"entryPath", entryPath, "error", err)
			continue
		}
		if err := pm.finishUninstall(ctx, state); err != nil {
			errs = append(errs, err)
			slog.Error("resume plugin uninstall failed",
				"entryPath", entryPath, "keepData", state.KeepData, "error", err)
			continue
		}
		slog.Info("resumed and completed plugin uninstall",
			"entryPath", entryPath, "keepData", state.KeepData)
		recovered = append(recovered, entryPath)
	}
	return recovered, errors.Join(errs...)
}
