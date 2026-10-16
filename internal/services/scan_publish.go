package services

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"songloft/internal/database"
	"songloft/internal/models"
)

// 扫描相关 config key。handlers 层的同名常量保留为对这里的引用，
// 避免「业务端点」与「发布流程」各自拼写字符串造成漂移。
const (
	musicPathConfigKey       = "music_path"
	scanTitleSourceConfigKey = "scan_title_source"
)

// scanStopReason 描述扫描为什么被要求停止。
type scanStopReason int

const (
	scanStopNone   scanStopReason = iota // 未收到停止信号
	scanStopUser                         // 用户经 /scan/cancel 取消：保留已提取成果
	scanStopSwitch                       // 配置发布要求旧代扫描收尾：旧结果一律不得越过发布边界
)

// scanRun 是一次扫描任务的运行时句柄，也是配置发布的协调单元（fence）。
//
// 生命周期：ScanAndImportAsync 创建并登记为 activeRun，goroutine 退出时注销。
// 发布流程（PublishMusicPath）通过 activeRun 找到在跑的旧代扫描，关闭 switchCh
// 并等待 done，保证成功响应返回前旧扫描已经停止接收新工作并完全收尾。
type scanRun struct {
	generation uint64   // 启动时捕获的配置代际
	scanner    *Scanner // 启动时捕获的扫描器，运行期不回读当前 binding

	// userCancel 复用 ScanProgressManager 的用户取消通道（可能随 Complete/Fail 被置 nil，
	// 这里持有启动时的那份引用）。
	userCancel <-chan struct{}
	// switchCh 由配置发布关闭：旧代扫描即刻停止接收新工作，已发现/已提取/待提交/
	// CUE 处理中的旧结果全部丢弃，不再执行歌单与指纹等任何后置副作用。
	switchCh chan struct{}

	ctx      context.Context // userCancel 或 switchCh 任一触发都会被取消（用于杀 ffprobe 子进程）
	cancelFn context.CancelFunc
	done     chan struct{} // goroutine 退出时关闭，发布方据此 join

	// abandoned 发布边界落下前置位；flushScanBatch / processCueSheet / 自动歌单 /
	// 自动指纹等所有写库收口点在动手前复核它，是「旧结果不能越过边界」的第二道防线。
	abandoned atomic.Bool
}

// stopReason 非阻塞判断当前停止原因。配置发布优先于用户取消：发布进行中
// 即便用户同时点了取消，也必须按「丢弃旧结果」收尾，否则边界会被旧写入穿透。
func (r *scanRun) stopReason() scanStopReason {
	select {
	case <-r.switchCh:
		return scanStopSwitch
	default:
	}
	select {
	case <-r.userCancel:
		return scanStopUser
	default:
	}
	return scanStopNone
}

// switched 判断发布边界是否已落下。
func (r *scanRun) switched() bool {
	select {
	case <-r.switchCh:
		return true
	default:
	}
	return false
}

// terminalize 按停止原因把进度推进终态。返回 true 表示调用方应当立刻结束扫描。
func (s *SongService) terminalizeIfStopped(run *scanRun) bool {
	switch run.stopReason() {
	case scanStopSwitch:
		s.scanProgressManager.SetStoppedExternally()
		return true
	case scanStopUser:
		s.scanProgressManager.SetCancelled()
		return true
	default:
		return false
	}
}

// MusicPathConfig 是音乐目录与排除规则的发布载荷。
// JSON 形态与 config 表 music_path 行、/settings/music-path 接口体保持互通。
type MusicPathConfig struct {
	Path                  string   `json:"path"`
	ExcludeDirs           []string `json:"exclude_dirs"`
	ExcludePaths          []string `json:"exclude_paths"`
	AutoCreateExcludeDirs []string `json:"auto_create_exclude_dirs"`
	// Generation 由服务端在发布成功时写入，外部请求携带的值一律忽略。
	// 重启后仅凭这一行就能恢复「最后成功发布的那一代」。
	Generation uint64 `json:"generation,omitempty"`
}

// PublishMusicPath 以「发布边界」语义应用新一代音乐目录/排除配置。
//
// 成功返回（= 配置更新成功响应可以发出）前，按顺序保证：
//  1. 关闭启动闸门，手动/自动扫描不再接收新工作；
//  2. 旧配置的在跑扫描被要求停止并 join 完成：旧结果（发现/提取/待批量提交/CUE 处理中）
//     一律丢弃，不再更新进度、不建歌单、不启动指纹；旧扫描尾部启动的指纹任务也被收尾；
//  3. 按新规则清理曲库：删除不再位于新音乐根目录、或落入新排除范围的记录
//     （含 CUE 来源组），即使旧文件仍真实存在；
//  4. 持久化新一代配置（提交点）并原子替换内存 Scanner，执行 afterSwap 接线。
//
// 失败语义：步骤 2/3 失败时不落库、不换 Scanner，旧配置继续完整可用；
// 步骤 4 失败时内存仍保留旧配置，已清理的行会在旧配置下一次扫描时按磁盘文件恢复。
// 进程在提交点前后崩溃，重启都只会恢复已成功发布的那一代（提交前=旧代，提交后=新代，
// 曲库清理发生在提交之前）。
//
// afterSwap 在新 Scanner 原子生效后同步调用（仅用于刷新 handler 引用等内存接线，
// 不应返回错误、不应做重活）。返回值为新一代代际号。
func (s *SongService) PublishMusicPath(ctx context.Context, req MusicPathConfig, afterSwap func(*Scanner)) (uint64, error) {
	if s.configService == nil {
		return 0, fmt.Errorf("configService 未注入，无法发布扫描配置")
	}
	// 通用 /configs 入口可能只提交部分字段（nil 切片），统一规范化为空数组，
	// 保证持久化的 JSON 形态确定、Scanner 排除规则与 GET 响应稳定。
	if req.ExcludeDirs == nil {
		req.ExcludeDirs = []string{}
	}
	if req.ExcludePaths == nil {
		req.ExcludePaths = []string{}
	}
	if req.AutoCreateExcludeDirs == nil {
		req.AutoCreateExcludeDirs = []string{}
	}

	// 整个发布流程串行化，避免两次切换交错留下中间状态。
	s.publishMu.Lock()
	defer s.publishMu.Unlock()

	formats, err := s.loadSupportedFormats()
	if err != nil {
		return 0, err
	}
	newScanner := NewScanner(&ScanConfig{
		MusicPath:        req.Path,
		ExcludeDirs:      req.ExcludeDirs,
		ExcludePaths:     req.ExcludePaths,
		SupportedFormats: formats,
	})

	// 关闸 + 抓取活动扫描必须与扫描注册互斥：关闸后不可能再有新 run 登记。
	// 锁序固定为 startMu → scanRegMu，与 ScanAndImportAsync 的注册路径一致。
	s.startMu.Lock()
	s.publishing.Store(true)
	s.scanRegMu.Lock()
	run := s.activeRun
	s.scanRegMu.Unlock()
	s.startMu.Unlock()
	defer s.publishing.Store(false)

	// 停止旧代扫描并等待其完全收尾（ctx 可被 HTTP 请求取消打断等待）。
	if run != nil {
		slog.Info("配置发布：停止在跑的旧配置扫描", "oldGeneration", run.generation)
		close(run.switchCh)
		run.abandoned.Store(true)
		run.cancelFn()
		select {
		case <-run.done:
		case <-ctx.Done():
			return 0, fmt.Errorf("等待旧扫描收尾被中断: %w", ctx.Err())
		}
	}

	// 旧扫描尾部可能已异步启动指纹计算（runAutoFingerprint 启动后即与扫描 goroutine
	// 脱钩），同样属于旧配置任务，发布前必须停掉并等其退出。
	if fs := s.fingerprintService.Load(); fs != nil {
		if fs.Cancel() {
			slog.Info("配置发布：已停止旧扫描启动的指纹计算")
		}
	}

	// 按新规则清理曲库。此时已无任何旧代写入者，清理结果不会被旧批次「复活」。
	cleaned, err := s.cleanSongsForConfig(ctx, newScanner)
	if err != nil {
		slog.Error("配置发布：按新规则清理曲库失败，保留旧配置", "error", err)
		return 0, fmt.Errorf("按新规则清理曲库失败: %w", err)
	}
	if cleaned > 0 {
		slog.Info("配置发布：已清除不属于新配置的记录", "cleaned", cleaned)
	}

	// 提交点：持久化新一代配置。此行成功前崩溃/失败，重启恢复的仍是旧代。
	old := s.binding.Load()
	nextGeneration := uint64(1)
	if old != nil {
		nextGeneration = old.generation + 1
	}
	persisted := MusicPathConfig{
		Path:                  req.Path,
		ExcludeDirs:           req.ExcludeDirs,
		ExcludePaths:          req.ExcludePaths,
		AutoCreateExcludeDirs: req.AutoCreateExcludeDirs,
		Generation:            nextGeneration,
	}
	if err := s.configService.SetJSON(musicPathConfigKey, persisted); err != nil {
		slog.Error("配置发布：持久化新配置失败，保留旧配置", "error", err)
		return 0, fmt.Errorf("持久化扫描配置失败: %w", err)
	}

	// 内存 Scanner 与代际原子生效；此后新发起的手动/自动扫描统一使用新规则。
	s.binding.Store(&scannerBinding{scanner: newScanner, generation: nextGeneration})
	if s.metadataExtractor != nil {
		s.metadataExtractor.SetTitleSource(s.configService.GetString(scanTitleSourceConfigKey, "tag"))
	}
	if afterSwap != nil {
		afterSwap(newScanner)
	}
	slog.Info("配置发布完成",
		"generation", nextGeneration,
		"musicPath", req.Path,
		"excludeDirs", req.ExcludeDirs,
		"excludePaths", req.ExcludePaths,
		"cleaned", cleaned,
	)
	return nextGeneration, nil
}

// loadSupportedFormats 读取 scan_config 支持的音频格式；未配置时使用内置全量默认列表。
func (s *SongService) loadSupportedFormats() ([]string, error) {
	if s.configService == nil {
		return nil, fmt.Errorf("configService 未注入")
	}
	var scanConfigData struct {
		SupportedFormats []string `json:"supported_formats"`
	}
	if err := s.configService.GetJSON("scan_config", &scanConfigData); err != nil || len(scanConfigData.SupportedFormats) == 0 {
		return []string{"mp3", "flac", "wav", "ape", "ogg", "m4a", "mp4", "mov", "wma", "aif", "aiff", "mka", "mkv", "webm", "avi", "ts", "mpg", "opus", "m4b", "oga", "mpeg", "m4v", "flv", "wmv", "rm", "rmvb", "3gp", "dsf"}, nil
	}
	return scanConfigData.SupportedFormats, nil
}

// cleanSongsForConfig 按「新配置」清理曲库：删除不属于新 Scanner 的本地歌曲记录。
//
// 与 CleanInvalidSongs 的差异：
//   - 不要求文件已从磁盘消失——根目录改走或新增排除规则时旧文件仍真实存在，记录也要清除；
//   - 同时清理 CUE 来源组：CUE 源或其引用音频脱离新根目录/进入排除区时整组删除；
//   - 不删除磁盘文件（deleteFiles 恒为 false）。
//
// 必须在无旧代写入者（旧扫描已 join）时调用，返回清理的记录数。
func (s *SongService) cleanSongsForConfig(ctx context.Context, newScanner *Scanner) (int, error) {
	songs, err := s.songs.List(ctx, &database.SongFilter{
		Type:  models.TypeLocal,
		Limit: models.MaxPaginationLimit,
	})
	if err != nil {
		return 0, fmt.Errorf("查询本地歌曲失败: %w", err)
	}

	var staleIDs []int64
	badCueSources := make(map[string]struct{})
	for _, song := range songs {
		if song.CueSourcePath != "" {
			if !newScanner.OwnsPath(song.CueSourcePath) ||
				(song.CueAudioPath != "" && !newScanner.OwnsPath(song.CueAudioPath)) {
				badCueSources[song.CueSourcePath] = struct{}{}
			}
			continue
		}
		if !newScanner.OwnsPath(song.FilePath) {
			staleIDs = append(staleIDs, song.ID)
		}
	}

	cleaned := 0
	if len(staleIDs) > 0 {
		deleted, err := s.BatchDelete(ctx, staleIDs, false)
		if err != nil {
			return cleaned, fmt.Errorf("清理越界/排除歌曲失败: %w", err)
		}
		cleaned += deleted
	}
	for source := range badCueSources {
		deleted, err := s.songs.DeleteByCueSource(ctx, source)
		if err != nil {
			return cleaned, fmt.Errorf("清理越界/排除 CUE 来源失败 %s: %w", source, err)
		}
		if deleted > 0 {
			slog.Info("配置发布：清理 CUE 来源组", "source", source, "deleted", deleted)
		}
		cleaned += deleted
	}
	return cleaned, nil
}
