package services

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"songloft/internal/database"
	"songloft/internal/database/testutil"
	"songloft/internal/models"
)

// waitScanTerminal 轮询扫描进度直到进入终态或超时，返回最终进度。
func waitScanTerminal(t *testing.T, svc *SongService) ScanProgress {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		p := svc.GetScanProgress()
		switch p.Status {
		case ScanStatusCompleted, ScanStatusFailed, ScanStatusCancelled:
			return p
		}
		time.Sleep(10 * time.Millisecond)
	}
	p := svc.GetScanProgress()
	t.Fatalf("扫描未在超时时间内进入终态: %+v", p)
	return p
}

// newPublishTestService 构造带真实内存 DB、ConfigService 与 tag 提取器的 SongService，
// 初始 Scanner 指向 initialRoot。返回 DB 与 service。
func newPublishTestService(t *testing.T, initialRoot string) (*database.SQLiteDB, *SongService) {
	t.Helper()
	mdb := testutil.OpenMemoryDB(t)
	cfgSvc := NewConfigService(mdb.ConfigRepository())
	if err := cfgSvc.SetJSON(musicPathConfigKey, MusicPathConfig{
		Path:        initialRoot,
		ExcludeDirs: []string{},
		ExcludePaths: []string{},
	}); err != nil {
		t.Fatal(err)
	}
	scanner := NewScanner(&ScanConfig{MusicPath: initialRoot, SupportedFormats: []string{"flac"}})
	extractor := NewMetadataExtractor(&MetadataConfig{FFProbePath: "ffprobe", CoverStoragePath: t.TempDir()})
	svc := NewSongService(mdb.SongRepository(), mdb, extractor, scanner, cfgSvc, nil)
	return mdb, svc
}

// seedFlac 把测试 FLAC 样本复制为 dst（mtime 拨到 1 分钟前避开写入稳定性窗口）。
func seedFlac(t *testing.T, dst string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		t.Fatal(err)
	}
	sample, err := os.ReadFile(filepath.Join("..", "..", "pkg", "tag", "testdata", "with_tags", "sample.flac"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, sample, 0644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(dst, old, old); err != nil {
		t.Fatal(err)
	}
}

func localSongPaths(t *testing.T, mdb *database.SQLiteDB) map[string]bool {
	t.Helper()
	paths, err := mdb.SongRepository().ListLocalPaths(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]bool, len(paths))
	for p := range paths {
		out[p] = true
	}
	return out
}

// TestPublishMusicPath_SwapsAndCleansOutsideRoot 发布新音乐根目录：
// 旧根目录下的记录即使文件仍存在也必须清除；内存 Scanner、代际、持久化配置一致切换；
// 重启模拟后只恢复新一代配置，新扫描不会把旧根歌曲带回来。
func TestPublishMusicPath_SwapsAndCleansOutsideRoot(t *testing.T) {
	rootA := filepath.Join(t.TempDir(), "musicA")
	rootB := filepath.Join(t.TempDir(), "musicB")
	songA := filepath.Join(rootA, "a.flac")
	songB := filepath.Join(rootB, "b.flac")
	seedFlac(t, songA)
	seedFlac(t, songB)

	mdb, svc := newPublishTestService(t, rootA)

	if err := svc.ScanAndImportAsync(false, nil); err != nil {
		t.Fatal(err)
	}
	if p := waitScanTerminal(t, svc); p.Status != ScanStatusCompleted {
		t.Fatalf("初始扫描状态: %+v", p)
	}
	if !localSongPaths(t, mdb)[songA] {
		t.Fatalf("初始扫描应导入旧根目录歌曲 %s", songA)
	}

	gen, err := svc.PublishMusicPath(context.Background(), MusicPathConfig{Path: rootB}, nil)
	if err != nil {
		t.Fatalf("PublishMusicPath: %v", err)
	}
	if gen != 1 {
		t.Errorf("首次发布代际: got %d want 1", gen)
	}
	if svc.ScanGeneration() != 1 || svc.Scanner().GetMusicPath() != rootB {
		t.Errorf("内存 binding 未切换: gen=%d root=%q", svc.ScanGeneration(), svc.Scanner().GetMusicPath())
	}
	// 发布只负责清除「不属于新配置」的记录，不会替新根目录做导入（b.flac 尚不在库中）。
	paths := localSongPaths(t, mdb)
	if paths[songA] {
		t.Errorf("旧根目录歌曲在发布后仍存在（文件真实存在也应清除）: %s", songA)
	}
	if paths[songB] {
		t.Errorf("新根目录尚未扫描，不应提前出现记录: %s", songB)
	}

	var persisted MusicPathConfig
	if err := svc.configService.GetJSON(musicPathConfigKey, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Path != rootB || persisted.Generation != 1 {
		t.Fatalf("持久化的不是成功发布的那一代: %+v", persisted)
	}

	// 模拟服务重启：从持久化配置重建 Scanner，再扫一次，旧根歌曲不得复活。
	restartScanner := NewScanner(&ScanConfig{MusicPath: persisted.Path, SupportedFormats: []string{"flac"}})
	svc2 := NewSongService(mdb.SongRepository(), mdb, svc.metadataExtractor, restartScanner, svc.configService, nil)
	if err := svc2.ScanAndImportAsync(false, nil); err != nil {
		t.Fatal(err)
	}
	if p := waitScanTerminal(t, svc2); p.Status != ScanStatusCompleted {
		t.Fatalf("重启后扫描状态: %+v", p)
	}
	paths = localSongPaths(t, mdb)
	if paths[songA] {
		t.Errorf("重启恢复新一代配置后，旧根目录歌曲被新扫描带回: %s", songA)
	}
	if !paths[songB] {
		t.Errorf("新根目录歌曲在重启扫描后丢失: %s", songB)
	}
}

// TestPublishMusicPath_ExcludeRuleOnly 仅调整排除规则：同根目录下新进入排除范围的
// 记录必须清除，即使文件仍真实存在；其余记录保留。
func TestPublishMusicPath_ExcludeRuleOnly(t *testing.T) {
	root := t.TempDir()
	keep := filepath.Join(root, "keep", "k.flac")
	gone := filepath.Join(root, "secret", "s.flac")
	seedFlac(t, keep)
	seedFlac(t, gone)

	mdb, svc := newPublishTestService(t, root)
	if err := svc.ScanAndImportAsync(false, nil); err != nil {
		t.Fatal(err)
	}
	if p := waitScanTerminal(t, svc); p.Status != ScanStatusCompleted {
		t.Fatalf("扫描状态: %+v", p)
	}

	gen, err := svc.PublishMusicPath(context.Background(), MusicPathConfig{
		Path:        root,
		ExcludeDirs: []string{"secret"},
	}, nil)
	if err != nil {
		t.Fatalf("PublishMusicPath: %v", err)
	}
	if gen != 1 {
		t.Errorf("代际: got %d want 1", gen)
	}
	paths := localSongPaths(t, mdb)
	if paths[gone] {
		t.Errorf("进入排除范围的记录应被清除（文件仍存在）: %s", gone)
	}
	if !paths[keep] {
		t.Errorf("排除范围外的记录不应被动到: %s", keep)
	}
}

// TestPublishMusicPath_CueGroupsCleaned CUE 源脱离新根目录时整组记录必须清除。
func TestPublishMusicPath_CueGroupsCleaned(t *testing.T) {
	rootA := filepath.Join(t.TempDir(), "a")
	rootB := filepath.Join(t.TempDir(), "b")
	mdb, svc := newPublishTestService(t, rootA)
	repo := mdb.SongRepository()
	ctx := context.Background()

	cueSrc := filepath.Join(rootA, "disc.flac")
	for i := 1; i <= 3; i++ {
		if err := repo.Create(ctx, &models.Song{
			Type:          models.TypeLocal,
			Title:         "cue track",
			FilePath:      cueSrc,
			CueSourcePath: cueSrc,
			CueAudioPath:  cueSrc,
			CueTrackIndex: i,
			AddedAt:       time.Now(),
			UpdatedAt:     time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := svc.PublishMusicPath(ctx, MusicPathConfig{Path: rootB}, nil); err != nil {
		t.Fatalf("PublishMusicPath: %v", err)
	}
	srcs, err := repo.ListCueSources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(srcs) != 0 {
		t.Errorf("越界 CUE 来源组应被整组清除，剩余: %v", srcs)
	}
}

// failingListRepo 包装真实仓储但让 List 恒失败，用于模拟发布清理阶段的 DB 故障。
type failingListRepo struct {
	SongRepository
}

func (f *failingListRepo) List(ctx context.Context, filter *database.SongFilter) ([]*models.Song, error) {
	return nil, errors.New("simulated list failure")
}

// TestPublishMusicPath_CleanupFailureKeepsOldConfig 清理失败时接口报错，
// 旧配置继续完整可用：内存 Scanner/代际不变、持久化配置仍是旧代、闸门释放后可正常扫描。
func TestPublishMusicPath_CleanupFailureKeepsOldConfig(t *testing.T) {
	rootA := filepath.Join(t.TempDir(), "a")
	rootB := filepath.Join(t.TempDir(), "b")
	if err := os.MkdirAll(rootA, 0755); err != nil {
		t.Fatal(err)
	}

	mdb := testutil.OpenMemoryDB(t)
	cfgSvc := NewConfigService(mdb.ConfigRepository())
	if err := cfgSvc.SetJSON(musicPathConfigKey, MusicPathConfig{Path: rootA}); err != nil {
		t.Fatal(err)
	}
	scannerA := NewScanner(&ScanConfig{MusicPath: rootA, SupportedFormats: []string{"flac"}})
	extractor := NewMetadataExtractor(&MetadataConfig{FFProbePath: "ffprobe", CoverStoragePath: t.TempDir()})
	svc := NewSongService(&failingListRepo{SongRepository: mdb.SongRepository()}, mdb, extractor, scannerA, cfgSvc, nil)

	_, err := svc.PublishMusicPath(context.Background(), MusicPathConfig{Path: rootB}, nil)
	if err == nil {
		t.Fatal("清理失败时 PublishMusicPath 必须返回错误")
	}
	if svc.Scanner().GetMusicPath() != rootA || svc.ScanGeneration() != 0 {
		t.Errorf("失败后内存 binding 应保持旧代: root=%q gen=%d", svc.Scanner().GetMusicPath(), svc.ScanGeneration())
	}
	// 清理发生在落库之前：失败时持久化配置必须仍是旧值。
	var persisted MusicPathConfig
	if err := cfgSvc.GetJSON(musicPathConfigKey, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Path != rootA {
		t.Errorf("失败后持久化配置应为旧根目录, got %q", persisted.Path)
	}

	// 闸门已释放：扫描入口恢复可用（空目录扫描应正常完成）。
	svc2 := NewSongService(mdb.SongRepository(), mdb, extractor, scannerA, cfgSvc, nil)
	if err := svc2.ScanAndImportAsync(false, nil); err != nil {
		t.Fatalf("发布失败后扫描入口应恢复: %v", err)
	}
	if p := waitScanTerminal(t, svc2); p.Status != ScanStatusCompleted {
		t.Fatalf("旧配置下扫描状态: %+v", p)
	}
}

// blockingRun 模拟一个「正在跑、收到发布信号后才收尾」的旧扫描：
// publisher 关闭 switchCh 后本协程才能解除阻塞并 close(done)。
func registerBlockingRun(svc *SongService) (run *scanRun, observed chan struct{}, release chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	run = &scanRun{
		generation: 0,
		scanner:    svc.Scanner(),
		switchCh:   make(chan struct{}),
		done:       make(chan struct{}),
		ctx:        ctx,
		cancelFn:   cancel,
	}
	observed = make(chan struct{})
	release = make(chan struct{})
	svc.scanRegMu.Lock()
	svc.activeRun = run
	svc.scanRegMu.Unlock()
	go func() {
		<-run.switchCh
		close(observed)
		<-release
		cancel()
		close(run.done)
	}()
	return run, observed, release
}

// TestPublishMusicPath_StopsActiveRunBeforeBoundary 发布必须先停止并 join 旧扫描，
// join 完成前不得落库、不得切换 Scanner，且闸门期间拒绝新扫描；join 后统一生效。
func TestPublishMusicPath_StopsActiveRunBeforeBoundary(t *testing.T) {
	rootA := filepath.Join(t.TempDir(), "a")
	rootB := filepath.Join(t.TempDir(), "b")
	if err := os.MkdirAll(rootA, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rootB, 0755); err != nil {
		t.Fatal(err)
	}
	_, svc := newPublishTestService(t, rootA)

	run, observed, release := registerBlockingRun(svc)
	pubErr := make(chan error, 1)
	go func() {
		_, err := svc.PublishMusicPath(context.Background(), MusicPathConfig{Path: rootB}, nil)
		pubErr <- err
	}()

	select {
	case <-observed:
	case <-time.After(5 * time.Second):
		t.Fatal("发布流程应关闭旧扫描的 switchCh")
	}
	if !run.abandoned.Load() {
		t.Error("旧扫描应被标记 abandoned")
	}

	// join 完成前：旧配置仍在线，且新扫描被闸门拒绝。
	if svc.Scanner().GetMusicPath() != rootA {
		t.Errorf("join 完成前不得切换 Scanner, got %q", svc.Scanner().GetMusicPath())
	}
	var persisted MusicPathConfig
	_ = svc.configService.GetJSON(musicPathConfigKey, &persisted)
	if persisted.Path == rootB {
		t.Error("join 完成前不得落库新一代配置")
	}
	if err := svc.ScanAndImportAsync(false, nil); err == nil {
		t.Error("发布闸门期间应拒绝新扫描")
	}

	close(release)
	select {
	case err := <-pubErr:
		if err != nil {
			t.Fatalf("PublishMusicPath: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("发布流程在旧扫描收尾后未返回")
	}
	if svc.Scanner().GetMusicPath() != rootB || svc.ScanGeneration() != 1 {
		t.Errorf("发布后 binding 异常: root=%q gen=%d", svc.Scanner().GetMusicPath(), svc.ScanGeneration())
	}

	// 闸门释放：新扫描可以启动并使用新规则（空根目录，快速完成）。
	if err := svc.ScanAndImportAsync(false, nil); err != nil {
		t.Fatalf("发布完成后应允许新扫描: %v", err)
	}
	if p := waitScanTerminal(t, svc); p.Status != ScanStatusCompleted {
		t.Fatalf("新一代扫描状态: %+v", p)
	}
}

// TestAbandonedRun_DoesNotWriteBatches 已被发布边界废弃的批次即使到达 flush 收口点，
// 也不得写库、不得更新进度。
func TestAbandonedRun_DoesNotWriteBatches(t *testing.T) {
	mdb := testutil.OpenMemoryDB(t)
	svc := NewSongService(mdb.SongRepository(), mdb, nil, NewScanner(&ScanConfig{MusicPath: t.TempDir()}), nil, nil)
	run := newDirectScanRun(t, svc.Scanner())
	run.abandoned.Store(true)

	batch := []scanExtractResult{{
		item:     scanProcessItem{filePath: filepath.Join(t.TempDir(), "x.flac")},
		metadata: &Metadata{Title: "should-not-exist", Format: "flac"},
	}}
	svc.flushScanBatch(context.Background(), run, batch)

	n, err := mdb.SongRepository().Count(context.Background(), &database.SongFilter{Type: models.TypeLocal})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("废弃批次写入了 %d 条记录，应为 0", n)
	}
}

// TestScannerOwnsPath 直接覆盖新规则判定的边界情形。
func TestScannerOwnsPath(t *testing.T) {
	sep := string(filepath.Separator)
	root := filepath.Clean(sep + "music")
	sc := func(excludeDirs, excludePaths []string) *Scanner {
		return NewScanner(&ScanConfig{MusicPath: root, ExcludeDirs: excludeDirs, ExcludePaths: excludePaths})
	}

	if !sc(nil, nil).OwnsPath(filepath.Join(root, "a.flac")) {
		t.Error("根目录下的普通文件应属于配置")
	}
	if sc(nil, nil).OwnsPath(filepath.Join(sep+"other", "a.flac")) {
		t.Error("根目录之外的文件不应属于配置")
	}
	if sc([]string{"secret"}, nil).OwnsPath(filepath.Join(root, "secret", "a.flac")) {
		t.Error("排除名称命中目录中的文件不应属于配置")
	}
	excluded := filepath.Join(root, "noscan")
	if sc(nil, []string{excluded}).OwnsPath(filepath.Join(excluded, "a.flac")) {
		t.Error("排除路径命中的文件不应属于配置")
	}
}
