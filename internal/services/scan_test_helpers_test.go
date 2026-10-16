package services

import (
	"context"
	"testing"
)

// newDirectScanRun 构造一个不走 ScanAndImportAsync 注册流程的 scanRun，
// 供直接调用 doScanAndImport 的测试使用（无用户取消通道、代际 0、未发布）。
func newDirectScanRun(t *testing.T, scanner *Scanner) *scanRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &scanRun{
		scanner:  scanner,
		switchCh: make(chan struct{}),
		done:     make(chan struct{}),
		ctx:      ctx,
		cancelFn: cancel,
	}
}
