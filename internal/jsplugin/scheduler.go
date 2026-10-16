package jsplugin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// MessageType 消息类型
type MessageType int

const (
	MsgHTTPRequest      MessageType = iota // HTTP 路由请求
	MsgTimerFire                           // 定时器触发
	MsgInterPlugin                         // 插件间通信
	MsgLifecycle                           // 生命周期事件（init/deinit）
	MsgHostCall                            // 宿主函数调用结果
	MsgHealthCheck                         // 健康检查
	MsgPlayEvent                           // 播放事件（play/finish/skip）
	MsgNetData                             // 网络数据事件（UDP 收包推送）
	MsgWebSocketOpen                       // 入站 WebSocket 已建立
	MsgWebSocketMessage                    // 入站 WebSocket 收到消息
	MsgWebSocketClose                      // 入站 WebSocket 已关闭
)

const (
	defaultQueueSize   = 256
	defaultCallTimeout = 30 * time.Second
	// maxCallTimeout 为插件 HTTP 调用可被显式放宽到的上限（见 forwardToJSRuntime
	// 的 X-Plugin-Timeout-Ms 头）。与 yt-dlp 等长任务的 command.exec 5min 预算对齐，
	// 防止调用方任意放大导致单 worker 被长时间占用。
	maxCallTimeout = 300 * time.Second
)

// 错误定义
var (
	ErrSchedulerClosed     = errors.New("scheduler: closed")
	ErrServiceNotFound     = errors.New("scheduler: service not found")
	ErrServiceExists       = errors.New("scheduler: service already registered")
	ErrQueueFull           = errors.New("scheduler: message queue full (backpressure)")
	ErrCallTimeout         = errors.New("scheduler: call timeout")
	ErrServiceUnregistered = errors.New("scheduler: service unregistered during call")
	// ErrDrainTimeout 表示在限定时间内未能排空插件队列中已排队/处理中的消息。
	// 插件卸载用它作为「仍在运行、不得继续破坏性删除」的可观察信号。
	ErrDrainTimeout = errors.New("scheduler: drain timed out, service still processing")
)

const defaultDrainTimeout = 10 * time.Second

// Message 统一消息格式（借鉴 Skynet 的 session + type 机制）
type Message struct {
	ID       uint64          // 全局递增消息 ID
	Type     MessageType     // 消息类型
	Source   string          // 发送方 service name（空字符串表示系统消息）
	Target   string          // 目标 service name
	Session  uint64          // 请求-响应配对 session
	Data     interface{}     // 消息体（具体类型由 MessageType 决定）
	RespChan chan *Message   // 同步调用的响应通道（nil 表示异步/不需要响应）
	Ctx      context.Context // 请求上下文（用于超时控制）
}

// MessageHandler 消息处理接口
type MessageHandler interface {
	HandleMessage(msg *Message) *Message // 处理消息，返回响应（可为 nil）
}

// serviceEntry 内部服务条目
type serviceEntry struct {
	name     string
	msgQueue chan *Message
	handler  MessageHandler
	cancel   context.CancelFunc
	done     chan struct{} // worker 退出信号
	closed   atomic.Bool   // 标记是否已关闭入口
	// sendMu 串行化「closed 检查 → 入队」与「关闭队列」，杜绝向已关闭 channel
	// 发送导致的 panic（dispatch 不持注册表锁，Unregister 可能并发发生）。
	sendMu sync.Mutex
	// inflight 跟踪所有「已入队但尚未处理完」的消息。DrainService 靠它等待
	// 队列排空与运行中的调用结束。刻意不用 sync.WaitGroup：排空超时后服务要
	// unfreeze 恢复接客，WaitGroup 不允许零计数后再 Add 与旧 Wait 并发。
	inflight inflightTracker
}

// inflightTracker 是带「归零通知」的在途消息计数器，支持超时等待后被放弃、
// 之后继续计数（不像 sync.WaitGroup 那样有 Wait/Add 复用约束）。
type inflightTracker struct {
	mu     sync.Mutex
	n      int64
	zeroCh chan struct{} // 非 nil 表示有等待者；n 归零时关闭它
}

// add 调整在途计数；归零时通知当前等待者（若有）。
func (t *inflightTracker) add(delta int64) {
	t.mu.Lock()
	t.n += delta
	if t.n == 0 && t.zeroCh != nil {
		close(t.zeroCh)
		t.zeroCh = nil
	}
	t.mu.Unlock()
}

// zeroChannel 返回一个在计数归零时关闭的 channel；计数已为 0 时返回已关闭 channel。
func (t *inflightTracker) zeroChannel() chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.n == 0 {
		return closedChan
	}
	if t.zeroCh == nil {
		t.zeroCh = make(chan struct{})
	}
	return t.zeroCh
}

// closedChan 是进程级共享的已关闭 channel。
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// freeze 拒绝新消息入队，但保留注册与 worker：排空超时可以 unfreeze 恢复服务，
// 成功则由后续 shutdown/Unregister 关队列。
func (e *serviceEntry) freeze() {
	e.sendMu.Lock()
	e.closed.Store(true)
	e.sendMu.Unlock()
}

// unfreeze 恢复接收新消息（排空超时回滚用）。
func (e *serviceEntry) unfreeze() {
	e.sendMu.Lock()
	e.closed.Store(false)
	e.sendMu.Unlock()
}

// shutdown 拒绝新消息并关闭队列。必须与 tryEnqueue 在同一把 sendMu 下配对，
// 否则 dispatch 可能在 closed 检查之后撞上 close。
func (e *serviceEntry) shutdown() {
	e.sendMu.Lock()
	e.closed.Store(true)
	close(e.msgQueue)
	e.sendMu.Unlock()
}

// tryEnqueue 在入口开放时把消息放入队列并计入 inflight。
// 入口关闭返回 ErrServiceUnregistered，队列满返回 ErrQueueFull。
func (e *serviceEntry) tryEnqueue(msg *Message) error {
	e.sendMu.Lock()
	defer e.sendMu.Unlock()

	if e.closed.Load() {
		return fmt.Errorf("%w: %q", ErrServiceUnregistered, e.name)
	}

	// 先计数再入队：若先入队后计数，DrainService 可能在消息刚排队时误判已排空。
	e.inflight.add(1)
	select {
	case e.msgQueue <- msg:
		return nil
	default:
		e.inflight.add(-1)
		return fmt.Errorf("%w: target=%q, queue capacity=%d", ErrQueueFull, e.name, cap(e.msgQueue))
	}
}

// ServiceScheduler 借鉴 Skynet 的消息分发机制
type ServiceScheduler struct {
	services   map[string]*serviceEntry
	mu         sync.RWMutex
	msgIDSeq   atomic.Uint64
	sessionSeq atomic.Uint64
	workerSize int
	closed     atomic.Bool
}

// NewServiceScheduler 创建调度器
func NewServiceScheduler(workerSize int) *ServiceScheduler {
	if workerSize <= 0 {
		workerSize = 1
	}
	return &ServiceScheduler{
		services:   make(map[string]*serviceEntry),
		workerSize: workerSize,
	}
}

// RegisterService 注册一个服务
func (s *ServiceScheduler) RegisterService(name string, handler MessageHandler, queueSize int) error {
	if s.closed.Load() {
		return ErrSchedulerClosed
	}
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.services[name]; exists {
		return ErrServiceExists
	}

	ctx, cancel := context.WithCancel(context.Background())
	entry := &serviceEntry{
		name:     name,
		msgQueue: make(chan *Message, queueSize),
		handler:  handler,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	s.services[name] = entry

	// 启动 worker goroutine
	go s.runWorker(ctx, entry)

	return nil
}

// runWorker 服务的 worker 协程，串行处理消息
func (s *ServiceScheduler) runWorker(ctx context.Context, entry *serviceEntry) {
	defer close(entry.done)

	for {
		select {
		case msg, ok := <-entry.msgQueue:
			if !ok {
				// 关队列时仍排队的消息不会再被任何消费者处理（同步调用方会拿到
				// ErrCallTimeout/ErrServiceUnregistered），按剩余长度一次性结清
				// inflight，避免计数泄漏导致 DrainService 永远等不到归零。
				if n := len(entry.msgQueue); n > 0 {
					entry.inflight.add(-int64(n))
				}
				return
			}
			s.processMessage(entry, msg)
		case <-ctx.Done():
			// 上下文取消，排空剩余消息
			s.drainQueue(entry)
			return
		}
	}
}

// processMessage 处理单条消息
func (s *ServiceScheduler) processMessage(entry *serviceEntry, msg *Message) {
	// 与 tryEnqueue 的计数配对：无论跳过、处理成功还是 handler panic（recover
	// 由上层保证），退出都要减一。
	defer entry.inflight.add(-1)

	// 客户端在排队期间已经放弃（如用户快速切歌触发 abort）的请求，直接跳过，
	// 避免 worker 被串行化的 ExecuteJS 卡住，新切的歌排在它后面一直 pending。
	// 已取消的消息往往 RespChan 也无人接收（Call 已 return），无需回填 resp。
	if msg.Ctx != nil && msg.Ctx.Err() != nil {
		if msg.RespChan != nil {
			// 兜底回个 nil，防止某些等待者卡死；Call 端已被 callCtx 唤醒，
			// 不会真正消费这里的值。
			select {
			case msg.RespChan <- nil:
			default:
			}
		}
		return
	}

	resp := entry.handler.HandleMessage(msg)
	if msg.RespChan != nil {
		select {
		case msg.RespChan <- resp:
		default:
			// 响应通道已满或无人接收，丢弃
		}
	}
}

// drainQueue 排空消息队列中的剩余消息
func (s *ServiceScheduler) drainQueue(entry *serviceEntry) {
	for {
		select {
		case msg, ok := <-entry.msgQueue:
			if !ok {
				return
			}
			s.processMessage(entry, msg)
		default:
			return
		}
	}
}

// DrainService 先冻结服务（拒绝新消息入队），再等待所有已排队/处理中的消息排空。
//
// 与 UnregisterService 的区别：不摘注册表、不关队列——排空超时时可以 unfreeze
// 让服务恢复正常接客，调用方据此中止卸载（不允许在插件仍在执行时删包删库）。
// 排空成功后调用方应紧接着 UnregisterService，期间新消息仍被拒绝。
//
// timeout<=0 使用 defaultDrainTimeout。
func (s *ServiceScheduler) DrainService(name string, timeout time.Duration) error {
	s.mu.RLock()
	entry, exists := s.services[name]
	s.mu.RUnlock()
	if !exists {
		return ErrServiceNotFound
	}

	entry.freeze()

	if timeout <= 0 {
		timeout = defaultDrainTimeout
	}
	select {
	case <-entry.inflight.zeroChannel():
		return nil
	case <-time.After(timeout):
		// 仍有调用在运行：放开入口，插件继续正常服务，本次卸载中止。
		// 放弃的等待者不影响计数器（zeroCh 归零时自然关闭，无人接收即可）。
		entry.unfreeze()
		return fmt.Errorf("%w: %q", ErrDrainTimeout, name)
	}
}

// UnregisterService 注销服务（关队列并等待 worker 退出，超时则强制取消）。
// 优雅卸载应先 DrainService 排空，成功后再调本方法。
func (s *ServiceScheduler) UnregisterService(name string, timeout time.Duration) error {
	s.mu.Lock()
	entry, exists := s.services[name]
	if !exists {
		s.mu.Unlock()
		return ErrServiceNotFound
	}
	// 从注册表移除
	delete(s.services, name)
	s.mu.Unlock()

	// 拒绝新消息并关闭队列（sendMu 保护，杜绝并发 send on closed channel）
	entry.shutdown()

	// 等待 worker 完成或超时
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
	select {
	case <-entry.done:
		return nil
	case <-time.After(timeout):
		entry.cancel()
		<-entry.done

		return fmt.Errorf("scheduler: unregister %q timed out, worker force stopped", name)
	}
}

// Send 异步发送消息（不等待响应）
func (s *ServiceScheduler) Send(msg *Message) error {
	if s.closed.Load() {
		return ErrSchedulerClosed
	}
	msg.ID = s.msgIDSeq.Add(1)
	msg.RespChan = nil // 异步发送不需要响应通道
	return s.dispatch(msg)
}

// Call 同步调用（等待响应或超时）
// timeout 为 0 时使用默认 30s 超时
func (s *ServiceScheduler) Call(ctx context.Context, target, source string, msgType MessageType, data interface{}, timeout time.Duration) (*Message, error) {
	if s.closed.Load() {
		return nil, ErrSchedulerClosed
	}
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	session := s.sessionSeq.Add(1)
	respChan := make(chan *Message, 1)

	msg := &Message{
		ID:       s.msgIDSeq.Add(1),
		Type:     msgType,
		Source:   source,
		Target:   target,
		Session:  session,
		Data:     data,
		RespChan: respChan,
		Ctx:      callCtx,
	}

	if err := s.dispatch(msg); err != nil {
		return nil, err
	}

	select {
	case resp := <-respChan:
		return resp, nil
	case <-callCtx.Done():
		return nil, ErrCallTimeout
	}
}

// dispatch 投递消息到目标服务队列
func (s *ServiceScheduler) dispatch(msg *Message) error {
	s.mu.RLock()
	entry, exists := s.services[msg.Target]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("%w: %q", ErrServiceNotFound, msg.Target)
	}

	return entry.tryEnqueue(msg)
}

// HasService 检查服务是否存在
func (s *ServiceScheduler) HasService(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.services[name]
	return exists
}

// ServiceNames 返回所有已注册服务名（排序后返回）
func (s *ServiceScheduler) ServiceNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.services))
	for name := range s.services {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Close 关闭调度器（停止所有 worker，排空消息队列）
func (s *ServiceScheduler) Close() error {
	if s.closed.Swap(true) {
		return ErrSchedulerClosed // 已经关闭过
	}

	s.mu.Lock()
	entries := make([]*serviceEntry, 0, len(s.services))
	for _, entry := range s.services {
		entries = append(entries, entry)
	}
	// 清空注册表
	s.services = make(map[string]*serviceEntry)
	s.mu.Unlock()

	// 关闭所有服务
	for _, entry := range entries {
		entry.shutdown()
	}

	// 等待所有 worker 退出
	timeout := time.After(10 * time.Second)
	for _, entry := range entries {
		select {
		case <-entry.done:
		case <-timeout:
			entry.cancel()
		}
	}

	return nil
}
