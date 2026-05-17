// Package mux 提供 TCP 连接多路复用
//
// SSH 风格包处理：长度前缀帧 + 窗口流控 + 通道状态机。
// 协议格式：[2B magic 0xAE01] [2B port] [2B len] [2B type] [data]
//
// 包类型（type 字段）：
//   0x0000 = DATA          数据帧
//   0x0001 = OPEN          打开通道
//   0x0002 = CLOSE         关闭通道
//   0x0003 = WINDOW_UPDATE  窗口更新（接收方消费后通知）
package mux

import (
	alog "Aether/common/log"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Magic           = 0xAE01
	FrameHeaderSize = 8      // 2B magic + 2B port + 2B len + 2B type
	MaxFrameSize    = 65535
	workerCount     = 4
	windowSize      = 1024 * 1024 // 1MB 流控窗口
	sendQueueSize   = 64          // 每通道发送队列大小
)

// 包类型
const (
	PktData         uint16 = 0x0000
	PktOpen         uint16 = 0x0001
	PktClose        uint16 = 0x0002
	PktWindowUpdate uint16 = 0x0003
)

// frameOut 待发送的帧
type frameOut struct {
	port  uint16
	data  []byte
	srcFd int // splice 源 fd，-1 表示普通写入
	n     int // splice 字节数
	ctrl  bool // 控制帧（不参与流控）
}

// frameIn 接收到的帧
type frameIn struct {
	port  uint16
	ptype uint16
	data  []byte
}

// Channel 虚拟通道
type Channel struct {
	Port      uint16
	DataChan  chan []byte
	CloseChan chan struct{}
	Mux       *Multiplexer
	closeOnce sync.Once

	// 流控
	sendQueue  chan frameOut // 发送队列
	sendWindow int32         // 可用发送窗口（字节）
}

// Multiplexer 多路复用器
type Multiplexer struct {
	conn         net.Conn
	connFdFile   *os.File
	channels     map[uint16]*Channel
	lock         sync.RWMutex // 保护 channels
	writeLock    sync.Mutex   // 保护 conn 写入
	workChan     chan frameIn
	closed       atomic.Bool
	closeOnce    sync.Once
	closeChan    chan struct{}
	writeNotify  chan struct{} // 通知 writeLoop 有新数据
	spliceAvail  bool
	LocalTarget  string
	OnNewChannel func(ch *Channel)
}

// New 创建多路复用器
func New(conn net.Conn) *Multiplexer {
	m := &Multiplexer{
		conn:        conn,
		channels:    make(map[uint16]*Channel),
		workChan:    make(chan frameIn, 64),
		closeChan:   make(chan struct{}),
		writeNotify: make(chan struct{}, 1),
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		tcpConn.SetNoDelay(true)
		tcpConn.SetReadBuffer(256 * 1024)
		tcpConn.SetWriteBuffer(256 * 1024)

		if f, err := tcpConn.File(); err == nil {
			m.connFdFile = f
		}
	}

	m.spliceAvail = checkSpliceAvail(conn)

	go m.readLoop()
	go m.writeLoop()
	for i := 0; i < workerCount; i++ {
		go m.worker()
	}
	return m
}

// notifyWrite 通知 writeLoop 有新数据
func (m *Multiplexer) notifyWrite() {
	select {
	case m.writeNotify <- struct{}{}:
	default:
	}
}

// writeLoop 写入 TCP：轮询所有通道的发送队列
func (m *Multiplexer) writeLoop() {
	idleTimer := time.NewTimer(time.Second)
	defer idleTimer.Stop()

	for {
		if m.closed.Load() {
			return
		}

		// 收集所有通道
		m.lock.RLock()
		channels := make([]*Channel, 0, len(m.channels))
		for _, ch := range m.channels {
			channels = append(channels, ch)
		}
		m.lock.RUnlock()

		if len(channels) == 0 {
			// 无通道，等待通知或超时
			select {
			case <-m.writeNotify:
				continue
			case <-m.closeChan:
				return
			case <-idleTimer.C:
				idleTimer.Reset(time.Second)
				continue
			}
		}

		madeProgress := false
		for _, ch := range channels {
			select {
			case frame := <-ch.sendQueue:
				m.writeLock.Lock()
				var err error
				if frame.srcFd >= 0 {
					err = m.writeFrameSplice(frame.port, frame.srcFd, frame.n)
				} else {
					err = m.writeFrame(frame.port, frame.data, frame.ctrl)
				}
				m.writeLock.Unlock()
				if err != nil {
				if !m.closed.Load() {
					alog.Error(alog.CatMux, "mux write error", "error", err)
				}
					m.Close()
					return
				}
				madeProgress = true
			default:
			}
		}

		if !madeProgress {
			select {
			case <-m.writeNotify:
			case <-m.closeChan:
				return
			}
		}
	}
}

// writeFrame 写入一帧到 TCP
func (m *Multiplexer) writeFrame(port uint16, data []byte, ctrl bool) error {
	dataLen := len(data)
	if dataLen > MaxFrameSize {
		return fmt.Errorf("data too large: %d", dataLen)
	}

	ptype := PktData
	if ctrl {
		ptype = PktOpen // 具体类型由调用方通过 data 编码
	}

	buf := make([]byte, FrameHeaderSize+dataLen)
	binary.BigEndian.PutUint16(buf[0:2], Magic)
	binary.BigEndian.PutUint16(buf[2:4], port)
	binary.BigEndian.PutUint16(buf[4:6], uint16(dataLen))
	binary.BigEndian.PutUint16(buf[6:8], ptype)
	copy(buf[FrameHeaderSize:], data)

	for written := 0; written < len(buf); {
		n, err := m.conn.Write(buf[written:])
		written += n
		if err != nil {
			return err
		}
	}
	return nil
}

// writeFrameSplice splice 零拷贝写入（Linux only）
func (m *Multiplexer) writeFrameSplice(port uint16, srcFd int, n int) error {
	if n > MaxFrameSize {
		n = MaxFrameSize
	}

	// 先 splice 到 pipe，获取实际字节数
	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create pipe: %w", err)
	}
	defer pr.Close()

	spliced, err := spliceFd(srcFd, int(pw.Fd()), n)
	if err != nil {
		pw.Close()
		return fmt.Errorf("splice src→pipe: %w", err)
	}
	if spliced == 0 {
		pw.Close()
		return fmt.Errorf("splice: EOF")
	}
	pw.Close() // 关闭写端，让读端知道数据已写完

	// 写正确的 header（使用实际字节数）
	header := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint16(header[0:2], Magic)
	binary.BigEndian.PutUint16(header[2:4], port)
	binary.BigEndian.PutUint16(header[4:6], uint16(spliced))
	binary.BigEndian.PutUint16(header[6:8], PktData)

	if _, err := m.conn.Write(header); err != nil {
		return err
	}

	// splice pipe → conn
	_, err = spliceFd(int(pr.Fd()), m.connFd(), int(spliced))
	return err
}

func (m *Multiplexer) connFd() int {
	if m.connFdFile != nil {
		return int(m.connFdFile.Fd())
	}
	return -1
}

// SpliceAvailable 是否支持 splice
func (m *Multiplexer) SpliceAvailable() bool {
	return m.spliceAvail
}

// readLoop 读取 TCP 数据，解析帧，分发到 workChan
func (m *Multiplexer) readLoop() {
	defer func() {
		m.lock.Lock()
		for _, ch := range m.channels {
			ch.closeOnce.Do(func() { close(ch.CloseChan) })
		}
		m.channels = make(map[uint16]*Channel)
		m.lock.Unlock()
		m.Close()
	}()

	header := make([]byte, FrameHeaderSize)
	for {
		if _, err := io.ReadFull(m.conn, header); err != nil {
			if !m.closed.Load() {
				alog.Error(alog.CatMux, "mux read header error", "error", err)
			}
			return
		}

		magic := binary.BigEndian.Uint16(header[0:2])
		if magic != Magic {
			alog.Error(alog.CatMux, "mux invalid magic", "magic", fmt.Sprintf("0x%04x", magic))
			return
		}

		port := binary.BigEndian.Uint16(header[2:4])
		dataLen := binary.BigEndian.Uint16(header[4:6])
		ptype := binary.BigEndian.Uint16(header[6:8])

		if dataLen > MaxFrameSize {
			alog.Error(alog.CatMux, "mux frame too large", "size", dataLen)
			return
		}

		var data []byte
		if dataLen > 0 {
			data = make([]byte, dataLen)
			if _, err := io.ReadFull(m.conn, data); err != nil {
				alog.Error(alog.CatMux, "mux read data error", "error", err)
				return
			}
		}

		select {
		case m.workChan <- frameIn{port: port, ptype: ptype, data: data}:
		case <-m.closeChan:
			return
		}
	}
}

// worker 消费者
func (m *Multiplexer) worker() {
	for {
		select {
		case frame, ok := <-m.workChan:
			if !ok {
				return
			}
			m.dispatch(frame.port, frame.ptype, frame.data)
		case <-m.closeChan:
			return
		}
	}
}

// dispatch 分发帧到对应通道
func (m *Multiplexer) dispatch(port uint16, ptype uint16, data []byte) {
	switch ptype {
	case PktData:
		m.dispatchData(port, data)
	case PktClose:
		m.closeChannelLocal(port)
	case PktWindowUpdate:
		m.handleWindowUpdate(port, data)
	case PktOpen:
		// OPEN 帧由服务端处理，客户端忽略
	default:
		alog.Warn(alog.CatMux, "mux unknown packet type", "type", fmt.Sprintf("0x%04x", ptype))
	}
}

// dispatchData 分发数据帧到通道
func (m *Multiplexer) dispatchData(port uint16, data []byte) {
	m.lock.RLock()
	ch, ok := m.channels[port]
	m.lock.RUnlock()

	if ok {
		select {
		case ch.DataChan <- data:
		case <-ch.CloseChan:
		case <-m.closeChan:
		}
		return
	}

	// 通道不存在，创建新通道
	if m.OnNewChannel == nil {
		return
	}

	m.lock.Lock()
	// 双重检查
	if _, exists := m.channels[port]; exists {
		m.lock.Unlock()
		// 已存在，重新分发
		m.dispatchData(port, data)
		return
	}
	ch = &Channel{
		Port:       port,
		DataChan:   make(chan []byte, 32),
		CloseChan:  make(chan struct{}),
		Mux:        m,
		sendQueue:  make(chan frameOut, sendQueueSize),
		sendWindow: windowSize,
	}
	m.channels[port] = ch
	m.lock.Unlock()

	go m.OnNewChannel(ch)

	select {
	case ch.DataChan <- data:
	case <-ch.CloseChan:
	case <-m.closeChan:
	}
}

// handleWindowUpdate 处理窗口更新帧
func (m *Multiplexer) handleWindowUpdate(port uint16, data []byte) {
	if len(data) < 4 {
		return
	}
	addWindow := int32(binary.BigEndian.Uint32(data[:4]))

	m.lock.RLock()
	ch, ok := m.channels[port]
	m.lock.RUnlock()

	if ok {
		atomic.AddInt32(&ch.sendWindow, addWindow)
	}
}

// enqueueFrame 入队发送帧
func (m *Multiplexer) enqueueFrame(ch *Channel, frame frameOut) error {
	select {
	case ch.sendQueue <- frame:
		m.notifyWrite()
		return nil
	case <-m.closeChan:
		return fmt.Errorf("multiplexer closed")
	}
}

// Send 发送数据到指定端口（带流控）
func (m *Multiplexer) Send(port uint16, data []byte) error {
	if m.closed.Load() {
		return fmt.Errorf("multiplexer closed")
	}

	m.lock.RLock()
	ch, ok := m.channels[port]
	m.lock.RUnlock()

	if !ok {
		return fmt.Errorf("channel %d not found", port)
	}

	// 流控：等待窗口（带超时避免 busy-wait）
	dataLen := int32(len(data))
	for atomic.LoadInt32(&ch.sendWindow) < dataLen {
		if m.closed.Load() {
			return fmt.Errorf("multiplexer closed")
		}
		// 短暂等待后重试
		time.Sleep(100 * time.Microsecond)
	}

	// 扣减窗口
	atomic.AddInt32(&ch.sendWindow, -dataLen)

	// 复制数据
	buf := make([]byte, len(data))
	copy(buf, data)

	return m.enqueueFrame(ch, frameOut{port: port, data: buf, srcFd: -1})
}

// SpliceSend 零拷贝发送（Linux only）
func (m *Multiplexer) SpliceSend(port uint16, srcFd int, n int) error {
	if m.closed.Load() {
		return fmt.Errorf("multiplexer closed")
	}
	if !m.spliceAvail {
		return fmt.Errorf("splice not available")
	}
	if n > MaxFrameSize {
		n = MaxFrameSize
	}

	m.lock.RLock()
	ch, ok := m.channels[port]
	m.lock.RUnlock()

	if !ok {
		return fmt.Errorf("channel %d not found", port)
	}

	n32 := int32(n)
	for atomic.LoadInt32(&ch.sendWindow) < n32 {
		if m.closed.Load() {
			return fmt.Errorf("multiplexer closed")
		}
		time.Sleep(100 * time.Microsecond)
	}

	atomic.AddInt32(&ch.sendWindow, -n32)

	return m.enqueueFrame(ch, frameOut{port: port, data: nil, srcFd: srcFd, n: n})
}

// sendCtrlFrame 发送控制帧（通过 writeLock 保护写入）
func (m *Multiplexer) sendCtrlFrame(port uint16, ptype uint16) error {
	if m.closed.Load() {
		return fmt.Errorf("multiplexer closed")
	}

	m.writeLock.Lock()
	defer m.writeLock.Unlock()

	if m.closed.Load() {
		return fmt.Errorf("multiplexer closed")
	}

	frame := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint16(frame[0:2], Magic)
	binary.BigEndian.PutUint16(frame[2:4], port)
	binary.BigEndian.PutUint16(frame[4:6], 0)
	binary.BigEndian.PutUint16(frame[6:8], ptype)

	for written := 0; written < len(frame); {
		n, err := m.conn.Write(frame[written:])
		written += n
		if err != nil {
			return err
		}
	}
	return nil
}

// OpenChannel 打开通道并发送 OPEN 帧
func (m *Multiplexer) OpenChannel(port uint16) (*Channel, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.closed.Load() {
		return nil, fmt.Errorf("multiplexer closed")
	}
	if ch, exists := m.channels[port]; exists {
		return ch, nil
	}
	ch := &Channel{
		Port:       port,
		DataChan:   make(chan []byte, 32),
		CloseChan:  make(chan struct{}),
		Mux:        m,
		sendQueue:  make(chan frameOut, sendQueueSize),
		sendWindow: windowSize,
	}
	m.channels[port] = ch

	// 发送 OPEN 帧
	go m.sendCtrlFrame(port, PktOpen)

	return ch, nil
}

// CloseChannel 关闭通道并发送 CLOSE 帧
func (m *Multiplexer) CloseChannel(port uint16) {
	m.closeChannelLocal(port)

	if !m.closed.Load() {
		go m.sendCtrlFrame(port, PktClose)
	}
}

// closeChannelLocal 本地关闭通道（不发送 CLOSE 帧）
func (m *Multiplexer) closeChannelLocal(port uint16) {
	m.lock.Lock()
	ch, ok := m.channels[port]
	if ok {
		delete(m.channels, port)
	}
	m.lock.Unlock()

	if ok {
		ch.closeOnce.Do(func() { close(ch.CloseChan) })
	}
}

// SendWindowUpdate 发送窗口更新帧（消费数据后调用）
func (m *Multiplexer) SendWindowUpdate(port uint16, consumed int) error {
	return m.sendCtrlFrame(port, PktWindowUpdate)
}

// Close 关闭多路复用器
func (m *Multiplexer) Close() {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		close(m.closeChan)
		if m.connFdFile != nil {
			m.connFdFile.Close()
		}
		m.conn.Close()
	})
}

// Done 返回关闭信号
func (m *Multiplexer) Done() <-chan struct{} {
	return m.closeChan
}

// GetConn 获取底层连接
func (m *Multiplexer) GetConn() net.Conn {
	return m.conn
}

// Receive 非阻塞接收
func (ch *Channel) Receive() ([]byte, bool) {
	select {
	case data := <-ch.DataChan:
		return data, true
	case <-ch.CloseChan:
		return nil, false
	default:
		return nil, false
	}
}

// ReceiveBlocking 阻塞接收，优先排空缓冲区
func (ch *Channel) ReceiveBlocking() ([]byte, bool) {
	select {
	case data := <-ch.DataChan:
		return data, true
	case <-ch.CloseChan:
		// 排空剩余数据
		for {
			select {
			case data := <-ch.DataChan:
				return data, true
			default:
				return nil, false
			}
		}
	}
}

// HandleChannel 处理通道：连接本地服务，双向转发（客户端使用）
func (m *Multiplexer) HandleChannel(ch *Channel) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatTunnel, "PANIC in HandleChannel", "port", ch.Port, "error", r)
		}
		m.CloseChannel(ch.Port)
	}()

	localConn, err := net.Dial("tcp", m.LocalTarget)
	if err != nil {
		alog.Error(alog.CatTunnel, "failed to connect to local", "port", ch.Port, "target", m.LocalTarget, "error", err)
		return
	}

	alog.Info(alog.CatTunnel, "connected to local", "port", ch.Port, "target", m.LocalTarget)

	var wg sync.WaitGroup
	wg.Add(2)

	// 隧道 → 本地
	go func() {
		defer wg.Done()
		defer localConn.Close()
		for {
			data, ok := ch.ReceiveBlocking()
			if !ok {
				return
			}
			if _, err := localConn.Write(data); err != nil {
				if err != io.EOF {
					alog.Error(alog.CatTunnel, "local write error", "port", ch.Port, "error", err)
				}
				return
			}
			// 消费数据后发送窗口更新
			m.SendWindowUpdate(ch.Port, len(data))
		}
	}()

	// 本地 → 隧道
	go func() {
		defer wg.Done()

		// 尝试 splice
		if tc, ok := localConn.(*net.TCPConn); ok && m.SpliceAvailable() {
			if f, err := tc.File(); err == nil {
				fd := int(f.Fd())
				for {
					if err := m.SpliceSend(ch.Port, fd, MaxFrameSize); err != nil {
						break
					}
				}
				f.Close()
				return
			}
		}
		// 回退：Read→Send
		buf := make([]byte, MaxFrameSize)
		for {
			n, err := localConn.Read(buf)
			if err != nil {
				if err != io.EOF {
					alog.Error(alog.CatTunnel, "local read error", "port", ch.Port, "error", err)
				}
				return
			}
			if err := m.Send(ch.Port, buf[:n]); err != nil {
				alog.Error(alog.CatTunnel, "send error", "port", ch.Port, "error", err)
				return
			}
		}
	}()

	wg.Wait()
	alog.Info(alog.CatTunnel, "port closed", "port", ch.Port)
}
