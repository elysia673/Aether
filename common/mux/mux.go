// Package mux 提供 TCP 连接多路复用
//
// 在单个 TCP 连接上模拟多个虚拟通道，用于隧道传输。
// 协议格式：[2字节通道ID] [2字节数据长度] [数据]
// 通道ID=0 为控制通道，用于新建/关闭通道。
package mux

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// 帧协议常量
const (
	FrameHeaderSize = 4     // 帧头大小：2字节通道ID + 2字节数据长度
	MaxFrameSize    = 65535 // 最大帧数据大小
	ControlChanID   = 0     // 控制通道 ID
)

// Frame 多路复用帧
type Frame struct {
	ChanID  uint16
	Data    []byte
	IsClose bool
}

// Channel 虚拟通道
//
// 每个通道对应一个独立的逻辑连接。
type Channel struct {
	ID         uint16
	DataChan   chan []byte   // 数据接收缓冲区
	CloseChan  chan struct{} // 关闭信号
	RemoteAddr net.Addr      // 远端地址
	Mux        *Multiplexer  // 所属多路复用器
	closeOnce  sync.Once
}

// Multiplexer 多路复用器
//
// 在单个 net.Conn 上管理多个虚拟通道。
type Multiplexer struct {
	conn         net.Conn
	channels     map[uint16]*Channel
	nextChanID   uint16
	lock         sync.RWMutex
	writeLock    sync.Mutex
	closed       bool
	closeChan    chan struct{}
	LocalTarget  string            // 本地目标地址（客户端使用）
	OnNewChannel func(ch *Channel) // 新通道回调（客户端使用）
}

// New 创建多路复用器
//
// 自动启用 TCP keepalive 并启动读取循环。
func New(conn net.Conn) *Multiplexer {
	m := &Multiplexer{
		conn:       conn,
		channels:   make(map[uint16]*Channel),
		nextChanID: 1,
		closeChan:  make(chan struct{}),
	}

	// TCP 优化
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		tcpConn.SetNoDelay(true)
		tcpConn.SetReadBuffer(256 * 1024)
		tcpConn.SetWriteBuffer(256 * 1024)
	}

	go m.readLoop()
	return m
}

func (m *Multiplexer) readLoop() {
	defer func() {
		m.lock.Lock()
		m.closed = true
		for _, ch := range m.channels {
			ch.closeOnce.Do(func() {
				close(ch.CloseChan)
				close(ch.DataChan)
			})
		}
		m.channels = make(map[uint16]*Channel)
		m.lock.Unlock()
		close(m.closeChan)
	}()

	for {
		header := make([]byte, FrameHeaderSize)
		if _, err := io.ReadFull(m.conn, header); err != nil {
			if !m.closed {
				log.Printf("mux read header error: %v", err)
			}
			return
		}

		chanID := binary.BigEndian.Uint16(header[0:2])
		dataLen := binary.BigEndian.Uint16(header[2:4])

		if dataLen > MaxFrameSize {
			log.Printf("frame too large: %d", dataLen)
			return
		}

		data := make([]byte, dataLen)
		if dataLen > 0 {
			if _, err := io.ReadFull(m.conn, data); err != nil {
				log.Printf("mux read data error: %v", err)
				return
			}
		}

		if chanID == ControlChanID {
			m.handleControl(data)
			continue
		}

		m.lock.RLock()
		ch, ok := m.channels[chanID]
		m.lock.RUnlock()

		if ok {
			select {
			case ch.DataChan <- data:
			case <-ch.CloseChan:
			case <-m.closeChan:
				return
			}
		} else {
			log.Printf("received data for unknown channel %d", chanID)
		}
	}
}

func (m *Multiplexer) handleControl(data []byte) {
	if len(data) < 1 {
		return
	}

	cmd := data[0]
	switch cmd {
	case 0x01:
		if len(data) >= 3 {
			chanID := binary.BigEndian.Uint16(data[1:3])
			m.lock.Lock()
			if _, exists := m.channels[chanID]; !exists {
				ch := &Channel{
					ID:        chanID,
					DataChan:  make(chan []byte, 100),
					CloseChan: make(chan struct{}),
					Mux:       m,
				}
				m.channels[chanID] = ch
				if m.OnNewChannel != nil {
					m.lock.Unlock()
					go m.OnNewChannel(ch)
					return
				}
			}
			m.lock.Unlock()
		}
	case 0x02:
		if len(data) >= 3 {
			chanID := binary.BigEndian.Uint16(data[1:3])
			m.closeChannel(chanID)
		}
	}
}

func (m *Multiplexer) sendFrame(chanID uint16, data []byte) error {
	m.writeLock.Lock()
	defer m.writeLock.Unlock()

	if m.closed {
		return fmt.Errorf("multiplexer closed")
	}

	dataLen := len(data)
	if dataLen > MaxFrameSize {
		return fmt.Errorf("data too large: %d", dataLen)
	}

	header := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint16(header[0:2], chanID)
	binary.BigEndian.PutUint16(header[2:4], uint16(dataLen))

	if _, err := m.conn.Write(header); err != nil {
		m.Close()
		return err
	}

	if dataLen > 0 {
		if _, err := m.conn.Write(data); err != nil {
			m.Close()
			return err
		}
	}

	return nil
}

func (m *Multiplexer) Send(chanID uint16, data []byte) error {
	return m.sendFrame(chanID, data)
}

func (m *Multiplexer) CreateChannel() (*Channel, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.closed {
		return nil, fmt.Errorf("multiplexer closed")
	}

	for {
		if m.nextChanID == 0 {
			m.nextChanID = 1
		}
		chanID := m.nextChanID
		m.nextChanID++

		if _, exists := m.channels[chanID]; !exists {
			ch := &Channel{
				ID:        chanID,
				DataChan:  make(chan []byte, 100),
				CloseChan: make(chan struct{}),
				Mux:       m,
			}
			m.channels[chanID] = ch

			ctrlData := make([]byte, 3)
			ctrlData[0] = 0x01
			binary.BigEndian.PutUint16(ctrlData[1:3], chanID)

			if err := m.sendFrame(ControlChanID, ctrlData); err != nil {
				delete(m.channels, chanID)
				return nil, err
			}

			return ch, nil
		}
	}
}

func (m *Multiplexer) CloseChannel(chanID uint16) error {
	m.closeChannel(chanID)

	ctrlData := make([]byte, 3)
	ctrlData[0] = 0x02
	binary.BigEndian.PutUint16(ctrlData[1:3], chanID)

	return m.sendFrame(ControlChanID, ctrlData)
}

func (m *Multiplexer) closeChannel(chanID uint16) {
	m.lock.Lock()
	ch, ok := m.channels[chanID]
	if ok {
		delete(m.channels, chanID)
	}
	m.lock.Unlock()

	if ok {
		ch.closeOnce.Do(func() {
			close(ch.CloseChan)
			close(ch.DataChan)
		})
	}
}

func (m *Multiplexer) Close() {
	m.lock.Lock()
	if m.closed {
		m.lock.Unlock()
		return
	}
	m.closed = true
	m.lock.Unlock()

	m.conn.Close()
}

func (m *Multiplexer) Done() <-chan struct{} {
	return m.closeChan
}

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

func (ch *Channel) ReceiveBlocking() ([]byte, bool) {
	select {
	case data := <-ch.DataChan:
		return data, true
	case <-ch.CloseChan:
		return nil, false
	}
}

func (m *Multiplexer) HandleChannel(ch *Channel) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in HandleChannel channel %d: %v", ch.ID, r)
		}
		m.closeChannel(ch.ID)
	}()

	localConn, err := net.Dial("tcp", m.LocalTarget)
	if err != nil {
		log.Printf("channel %d: failed to connect to local %s: %v", ch.ID, m.LocalTarget, err)
		return
	}
	defer localConn.Close()

	log.Printf("channel %d: connected to local %s", ch.ID, m.LocalTarget)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in HandleChannel localWrite channel %d: %v", ch.ID, r)
			}
			wg.Done()
		}()
		for {
			data, ok := ch.ReceiveBlocking()
			if !ok {
				break
			}
			if _, err := localConn.Write(data); err != nil {
				if err != io.EOF {
					log.Printf("channel %d: local write error: %v", ch.ID, err)
				}
				break
			}
		}
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in HandleChannel localRead channel %d: %v", ch.ID, r)
			}
			wg.Done()
		}()
		buf := make([]byte, MaxFrameSize)
		for {
			n, err := localConn.Read(buf)
			if err != nil {
				if err != io.EOF {
					log.Printf("channel %d: local read error: %v", ch.ID, err)
				}
				break
			}
			if err := m.Send(ch.ID, buf[:n]); err != nil {
				log.Printf("channel %d: send error: %v", ch.ID, err)
				break
			}
		}
	}()

	wg.Wait()
	log.Printf("channel %d closed", ch.ID)
}
