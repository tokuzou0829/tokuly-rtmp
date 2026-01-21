package rtmp

import (
	"fmt"
	"log"
	"sync"
	"time"

	"tokuly-live-rtmp-server/pkg/storage"
)

type StreamManager struct {
	mu            sync.Mutex
	sessions      map[string]*Session
	cleanupTimers map[string]*time.Timer
	max           int
	cleanupDelay  time.Duration
	storage       *storage.Storage
}

func NewStreamManager(maxConcurrent int, storage *storage.Storage, cleanupDelay time.Duration) *StreamManager {
	return &StreamManager{
		sessions:      make(map[string]*Session),
		cleanupTimers: make(map[string]*time.Timer),
		max:           maxConcurrent,
		cleanupDelay:  cleanupDelay,
		storage:       storage,
	}
}

func (m *StreamManager) Register(session *Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[session.StreamKey]; ok {
		return fmt.Errorf("stream key already publishing")
	}
	if m.max > 0 && len(m.sessions) >= m.max {
		return fmt.Errorf("max concurrent streams reached")
	}
	m.sessions[session.StreamKey] = session
	if timer, ok := m.cleanupTimers[session.StreamKey]; ok {
		timer.Stop()
		delete(m.cleanupTimers, session.StreamKey)
	}
	return nil
}

func (m *StreamManager) Remove(streamKey, streamID string) {
	if streamKey == "" {
		return
	}
	if streamID == "" {
		streamID = streamKey
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, streamKey)
	if m.cleanupDelay > 0 && m.storage != nil {
		if timer, ok := m.cleanupTimers[streamKey]; ok {
			timer.Stop()
		}
		m.cleanupTimers[streamKey] = time.AfterFunc(m.cleanupDelay, func() {
			m.cleanupIfInactive(streamKey, streamID)
		})
	}
}

func (m *StreamManager) cleanupIfInactive(streamKey, streamID string) {
	if streamKey == "" || streamID == "" {
		return
	}
	m.mu.Lock()
	if _, ok := m.sessions[streamKey]; ok {
		delete(m.cleanupTimers, streamKey)
		m.mu.Unlock()
		return
	}
	delete(m.cleanupTimers, streamKey)
	m.mu.Unlock()

	if m.storage == nil {
		return
	}
	if err := m.storage.RemoveStreamDirs(streamID); err != nil {
		log.Printf("cleanup error: stream_key_hash=%s err=%v", maskStreamKey(streamKey), err)
	}
}
