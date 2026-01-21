package archive

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"tokuly-live-rtmp-server/pkg/config"
	"tokuly-live-rtmp-server/pkg/policy"
)

var ErrArchiveBusy = errors.New("archive busy")
var ErrArchiveActive = errors.New("archive already active")

type Manager struct {
	mu           sync.Mutex
	cfg          config.ArchiveConfig
	policy       policy.Policy
	allowNoAudio bool
	states       map[string]*ArchiveState
}

type ArchiveState struct {
	streamName string
	recordDir  string
	recordPath string
	hlsDir     string
	startTime  time.Time
	recorder   *Recorder
	active     bool
	closing    bool
	finalizing bool
	converting bool
	timer      *time.Timer
}

func NewManager(cfg config.ArchiveConfig, pol policy.Policy, allowNoAudio bool) *Manager {
	return &Manager{
		cfg:          cfg,
		policy:       pol,
		allowNoAudio: allowNoAudio,
		states:       make(map[string]*ArchiveState),
	}
}

func (m *Manager) Enabled() bool {
	return m != nil && m.cfg.Enable
}

func (m *Manager) CanPublish(streamName string) error {
	if !m.Enabled() || streamName == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[streamName]
	if state == nil {
		return nil
	}
	if state.finalizing || state.converting {
		return ErrArchiveBusy
	}
	if state.active {
		return ErrArchiveActive
	}
	return nil
}

func (m *Manager) Start(streamName string, bitrate int64) (*Recorder, error) {
	if !m.Enabled() || streamName == "" {
		return nil, nil
	}
	m.mu.Lock()
	state := m.states[streamName]
	if state != nil {
		if state.finalizing || state.converting {
			m.mu.Unlock()
			return nil, ErrArchiveBusy
		}
		if state.active {
			m.mu.Unlock()
			return nil, ErrArchiveActive
		}
		if state.closing {
			if state.timer != nil {
				state.timer.Stop()
				state.timer = nil
			}
			state.closing = false
			state.active = true
			rec := state.recorder
			m.mu.Unlock()
			if rec != nil {
				rec.StartSession()
				rec.SetBitrate(bitrate)
			}
			return rec, nil
		}
		m.removeDirsLocked(state)
	} else {
		if strings.Contains(m.cfg.RecordDirTemplate, "{streamName}") {
			_ = os.RemoveAll(filepath.Join(m.cfg.RootDir, streamName))
		}
		if strings.Contains(m.cfg.HLSDirTemplate, "{streamName}") {
			_ = os.RemoveAll(filepath.Join(m.cfg.HLSRootDir, streamName))
		}
	}
	rootDir := m.cfg.RootDir
	hlsRoot := m.cfg.HLSRootDir
	if rootDir == "" || hlsRoot == "" {
		m.mu.Unlock()
		return nil, fmt.Errorf("archive root dir empty")
	}
	if m.cfg.RecordFilename == "" {
		m.mu.Unlock()
		return nil, fmt.Errorf("archive record filename empty")
	}
	start := time.Now().UTC()
	recordRel := renderTemplate(m.cfg.RecordDirTemplate, streamName, start)
	hlsRel := renderTemplate(m.cfg.HLSDirTemplate, streamName, start)
	recordDir := filepath.Join(rootDir, recordRel)
	hlsDir := filepath.Join(hlsRoot, hlsRel)
	if err := os.MkdirAll(recordDir, 0755); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	recordPath := filepath.Join(recordDir, m.cfg.RecordFilename)
	recorder, err := NewRecorder(RecorderConfig{
		FragmentDuration:    m.cfg.FragmentDuration,
		LowBitrateThreshold: m.cfg.LowBitrateThreshold,
		MaxDurationLow:      m.cfg.MaxDurationLow,
		MaxSizeHighBytes:    m.cfg.MaxSizeHighBytes,
		AllowNoAudio:        m.allowNoAudio,
	}, recordPath)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	recorder.StartSession()
	recorder.SetBitrate(bitrate)
	state = &ArchiveState{
		streamName: streamName,
		recordDir:  recordDir,
		recordPath: recordPath,
		hlsDir:     hlsDir,
		startTime:  start,
		recorder:   recorder,
		active:     true,
	}
	m.states[streamName] = state
	m.mu.Unlock()
	return recorder, nil
}

func (m *Manager) EndSession(streamName string) {
	if !m.Enabled() || streamName == "" {
		return
	}
	var recorder *Recorder
	var grace time.Duration
	m.mu.Lock()
	state := m.states[streamName]
	if state == nil || state.closing || state.finalizing || state.converting {
		m.mu.Unlock()
		return
	}
	state.active = false
	state.closing = true
	if state.timer != nil {
		state.timer.Stop()
		state.timer = nil
	}
	recorder = state.recorder
	grace = m.cfg.ReconnectGrace
	if grace > 0 {
		state.timer = time.AfterFunc(grace, func() {
			m.finalize(streamName)
		})
	}
	m.mu.Unlock()

	if recorder != nil {
		recorder.Flush()
	}
	if grace <= 0 {
		m.finalize(streamName)
	}
}

func (m *Manager) finalize(streamName string) {
	if !m.Enabled() || streamName == "" {
		return
	}
	var (
		recorder   *Recorder
		recordPath string
		hlsDir     string
	)
	m.mu.Lock()
	state := m.states[streamName]
	if state == nil || state.finalizing || state.converting {
		m.mu.Unlock()
		return
	}
	state.closing = false
	state.finalizing = true
	if state.timer != nil {
		state.timer.Stop()
		state.timer = nil
	}
	recorder = state.recorder
	recordPath = state.recordPath
	hlsDir = state.hlsDir
	m.mu.Unlock()

	if recorder != nil {
		recorder.Close()
	}

	m.mu.Lock()
	state = m.states[streamName]
	if state == nil {
		m.mu.Unlock()
		return
	}
	state.converting = true
	m.mu.Unlock()

	err := m.convertToHLS(recordPath, hlsDir)
	if err != nil {
		log.Printf("archive convert error: stream=%s err=%v", streamName, err)
	}
	if notifyErr := m.notifyArchiveStatus(streamName, err == nil); notifyErr != nil {
		log.Printf("archive status notify error: stream=%s err=%v", streamName, notifyErr)
	}

	m.mu.Lock()
	state = m.states[streamName]
	if state != nil {
		state.finalizing = false
		state.converting = false
		state.recorder = nil
	}
	m.mu.Unlock()
}

func (m *Manager) removeDirsLocked(state *ArchiveState) {
	if state == nil {
		return
	}
	if state.recordDir != "" {
		if err := os.RemoveAll(state.recordDir); err != nil {
			log.Printf("archive cleanup error: dir=%s err=%v", state.recordDir, err)
		}
	}
	if state.hlsDir != "" {
		if err := os.RemoveAll(state.hlsDir); err != nil {
			log.Printf("archive cleanup error: dir=%s err=%v", state.hlsDir, err)
		}
	}
}

func (m *Manager) convertToHLS(recordPath, hlsDir string) error {
	if recordPath == "" {
		return fmt.Errorf("archive path empty")
	}
	info, err := os.Stat(recordPath)
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return fmt.Errorf("archive empty")
	}
	if hlsDir == "" {
		return fmt.Errorf("archive hls dir empty")
	}
	if err := os.RemoveAll(hlsDir); err != nil {
		return err
	}
	if err := os.MkdirAll(hlsDir, 0755); err != nil {
		return err
	}
	segmentDuration := formatSeconds(m.cfg.HLSSegmentDuration)
	segmentPattern := filepath.Join(hlsDir, "segment_%06d.m4s")
	outPlaylist := filepath.Join(hlsDir, "index.m3u8")
	args := []string{
		"-hide_banner",
		"-y",
		"-i", recordPath,
		"-map", "0:v:0",
		"-map", "0:a:0?",
		"-c:v", "copy",
		"-c:a", "copy",
		"-f", "hls",
		"-hls_time", segmentDuration,
		"-hls_list_size", "0",
		"-hls_flags", "program_date_time+independent_segments+round_durations",
		"-hls_segment_type", "fmp4",
		"-hls_segment_filename", segmentPattern,
		outPlaylist,
	}
	cmd := exec.Command(m.cfg.FFmpegPath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg failed: %w output=%s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (m *Manager) notifyArchiveStatus(streamName string, ok bool) error {
	if m.policy == nil {
		return nil
	}
	ctx := context.Background()
	return m.policy.NotifyArchiveStatus(ctx, streamName, ok)
}

func renderTemplate(tmpl, streamName string, start time.Time) string {
	if tmpl == "" {
		tmpl = "{streamName}/{startUTC}"
	}
	startUTC := start.UTC().Format("20060102T150405Z")
	out := strings.ReplaceAll(tmpl, "{streamName}", streamName)
	out = strings.ReplaceAll(out, "{startUTC}", startUTC)
	return out
}

func formatSeconds(d time.Duration) string {
	if d <= 0 {
		return "10"
	}
	seconds := d.Seconds()
	if seconds == float64(int64(seconds)) {
		return strconv.FormatInt(int64(seconds), 10)
	}
	return strconv.FormatFloat(seconds, 'f', -1, 64)
}
