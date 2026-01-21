package rtmp

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"tokuly-live-rtmp-server/pkg/archive"
	"tokuly-live-rtmp-server/pkg/config"
	"tokuly-live-rtmp-server/pkg/inspect"
	"tokuly-live-rtmp-server/pkg/packager"
	"tokuly-live-rtmp-server/pkg/policy"
	"tokuly-live-rtmp-server/pkg/storage"
	"tokuly-live-rtmp-server/pkg/util"
)

type Session struct {
	StreamKey string
	StreamName string
	App       string
	RemoteIP  string
	UserAgent string

	cfg     config.Config
	policy  policy.Policy
	storage *storage.Storage

	archiveManager  *archive.Manager
	archiveRecorder *archive.Recorder

	inspector *inspect.Inspector
	packager  *packager.Packager
	accepted  bool
	closed    bool
	videoInfoSent bool

	buffer         []ingestSample
	bufferStartMS  int64
	maxBufferDurMS int64
}

type ingestSample struct {
	kind   string
	tsMS   int64
	ctsMS  int64
	data   []byte
	isKey  bool
	avcCfg util.AVCConfig
	aacCfg util.AACConfig
}

func NewSession(cfg config.Config, policy policy.Policy, storage *storage.Storage, archiveManager *archive.Manager, streamKey, streamName, app, remoteIP, userAgent string, enableRewind bool) *Session {
	if streamName == "" {
		streamName = streamKey
	}
	sessionStorage := storage
	if storage != nil && storage.EnableRewind != enableRewind {
		copied := *storage
		copied.EnableRewind = enableRewind
		sessionStorage = &copied
	}
	inspector := inspect.New(inspect.Config{
		FirstKeyframeTimeout: cfg.Policy.FirstKeyframeTimeout,
		MaxInspectDuration:   cfg.Policy.MaxInspectDuration,
		AllowNoAudio:         cfg.Policy.AllowNoAudio,
		BitrateWindow:        cfg.Policy.InitialBitrateWindow,
	})
	pkg := packager.New(packager.Config{
		SegmentDuration:      cfg.HLS.SegmentDuration,
		PartDuration:         cfg.HLS.PartDuration,
		PlaylistWindow:       cfg.HLS.PlaylistWindow,
		TargetDuration:       cfg.HLS.TargetDuration,
		HoldBack:             cfg.HLS.HoldBack,
		PartHoldBack:         cfg.HLS.PartHoldBack,
		KeepSegments:         cfg.HLS.KeepSegments,
		RewindPlaylistWindow: cfg.HLS.RewindPlaylistWindow,
		InitFilename:         cfg.HLS.InitFilename,
		SegmentFilenameTmpl:  cfg.HLS.SegmentFilenameTmpl,
		PartFilenameTmpl:     cfg.HLS.PartFilenameTmpl,
		PlaylistName:         cfg.HLS.PlaylistFilename,
		RewindPlaylistName:   cfg.HLS.RewindPlaylistName,
		EnablePartial:        cfg.HLS.EnablePartial,
	}, sessionStorage, streamName)

	return &Session{
		StreamKey:       streamKey,
		StreamName:      streamName,
		App:             app,
		RemoteIP:        remoteIP,
		UserAgent:       userAgent,
		cfg:             cfg,
		policy:          policy,
		storage:         sessionStorage,
		archiveManager:  archiveManager,
		inspector:       inspector,
		packager:        pkg,
		maxBufferDurMS:  int64(cfg.Limits.MaxBufferedSeconds / time.Millisecond),
		bufferStartMS:   0,
		buffer:          nil,
		accepted:        false,
		closed:          false,
	}
}

func (s *Session) HandleVideoConfig(cfg util.AVCConfig) error {
	s.inspector.OnVideoConfig(cfg)
	if s.accepted {
		if s.archiveRecorder != nil {
			if err := s.archiveRecorder.UpdateVideoConfig(cfg); err != nil {
				return err
			}
		}
		return s.packager.UpdateVideoConfig(cfg)
	}
	return s.bufferSample(ingestSample{kind: "video-config", avcCfg: cfg})
}

func (s *Session) HandleAudioConfig(cfg util.AACConfig) error {
	s.inspector.OnAudioConfig(cfg)
	if s.accepted {
		if s.archiveRecorder != nil {
			if err := s.archiveRecorder.UpdateAudioConfig(cfg); err != nil {
				return err
			}
		}
		return s.packager.UpdateAudioConfig(cfg)
	}
	return s.bufferSample(ingestSample{kind: "audio-config", aacCfg: cfg})
}

func (s *Session) HandleVideoSample(tsMS int64, ctsMS int64, data []byte, isKey bool) error {
	s.inspector.OnVideoSample(tsMS, data, isKey)
	s.inspector.FinalizeIfTimeout(tsMS)
	s.tryNotifyVideoInfo()
	if err := s.maybeDecide(tsMS); err != nil {
		return err
	}
	if s.accepted {
		if s.archiveRecorder != nil {
			if err := s.archiveRecorder.AddVideoSample(tsMS, ctsMS, data, isKey); err != nil {
				return err
			}
		}
		return s.packager.AddVideoSample(tsMS, ctsMS, data, isKey)
	}
	return s.bufferSample(ingestSample{kind: "video", tsMS: tsMS, ctsMS: ctsMS, data: data, isKey: isKey})
}

func (s *Session) HandleAudioSample(tsMS int64, data []byte) error {
	s.inspector.OnAudioSample(tsMS, data)
	s.inspector.FinalizeIfTimeout(tsMS)
	s.tryNotifyVideoInfo()
	if err := s.maybeDecide(tsMS); err != nil {
		return err
	}
	if s.accepted {
		if s.archiveRecorder != nil {
			if err := s.archiveRecorder.AddAudioSample(tsMS, data); err != nil {
				return err
			}
		}
		return s.packager.AddAudioSample(tsMS, data)
	}
	return s.bufferSample(ingestSample{kind: "audio", tsMS: tsMS, data: data})
}

func (s *Session) HandleMetadata(meta map[string]interface{}) {
	if meta == nil {
		return
	}
	if fps, ok := readMetadataFloat(meta, "framerate", "videoframerate", "video_fps"); ok && fps > 0 {
		s.inspector.SetVideoFPS(fps)
	}
	s.tryNotifyVideoInfo()
}

func (s *Session) Close(ctx context.Context) {
	if s.closed {
		return
	}
	s.closed = true
	if s.accepted {
		if err := s.packager.Flush(); err != nil {
			log.Printf("packager flush error: %v", err)
		}
	}
	if s.archiveManager != nil {
		s.archiveManager.EndSession(s.StreamName)
	}
	if err := s.policy.NotifyStreamEnd(ctx, s.StreamKey); err != nil {
		log.Printf("stream end notify error: %v", err)
	}
}

func (s *Session) maybeDecide(tsMS int64) error {
	if s.accepted {
		return nil
	}
	res, ok := s.inspector.Result()
	if !ok {
		return nil
	}
	decision := s.policy.Evaluate(context.Background(), res)
	switch decision.Decision {
	case policy.DecisionReject:
		log.Printf("stream rejected: stream_key_hash=%s reason=%s", maskStreamKey(s.StreamKey), decision.Reason)
		return fmt.Errorf("rejected: %s", decision.Reason)
	case policy.DecisionAccept, policy.DecisionDegraded:
		log.Printf("stream accepted: stream_key_hash=%s decision=%d", maskStreamKey(s.StreamKey), decision.Decision)
		s.accepted = true
		if err := s.startArchive(res); err != nil {
			return err
		}
		if err := s.flushBuffer(); err != nil {
			return err
		}
		return nil
	default:
		return nil
	}
}

func (s *Session) tryNotifyVideoInfo() {
	if s.videoInfoSent {
		return
	}
	result, ok := s.inspector.Result()
	if !ok || result.VideoFPS <= 0 {
		return
	}
	s.videoInfoSent = true
	if err := s.policy.NotifyVideoInfo(context.Background(), s.StreamName, result); err != nil {
		log.Printf("video info notify error: %v", err)
	}
}

func (s *Session) startArchive(result inspect.Result) error {
	if s.archiveManager == nil || s.archiveRecorder != nil {
		return nil
	}
	recorder, err := s.archiveManager.Start(s.StreamName, result.InitialBitrate)
	if err != nil {
		return err
	}
	s.archiveRecorder = recorder
	return nil
}

func (s *Session) bufferSample(sample ingestSample) error {
	if sample.kind == "video" || sample.kind == "audio" {
		if s.bufferStartMS == 0 {
			s.bufferStartMS = sample.tsMS
		}
		if s.maxBufferDurMS > 0 && sample.tsMS-s.bufferStartMS > s.maxBufferDurMS {
			return fmt.Errorf("buffer exceeded")
		}
	}
	s.buffer = append(s.buffer, sample)
	return nil
}

func (s *Session) flushBuffer() error {
	for _, sample := range s.buffer {
		switch sample.kind {
		case "video-config":
			if s.archiveRecorder != nil {
				if err := s.archiveRecorder.UpdateVideoConfig(sample.avcCfg); err != nil {
					return err
				}
			}
			if err := s.packager.UpdateVideoConfig(sample.avcCfg); err != nil {
				return err
			}
		case "audio-config":
			if s.archiveRecorder != nil {
				if err := s.archiveRecorder.UpdateAudioConfig(sample.aacCfg); err != nil {
					return err
				}
			}
			if err := s.packager.UpdateAudioConfig(sample.aacCfg); err != nil {
				return err
			}
		case "video":
			if s.archiveRecorder != nil {
				if err := s.archiveRecorder.AddVideoSample(sample.tsMS, sample.ctsMS, sample.data, sample.isKey); err != nil {
					return err
				}
			}
			if err := s.packager.AddVideoSample(sample.tsMS, sample.ctsMS, sample.data, sample.isKey); err != nil {
				return err
			}
		case "audio":
			if s.archiveRecorder != nil {
				if err := s.archiveRecorder.AddAudioSample(sample.tsMS, sample.data); err != nil {
					return err
				}
			}
			if err := s.packager.AddAudioSample(sample.tsMS, sample.data); err != nil {
				return err
			}
		}
	}
	s.buffer = nil
	return nil
}

func readMetadataFloat(meta map[string]interface{}, keys ...string) (float64, bool) {
	for _, key := range keys {
		value, ok := meta[key]
		if !ok {
			continue
		}
		switch v := value.(type) {
		case float64:
			return v, true
		case float32:
			return float64(v), true
		case int:
			return float64(v), true
		case int64:
			return float64(v), true
		case uint64:
			return float64(v), true
		case string:
			parsed, err := strconv.ParseFloat(v, 64)
			if err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}
