package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	RTMP      RTMPConfig
	Policy    PolicyConfig
	HLS       HLSConfig
	Storage   StorageConfig
	Limits    LimitsConfig
	Auth      AuthConfig
	Archive   ArchiveConfig
	DebugRTMP bool
}

type RTMPConfig struct {
	ListenAddr   string
	App          string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type PolicyConfig struct {
	MaxWidth              int
	MaxHeight             int
	FirstKeyframeTimeout  time.Duration
	MaxGOPSeconds         float64
	AllowNoAudio          bool
	OnGOPTooLong          string // "reject" or "degraded"
	RequireAACLC          bool
	RejectIfVideoNotH264  bool
	RejectIfAudioNotAAC   bool
	MaxInspectDuration    time.Duration
	InitialBitrateWindow  time.Duration
	InitialBitrateMinimum int64
}

type HLSConfig struct {
	SegmentDuration      time.Duration
	PartDuration         time.Duration
	PlaylistWindow       time.Duration
	TargetDuration       time.Duration
	HoldBack             time.Duration
	PartHoldBack         time.Duration
	KeepSegments         int
	EnablePartial        bool
	EnableDiscontinuity  bool
	MaxDiscontinuitySeq  int
	PlaylistFilename     string
	SegmentFilenameTmpl  string
	PartFilenameTmpl     string
	InitFilename         string
	RewindPlaylistName   string
	RewindPlaylistWindow time.Duration
}

type StorageConfig struct {
	RootDir      string
	RewindRoot   string
	EnableRewind bool
}

type LimitsConfig struct {
	MaxConcurrentStreams int
	MaxBufferedSeconds   time.Duration
}

type AuthConfig struct {
	AuthURL       string
	StreamEndURL  string
	APIKey        string
	Version       string
	AuthTimeout   time.Duration
	HTTPUserAgent string
}

type ArchiveConfig struct {
	Enable              bool
	RootDir             string
	HLSRootDir          string
	RecordDirTemplate   string
	HLSDirTemplate      string
	RecordFilename      string
	FFmpegPath          string
	ReconnectGrace      time.Duration
	FragmentDuration    time.Duration
	HLSSegmentDuration  time.Duration
	LowBitrateThreshold int64
	MaxDurationLow      time.Duration
	MaxSizeHighBytes    int64
}

func DefaultConfig() Config {
	return Config{
		RTMP: RTMPConfig{
			ListenAddr:   ":1935",
			App:          "live2",
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		Policy: PolicyConfig{
			MaxWidth:              1920,
			MaxHeight:             1920,
			FirstKeyframeTimeout:  2 * time.Second,
			MaxGOPSeconds:         2.0,
			AllowNoAudio:          false,
			OnGOPTooLong:          "degraded",
			RequireAACLC:          true,
			RejectIfVideoNotH264:  true,
			RejectIfAudioNotAAC:   true,
			MaxInspectDuration:    5 * time.Second,
			InitialBitrateWindow:  2 * time.Second,
			InitialBitrateMinimum: 0,
		},
		HLS: HLSConfig{
			SegmentDuration:      2 * time.Second,
			PartDuration:         200 * time.Millisecond,
			PlaylistWindow:       12 * time.Second,
			TargetDuration:       2 * time.Second,
			HoldBack:             6 * time.Second,
			PartHoldBack:         1 * time.Second,
			KeepSegments:         6,
			EnablePartial:        true,
			EnableDiscontinuity:  true,
			MaxDiscontinuitySeq:  1000,
			PlaylistFilename:     "index.m3u8",
			SegmentFilenameTmpl:  "seg_%06d.m4s",
			PartFilenameTmpl:     "part_%06d_%02d.m4s",
			InitFilename:         "init.mp4",
			RewindPlaylistName:   "index.m3u8",
			RewindPlaylistWindow: 3600 * time.Second,
		},
		Storage: StorageConfig{
			RootDir:      "./live-hls",
			RewindRoot:   "./hls_rewind",
			EnableRewind: true,
		},
		Limits: LimitsConfig{
			MaxConcurrentStreams: 10,
			MaxBufferedSeconds:   10 * time.Second,
		},
		Auth: AuthConfig{
			AuthURL:       "https://api.tokuly.com/live/checkstream",
			StreamEndURL:  "https://api.tokuly.com/live/endstream",
			APIKey:        "",
			Version:       "tokuly-rtmp-server",
			AuthTimeout:   3 * time.Second,
			HTTPUserAgent: "go-rtmp-server/0.1",
		},
		Archive: ArchiveConfig{
			Enable:              true,
			RootDir:             "./archive",
			HLSRootDir:          "./archive-hls",
			RecordDirTemplate:   "{streamName}/{startUTC}",
			HLSDirTemplate:      "{streamName}/{startUTC}",
			RecordFilename:      "archive.mp4",
			FFmpegPath:          "/opt/homebrew/bin/ffmpeg",
			ReconnectGrace:      30 * time.Second,
			FragmentDuration:    2 * time.Second,
			HLSSegmentDuration:  10 * time.Second,
			LowBitrateThreshold: 12000000,
			MaxDurationLow:      90 * time.Minute,
			MaxSizeHighBytes:    int64(5) * 1024 * 1024 * 1024,
		},
		DebugRTMP: false,
	}
}

func Load() Config {
	cfg := DefaultConfig()

	if v := os.Getenv("RTMP_ADDR"); v != "" {
		cfg.RTMP.ListenAddr = v
	}
	if v := os.Getenv("RTMP_APP"); v != "" {
		cfg.RTMP.App = v
	}
	if v := os.Getenv("ROOT_DIR"); v != "" {
		cfg.Storage.RootDir = v
	}
	if v := os.Getenv("REWIND_ROOT_DIR"); v != "" {
		cfg.Storage.RewindRoot = v
	}
	if v := os.Getenv("ENABLE_REWIND"); v != "" {
		cfg.Storage.EnableRewind = parseBool(v, cfg.Storage.EnableRewind)
	}
	if v := os.Getenv("DEBUG_RTMP"); v != "" {
		cfg.DebugRTMP = parseBool(v, cfg.DebugRTMP)
	} else if v := os.Getenv("RTMP_DEBUG"); v != "" {
		cfg.DebugRTMP = parseBool(v, cfg.DebugRTMP)
	}
	if v := os.Getenv("AUTH_URL"); v != "" {
		cfg.Auth.AuthURL = v
	}
	if v := os.Getenv("STREAM_END_URL"); v != "" {
		cfg.Auth.StreamEndURL = v
	}
	if v := os.Getenv("AUTH_API_KEY"); v != "" {
		cfg.Auth.APIKey = v
	}
	if v := os.Getenv("AUTH_VERSION"); v != "" {
		cfg.Auth.Version = v
	}
	if v := os.Getenv("AUTH_TIMEOUT"); v != "" {
		cfg.Auth.AuthTimeout = parseDuration(v, cfg.Auth.AuthTimeout)
	}
	if v := os.Getenv("AUTH_USER_AGENT"); v != "" {
		cfg.Auth.HTTPUserAgent = v
	}

	if v := os.Getenv("MAX_WIDTH"); v != "" {
		cfg.Policy.MaxWidth = parseInt(v, cfg.Policy.MaxWidth)
	}
	if v := os.Getenv("MAX_HEIGHT"); v != "" {
		cfg.Policy.MaxHeight = parseInt(v, cfg.Policy.MaxHeight)
	}
	if v := os.Getenv("FIRST_KEYFRAME_TIMEOUT"); v != "" {
		cfg.Policy.FirstKeyframeTimeout = parseDuration(v, cfg.Policy.FirstKeyframeTimeout)
	}
	if v := os.Getenv("MAX_GOP_SECONDS"); v != "" {
		cfg.Policy.MaxGOPSeconds = parseFloat(v, cfg.Policy.MaxGOPSeconds)
	}
	if v := os.Getenv("ALLOW_NO_AUDIO"); v != "" {
		cfg.Policy.AllowNoAudio = parseBool(v, cfg.Policy.AllowNoAudio)
	}
	if v := os.Getenv("ON_GOP_TOO_LONG"); v != "" {
		cfg.Policy.OnGOPTooLong = v
	}
	if v := os.Getenv("REQUIRE_AAC_LC"); v != "" {
		cfg.Policy.RequireAACLC = parseBool(v, cfg.Policy.RequireAACLC)
	}
	if v := os.Getenv("REJECT_IF_VIDEO_NOT_H264"); v != "" {
		cfg.Policy.RejectIfVideoNotH264 = parseBool(v, cfg.Policy.RejectIfVideoNotH264)
	}
	if v := os.Getenv("REJECT_IF_AUDIO_NOT_AAC"); v != "" {
		cfg.Policy.RejectIfAudioNotAAC = parseBool(v, cfg.Policy.RejectIfAudioNotAAC)
	}
	if v := os.Getenv("MAX_INSPECT_DURATION"); v != "" {
		cfg.Policy.MaxInspectDuration = parseDuration(v, cfg.Policy.MaxInspectDuration)
	}
	if v := os.Getenv("INITIAL_BITRATE_WINDOW"); v != "" {
		cfg.Policy.InitialBitrateWindow = parseDuration(v, cfg.Policy.InitialBitrateWindow)
	}
	if v := os.Getenv("INITIAL_BITRATE_MINIMUM"); v != "" {
		cfg.Policy.InitialBitrateMinimum = parseInt64(v, cfg.Policy.InitialBitrateMinimum)
	}

	if v := os.Getenv("SEGMENT_DURATION"); v != "" {
		cfg.HLS.SegmentDuration = parseDuration(v, cfg.HLS.SegmentDuration)
	}
	if v := os.Getenv("PART_DURATION"); v != "" {
		cfg.HLS.PartDuration = parseDuration(v, cfg.HLS.PartDuration)
	}
	if v := os.Getenv("PLAYLIST_WINDOW"); v != "" {
		cfg.HLS.PlaylistWindow = parseDuration(v, cfg.HLS.PlaylistWindow)
	}
	if v := os.Getenv("TARGET_DURATION"); v != "" {
		cfg.HLS.TargetDuration = parseDuration(v, cfg.HLS.TargetDuration)
	}
	if v := os.Getenv("HOLD_BACK"); v != "" {
		cfg.HLS.HoldBack = parseDuration(v, cfg.HLS.HoldBack)
	}
	if v := os.Getenv("PART_HOLD_BACK"); v != "" {
		cfg.HLS.PartHoldBack = parseDuration(v, cfg.HLS.PartHoldBack)
	}
	if v := os.Getenv("KEEP_SEGMENTS"); v != "" {
		cfg.HLS.KeepSegments = parseInt(v, cfg.HLS.KeepSegments)
	}
	if v := os.Getenv("ENABLE_PARTIAL"); v != "" {
		cfg.HLS.EnablePartial = parseBool(v, cfg.HLS.EnablePartial)
	}

	if v := os.Getenv("MAX_CONCURRENT_STREAMS"); v != "" {
		cfg.Limits.MaxConcurrentStreams = parseInt(v, cfg.Limits.MaxConcurrentStreams)
	}
	if v := os.Getenv("MAX_BUFFERED_SECONDS"); v != "" {
		cfg.Limits.MaxBufferedSeconds = parseDuration(v, cfg.Limits.MaxBufferedSeconds)
	}

	if v := os.Getenv("ARCHIVE_ENABLE"); v != "" {
		cfg.Archive.Enable = parseBool(v, cfg.Archive.Enable)
	}
	if v := os.Getenv("ARCHIVE_ROOT_DIR"); v != "" {
		cfg.Archive.RootDir = v
	}
	if v := os.Getenv("ARCHIVE_HLS_ROOT_DIR"); v != "" {
		cfg.Archive.HLSRootDir = v
	}
	if v := os.Getenv("ARCHIVE_RECORD_DIR_TEMPLATE"); v != "" {
		cfg.Archive.RecordDirTemplate = v
	}
	if v := os.Getenv("ARCHIVE_HLS_DIR_TEMPLATE"); v != "" {
		cfg.Archive.HLSDirTemplate = v
	}
	if v := os.Getenv("ARCHIVE_RECORD_FILENAME"); v != "" {
		cfg.Archive.RecordFilename = v
	}
	if v := os.Getenv("ARCHIVE_FFMPEG_PATH"); v != "" {
		cfg.Archive.FFmpegPath = v
	}
	if v := os.Getenv("ARCHIVE_RECONNECT_GRACE"); v != "" {
		cfg.Archive.ReconnectGrace = parseDuration(v, cfg.Archive.ReconnectGrace)
	}
	if v := os.Getenv("ARCHIVE_FRAGMENT_DURATION"); v != "" {
		cfg.Archive.FragmentDuration = parseDuration(v, cfg.Archive.FragmentDuration)
	}
	if v := os.Getenv("ARCHIVE_HLS_SEGMENT_DURATION"); v != "" {
		cfg.Archive.HLSSegmentDuration = parseDuration(v, cfg.Archive.HLSSegmentDuration)
	}
	if v := os.Getenv("ARCHIVE_LOW_BITRATE_BPS"); v != "" {
		cfg.Archive.LowBitrateThreshold = parseInt64(v, cfg.Archive.LowBitrateThreshold)
	}
	if v := os.Getenv("ARCHIVE_MAX_DURATION_LOW"); v != "" {
		cfg.Archive.MaxDurationLow = parseDuration(v, cfg.Archive.MaxDurationLow)
	}
	if v := os.Getenv("ARCHIVE_MAX_SIZE_HIGH_BYTES"); v != "" {
		cfg.Archive.MaxSizeHighBytes = parseInt64(v, cfg.Archive.MaxSizeHighBytes)
	}

	return cfg
}

func parseBool(value string, fallback bool) bool {
	v, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return v
}

func parseInt(value string, fallback int) int {
	v, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return v
}

func parseInt64(value string, fallback int64) int64 {
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}

func parseFloat(value string, fallback float64) float64 {
	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return v
}

func parseDuration(value string, fallback time.Duration) time.Duration {
	v, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return v
}
