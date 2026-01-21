package inspect

import (
	"time"

	"github.com/Eyevinn/mp4ff/avc"

	"tokuly-live-rtmp-server/pkg/util"
)

type Result struct {
	VideoCodec       string
	AudioCodec       string
	Width            int
	Height           int
	VideoFPS         float64
	Profile          uint32
	Level            uint32
	SPS              []byte
	PPS              []byte
	ASC              []byte
	SampleRate       int
	Channels         int
	GOPSeconds       float64
	KeyframeReceived bool
	InitialBitrate   int64
}

type Config struct {
	FirstKeyframeTimeout time.Duration
	MaxInspectDuration   time.Duration
	AllowNoAudio         bool
	BitrateWindow        time.Duration
}

type Inspector struct {
	cfg Config

	startTSMS       int64
	started         bool
	videoConfigDone bool
	audioConfigDone bool

	firstKeyframeTS int64
	lastKeyframeTS  int64
	keyframesSeen   int
	videoFrames     int
	videoFirstTS    int64
	videoLastTS     int64

	bitrateStartTS int64
	bitrateBytes   int64

	result Result
	final  bool
}

func New(cfg Config) *Inspector {
	return &Inspector{cfg: cfg}
}

func (i *Inspector) OnVideoConfig(cfg util.AVCConfig) {
	if len(cfg.SPS) == 0 || len(cfg.PPS) == 0 {
		return
	}
	sps := cfg.SPS[0]
	parsed, err := avc.ParseSPSNALUnit(sps, true)
	if err == nil {
		i.result.Width = int(parsed.Width)
		i.result.Height = int(parsed.Height)
		i.result.Profile = parsed.Profile
		i.result.Level = parsed.Level
		if parsed.VUI != nil && parsed.VUI.TimingInfoPresentFlag && parsed.VUI.NumUnitsInTick > 0 {
			fps := float64(parsed.VUI.TimeScale) / (2.0 * float64(parsed.VUI.NumUnitsInTick))
			if fps > 0 && i.result.VideoFPS == 0 {
				i.result.VideoFPS = fps
			}
		}
	}
	i.result.VideoCodec = "H264"
	i.result.SPS = append([]byte(nil), cfg.SPS[0]...)
	i.result.PPS = append([]byte(nil), cfg.PPS[0]...)
	i.videoConfigDone = true
}

func (i *Inspector) OnAudioConfig(cfg util.AACConfig) {
	i.result.AudioCodec = "AAC"
	i.result.ASC = append([]byte(nil), cfg.ASC...)
	i.result.SampleRate = cfg.SampleRate
	i.result.Channels = cfg.Channels
	i.audioConfigDone = true
}

func (i *Inspector) OnVideoSample(tsMS int64, data []byte, isKey bool) {
	i.observeStart(tsMS)
	i.observeBitrate(tsMS, int64(len(data)))
	if i.videoFrames == 0 {
		i.videoFirstTS = tsMS
	}
	i.videoLastTS = tsMS
	i.videoFrames++

	if isKey {
		i.result.KeyframeReceived = true
		if i.keyframesSeen == 0 {
			i.firstKeyframeTS = tsMS
		} else {
			gop := float64(tsMS-i.lastKeyframeTS) / 1000.0
			i.result.GOPSeconds = gop
		}
		i.lastKeyframeTS = tsMS
		i.keyframesSeen++
	}
	i.maybeFinalize(tsMS)
}

func (i *Inspector) OnAudioSample(tsMS int64, data []byte) {
	i.observeStart(tsMS)
	i.observeBitrate(tsMS, int64(len(data)))
	i.maybeFinalize(tsMS)
}

func (i *Inspector) FinalizeIfTimeout(tsMS int64) {
	i.observeStart(tsMS)
	if i.final {
		return
	}
	if i.cfg.FirstKeyframeTimeout > 0 && !i.result.KeyframeReceived {
		if time.Duration(tsMS-i.startTSMS)*time.Millisecond >= i.cfg.FirstKeyframeTimeout {
			i.final = true
			return
		}
	}
	if i.cfg.MaxInspectDuration > 0 {
		if time.Duration(tsMS-i.startTSMS)*time.Millisecond >= i.cfg.MaxInspectDuration {
			i.final = true
			return
		}
	}
}

func (i *Inspector) Result() (Result, bool) {
	if !i.final {
		return Result{}, false
	}
	if i.result.VideoFPS == 0 {
		i.result.VideoFPS = i.estimateFPS()
	}
	return i.result, true
}

func (i *Inspector) SetVideoFPS(value float64) {
	if value <= 0 {
		return
	}
	if i.result.VideoFPS == 0 {
		i.result.VideoFPS = value
	}
}

func (i *Inspector) observeStart(tsMS int64) {
	if i.started {
		return
	}
	i.started = true
	i.startTSMS = tsMS
	i.bitrateStartTS = tsMS
}

func (i *Inspector) observeBitrate(tsMS int64, bytes int64) {
	if !i.started {
		return
	}
	i.bitrateBytes += bytes
	if i.cfg.BitrateWindow <= 0 {
		return
	}
	if time.Duration(tsMS-i.bitrateStartTS)*time.Millisecond >= i.cfg.BitrateWindow {
		seconds := float64(tsMS-i.bitrateStartTS) / 1000.0
		if seconds > 0 {
			i.result.InitialBitrate = int64(float64(i.bitrateBytes*8) / seconds)
		}
		// reset window for the next estimation
		i.bitrateStartTS = tsMS
		i.bitrateBytes = 0
	}
}

func (i *Inspector) maybeFinalize(tsMS int64) {
	if i.final {
		return
	}
	if !i.videoConfigDone {
		return
	}
	if !i.result.KeyframeReceived {
		if i.cfg.FirstKeyframeTimeout > 0 {
			if time.Duration(tsMS-i.startTSMS)*time.Millisecond >= i.cfg.FirstKeyframeTimeout {
				i.final = true
			}
		}
		return
	}
	if !i.audioConfigDone && !i.cfg.AllowNoAudio {
		if i.cfg.MaxInspectDuration > 0 {
			if time.Duration(tsMS-i.startTSMS)*time.Millisecond >= i.cfg.MaxInspectDuration {
				i.final = true
			}
		}
		return
	}
	i.final = true
}

func (i *Inspector) estimateFPS() float64 {
	if i.videoFrames < 2 {
		return 0
	}
	if i.videoLastTS <= i.videoFirstTS {
		return 0
	}
	durationSec := float64(i.videoLastTS-i.videoFirstTS) / 1000.0
	if durationSec <= 0 {
		return 0
	}
	return float64(i.videoFrames-1) / durationSec
}
