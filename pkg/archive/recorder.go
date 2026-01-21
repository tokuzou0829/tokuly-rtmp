package archive

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Eyevinn/mp4ff/mp4"

	"tokuly-live-rtmp-server/pkg/util"
)

var ErrAppendNotPossible = errors.New("archive append not possible")

type RecorderConfig struct {
	FragmentDuration    time.Duration
	LowBitrateThreshold int64
	MaxDurationLow      time.Duration
	MaxSizeHighBytes    int64
	AllowNoAudio        bool
}

type Recorder struct {
	mu   sync.Mutex
	cfg  RecorderConfig
	path string
	file *os.File

	bytesWritten int64

	limitMode     string
	maxDurationMS int64
	maxSizeBytes  int64

	sessions        int
	sessionStarted  bool
	sessionOffsetMS int64

	started   bool
	startTSMS int64
	lastTSMS  int64

	fragmentSeq        uint32
	fragmentDurationMS int64
	currentFragment    *fragmentBuilder

	videoTS uint32
	audioTS uint32
	videoID uint32
	audioID uint32

	avcConfig util.AVCConfig
	aacConfig util.AACConfig

	initWritten bool
	ignoreAudio bool

	videoState trackState
	audioState trackState

	stopped bool
	failed  bool
}

type fragmentBuilder struct {
	idx     int
	startMS int64
	endMS   int64
	samples []trackSample
}

type pendingSample struct {
	dtsMS int64
	ctsMS int64
	data  []byte
	isKey bool
}

type trackState struct {
	timescale     uint32
	pending       *pendingSample
	lastDurMS     int64
	defaultDurMS  int64
	lastDTSMS     int64
	hasStarted    bool
	trackID       uint32
	sampleIsVideo bool
}

type trackSample struct {
	trackID uint32
	sample  mp4.FullSample
}

func NewRecorder(cfg RecorderConfig, path string) (*Recorder, error) {
	if path == "" {
		return nil, fmt.Errorf("archive path empty")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	rec := &Recorder{
		cfg:                cfg,
		path:               path,
		file:               f,
		fragmentDurationMS: int64(cfg.FragmentDuration / time.Millisecond),
		videoTS:            90000,
	}
	rec.videoState.sampleIsVideo = true
	return rec, nil
}

func (r *Recorder) StartSession() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions++
	r.sessionStarted = false
	r.sessionOffsetMS = 0
}

func (r *Recorder) SetBitrate(bitrate int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.limitMode != "" {
		return
	}
	if bitrate <= r.cfg.LowBitrateThreshold {
		r.limitMode = "duration"
		r.maxDurationMS = int64(r.cfg.MaxDurationLow / time.Millisecond)
		return
	}
	r.limitMode = "size"
	r.maxSizeBytes = r.cfg.MaxSizeHighBytes
}

func (r *Recorder) UpdateVideoConfig(cfg util.AVCConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.initWritten && !util.EqualAVCConfig(r.avcConfig, cfg) {
		if r.sessions > 1 {
			return ErrAppendNotPossible
		}
		r.markFailed("video config changed")
		return nil
	}
	r.avcConfig = cfg
	r.videoState.sampleIsVideo = true
	return r.maybeWriteInit()
}

func (r *Recorder) UpdateAudioConfig(cfg util.AACConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.initWritten {
		if r.audioID == 0 {
			if r.sessions > 1 {
				return ErrAppendNotPossible
			}
			r.ignoreAudio = true
			return nil
		}
		if !equalAACConfig(r.aacConfig, cfg) {
			if r.sessions > 1 {
				return ErrAppendNotPossible
			}
			r.markFailed("audio config changed")
			return nil
		}
	}
	r.aacConfig = cfg
	if cfg.SampleRate <= 0 {
		r.audioTS = 0
		r.audioState.defaultDurMS = 20
		return r.maybeWriteInit()
	}
	r.audioTS = uint32(cfg.SampleRate)
	r.audioState.defaultDurMS = int64((1024.0 * 1000.0) / float64(cfg.SampleRate))
	if r.audioState.defaultDurMS == 0 {
		r.audioState.defaultDurMS = 20
	}
	return r.maybeWriteInit()
}

func (r *Recorder) AddVideoSample(tsMS int64, ctsMS int64, data []byte, isKey bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addSample(true, pendingSample{dtsMS: tsMS, ctsMS: ctsMS, data: data, isKey: isKey})
}

func (r *Recorder) AddAudioSample(tsMS int64, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addSample(false, pendingSample{dtsMS: tsMS, ctsMS: 0, data: data, isKey: false})
}

func (r *Recorder) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.flushLocked()
}

func (r *Recorder) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.flushLocked()
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
	}
}

func (r *Recorder) flushLocked() {
	if err := r.flushTrack(&r.videoState); err != nil {
		r.markFailed(err.Error())
	}
	if err := r.flushTrack(&r.audioState); err != nil {
		r.markFailed(err.Error())
	}
	if err := r.finalizeFragment(); err != nil {
		r.markFailed(err.Error())
	}
}

func (r *Recorder) addSample(isVideo bool, sample pendingSample) error {
	if r.stopped || r.failed {
		return nil
	}
	if !r.initWritten {
		return nil
	}
	if !isVideo && (r.ignoreAudio || r.audioID == 0) {
		return nil
	}
	adjustedTS := r.adjustTS(sample.dtsMS)
	if r.limitMode == "duration" && r.maxDurationMS > 0 && r.started {
		if adjustedTS-r.startTSMS >= r.maxDurationMS {
			r.stopped = true
			return nil
		}
	}
	sample.dtsMS = adjustedTS
	r.ensureStart(sample.dtsMS)

	statePtr := &r.audioState
	if isVideo {
		statePtr = &r.videoState
	}

	full, err := r.ingestSample(statePtr, sample)
	if err != nil {
		r.markFailed(err.Error())
		return nil
	}
	if full == nil {
		return nil
	}
	if err := r.appendToFragment(*full); err != nil {
		r.markFailed(err.Error())
	}
	return nil
}

func (r *Recorder) ingestSample(state *trackState, sample pendingSample) (*trackSample, error) {
	if state.timescale == 0 {
		if state.sampleIsVideo {
			state.timescale = r.videoTS
			state.trackID = r.videoID
			if state.defaultDurMS == 0 {
				state.defaultDurMS = 33
			}
		} else {
			state.timescale = r.audioTS
			state.trackID = r.audioID
		}
	}
	if state.hasStarted && state.pending != nil {
		if sample.dtsMS <= state.pending.dtsMS {
			sample.dtsMS = state.pending.dtsMS + maxInt64(state.lastDurMS, 1)
		}
	}
	if state.pending == nil {
		state.pending = &sample
		state.hasStarted = true
		state.lastDTSMS = sample.dtsMS
		return nil, nil
	}
	durMS := sample.dtsMS - state.pending.dtsMS
	if durMS <= 0 {
		durMS = state.lastDurMS
	}
	if durMS <= 0 {
		durMS = state.defaultDurMS
	}
	if durMS <= 0 {
		durMS = 1
	}
	full := buildFullSample(state, *state.pending, durMS)
	state.lastDurMS = durMS
	state.lastDTSMS = sample.dtsMS
	state.pending = &sample
	return &trackSample{trackID: state.trackID, sample: full}, nil
}

func (r *Recorder) flushTrack(state *trackState) error {
	if state.pending == nil || !r.initWritten {
		return nil
	}
	durMS := state.lastDurMS
	if durMS <= 0 {
		durMS = state.defaultDurMS
	}
	if durMS <= 0 {
		durMS = 1
	}
	full := buildFullSample(state, *state.pending, durMS)
	state.pending = nil
	return r.appendToFragment(trackSample{trackID: state.trackID, sample: full})
}

func (r *Recorder) appendToFragment(ts trackSample) error {
	fragIdx, fragStartMS := r.computeFragmentIndex(ts.sample.DecodeTime, ts.trackID == r.videoID)
	fragEndMS := fragStartMS + r.fragmentDurationMS
	if r.fragmentDurationMS <= 0 {
		fragEndMS = fragStartMS + 2000
	}
	if r.currentFragment == nil || fragIdx != r.currentFragment.idx {
		if err := r.finalizeFragment(); err != nil {
			return err
		}
		r.currentFragment = &fragmentBuilder{
			idx:     fragIdx,
			startMS: fragStartMS,
			endMS:   fragEndMS,
		}
	}
	r.currentFragment.samples = append(r.currentFragment.samples, ts)
	return nil
}

func (r *Recorder) computeFragmentIndex(decodeTime uint64, isVideo bool) (int, int64) {
	var tsMS int64
	if isVideo {
		tsMS = timescaleToMS(decodeTime, r.videoTS)
	} else {
		tsMS = timescaleToMS(decodeTime, r.audioTS)
	}
	rel := tsMS - r.startTSMS
	if rel < 0 {
		rel = 0
	}
	if r.fragmentDurationMS <= 0 {
		return int(rel / 2000), r.startTSMS + int64(rel/2000)*2000
	}
	idx := int(rel / r.fragmentDurationMS)
	start := r.startTSMS + int64(idx)*r.fragmentDurationMS
	return idx, start
}

func (r *Recorder) finalizeFragment() error {
	if r.currentFragment == nil || len(r.currentFragment.samples) == 0 {
		r.currentFragment = nil
		return nil
	}
	frag, err := r.createFragment(r.fragmentSeq+1, r.currentFragment.samples)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := frag.Encode(&buf); err != nil {
		return err
	}
	if err := r.writeBytes(buf.Bytes()); err != nil {
		return err
	}
	r.fragmentSeq++
	r.currentFragment = nil
	return nil
}

func (r *Recorder) createFragment(seqNumber uint32, samples []trackSample) (*mp4.Fragment, error) {
	var trackIDs []uint32
	if r.videoID != 0 {
		trackIDs = append(trackIDs, r.videoID)
	}
	if r.audioID != 0 {
		trackIDs = append(trackIDs, r.audioID)
	}
	frag, err := mp4.CreateMultiTrackFragment(seqNumber, trackIDs)
	if err != nil {
		return nil, err
	}
	for _, s := range samples {
		if err := frag.AddFullSampleToTrack(s.sample, s.trackID); err != nil {
			return nil, err
		}
	}
	return frag, nil
}

func (r *Recorder) maybeWriteInit() error {
	if r.initWritten {
		return nil
	}
	if len(r.avcConfig.SPS) == 0 || len(r.avcConfig.PPS) == 0 {
		return nil
	}
	if !r.cfg.AllowNoAudio && r.aacConfig.SampleRate == 0 {
		return nil
	}
	init := mp4.CreateEmptyInit()
	videoTrak := addEmptyTrack(init, r.videoTS, "video", "und")
	if err := videoTrak.SetAVCDescriptor("avc1", r.avcConfig.SPS, r.avcConfig.PPS, true); err != nil {
		return err
	}
	r.videoID = videoTrak.Tkhd.TrackID
	r.videoState.trackID = r.videoID
	r.videoState.timescale = r.videoTS
	r.videoState.defaultDurMS = 33
	if r.aacConfig.SampleRate > 0 {
		r.audioTS = uint32(r.aacConfig.SampleRate)
		audioTrak := addEmptyTrack(init, r.audioTS, "audio", "und")
		esds := mp4.CreateEsdsBox(r.aacConfig.ASC)
		channels := r.aacConfig.Channels
		if channels == 0 {
			channels = 2
		}
		mp4a := mp4.CreateAudioSampleEntryBox("mp4a", uint16(channels), 16, uint16(r.aacConfig.SampleRate), esds)
		audioTrak.Mdia.Minf.Stbl.Stsd.AddChild(mp4a)
		r.audioID = audioTrak.Tkhd.TrackID
		r.audioState.trackID = r.audioID
		r.audioState.timescale = r.audioTS
	} else {
		r.ignoreAudio = true
	}
	var buf bytes.Buffer
	if err := init.Encode(&buf); err != nil {
		return err
	}
	if err := r.writeBytes(buf.Bytes()); err != nil {
		return err
	}
	r.initWritten = true
	return nil
}

func (r *Recorder) adjustTS(tsMS int64) int64 {
	if !r.sessionStarted {
		r.sessionStarted = true
		if r.lastTSMS > 0 && tsMS < r.lastTSMS+1 {
			r.sessionOffsetMS = (r.lastTSMS + 1) - tsMS
		} else {
			r.sessionOffsetMS = 0
		}
	}
	adj := tsMS + r.sessionOffsetMS
	if adj > r.lastTSMS {
		r.lastTSMS = adj
	}
	return adj
}

func (r *Recorder) ensureStart(tsMS int64) {
	if r.started {
		return
	}
	r.started = true
	r.startTSMS = tsMS
}

func (r *Recorder) writeBytes(data []byte) error {
	if r.file == nil {
		return fmt.Errorf("archive file closed")
	}
	if len(data) == 0 {
		return nil
	}
	n, err := r.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("archive short write")
	}
	r.bytesWritten += int64(n)
	if r.limitMode == "size" && r.maxSizeBytes > 0 && r.bytesWritten >= r.maxSizeBytes {
		r.stopped = true
	}
	return nil
}

func (r *Recorder) markFailed(reason string) {
	if r.failed {
		return
	}
	r.failed = true
	r.stopped = true
	log.Printf("archive recorder stopped: %s", reason)
}

func addEmptyTrack(initSeg *mp4.InitSegment, timeScale uint32, mediaType, language string) *mp4.TrakBox {
	moov := initSeg.Moov
	trackID := uint32(len(moov.Traks) + 1)
	moov.Mvhd.NextTrackID = trackID + 1
	newTrak := mp4.CreateEmptyTrak(trackID, timeScale, mediaType, language)
	moov.AddChild(newTrak)
	moov.Mvex.AddChild(mp4.CreateTrex(trackID))
	return newTrak
}

func buildFullSample(state *trackState, sample pendingSample, durMS int64) mp4.FullSample {
	durTS := msToTimescale(durMS, state.timescale)
	decodeTime := msToTimescale64(sample.dtsMS, state.timescale)
	ctsOffset := msToTimescaleSigned(sample.ctsMS, state.timescale)
	flags := uint32(0)
	if state.sampleIsVideo {
		if sample.isKey {
			flags = mp4.SyncSampleFlags
		} else {
			flags = mp4.NonSyncSampleFlags
		}
	}
	s := mp4.NewSample(flags, durTS, uint32(len(sample.data)), ctsOffset)
	return mp4.FullSample{
		Sample:     s,
		DecodeTime: decodeTime,
		Data:       append([]byte(nil), sample.data...),
	}
}

func msToTimescale(ms int64, timescale uint32) uint32 {
	if ms <= 0 {
		return 0
	}
	return uint32((ms * int64(timescale)) / 1000)
}

func msToTimescale64(ms int64, timescale uint32) uint64 {
	if ms <= 0 {
		return 0
	}
	return uint64((ms * int64(timescale)) / 1000)
}

func msToTimescaleSigned(ms int64, timescale uint32) int32 {
	if ms == 0 {
		return 0
	}
	return int32((ms * int64(timescale)) / 1000)
}

func timescaleToMS(value uint64, timescale uint32) int64 {
	if timescale == 0 {
		return 0
	}
	return int64(value*1000) / int64(timescale)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func equalAACConfig(a, b util.AACConfig) bool {
	if a.SampleRate != b.SampleRate || a.Channels != b.Channels || a.ObjectType != b.ObjectType {
		return false
	}
	return bytes.Equal(a.ASC, b.ASC)
}
