package packager

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"time"

	"github.com/Eyevinn/mp4ff/mp4"

	"tokuly-live-rtmp-server/pkg/hls"
	"tokuly-live-rtmp-server/pkg/storage"
	"tokuly-live-rtmp-server/pkg/util"
)

type Config struct {
	SegmentDuration     time.Duration
	PartDuration        time.Duration
	PlaylistWindow      time.Duration
	TargetDuration      time.Duration
	HoldBack            time.Duration
	PartHoldBack        time.Duration
	KeepSegments        int
	RewindPlaylistWindow time.Duration
	InitFilename        string
	SegmentFilenameTmpl string
	PartFilenameTmpl    string
	PlaylistName        string
	RewindPlaylistName  string
	EnablePartial       bool
}

type Packager struct {
	cfg       Config
	storage   *storage.Storage
	streamID  string
	playlist  *hls.PlaylistManager
	rewind    *hls.PlaylistManager
	videoID   uint32
	audioID   uint32
	videoTS   uint32
	audioTS   uint32
	avcConfig util.AVCConfig
	aacConfig util.AACConfig

	initWritten bool

	started        bool
	startTSMS      int64
	segmentOffset  uint64
	lastSegmentSeq uint64
	fragmentSeq    uint32

	partDurationMS    int64
	segmentDurationMS int64

	currentPart    *partBuilder
	currentSegment *segmentBuilder

	videoState trackState
	audioState trackState

	pendingDiscontinuity bool
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

type partBuilder struct {
	segSeq   uint64
	partIdx  int
	startMS  int64
	endMS    int64
	samples  []trackSample
	duration time.Duration
}

type segmentBuilder struct {
	seq        uint64
	startMS    int64
	buffer     bytes.Buffer
	parts      []string
	durationMS int64
}

func New(cfg Config, storage *storage.Storage, streamID string) *Packager {
	liveCfg := hls.Config{
		SegmentDuration: cfg.SegmentDuration,
		PartDuration:    cfg.PartDuration,
		PlaylistWindow:  cfg.PlaylistWindow,
		TargetDuration:  cfg.TargetDuration,
		HoldBack:        cfg.HoldBack,
		PartHoldBack:    cfg.PartHoldBack,
		KeepSegments:    cfg.KeepSegments,
		EnablePartial:   cfg.EnablePartial,
		InitFilename:    cfg.InitFilename,
		PlaylistName:    cfg.PlaylistName,
	}
	p := &Packager{
		cfg:              cfg,
		storage:          storage,
		streamID:         streamID,
		playlist:         hls.New(liveCfg, storage, streamID),
		partDurationMS:   int64(cfg.PartDuration / time.Millisecond),
		segmentDurationMS: int64(cfg.SegmentDuration / time.Millisecond),
		videoTS:          90000,
	}
	p.videoState.sampleIsVideo = true
	if storage.EnableRewind {
		rewindCfg := hls.Config{
			SegmentDuration: cfg.SegmentDuration,
			PartDuration:    cfg.PartDuration,
			PlaylistWindow:  cfg.RewindPlaylistWindow,
			TargetDuration:  cfg.TargetDuration,
			HoldBack:        cfg.HoldBack,
			PartHoldBack:    cfg.PartHoldBack,
			KeepSegments:    int(cfg.RewindPlaylistWindow / cfg.SegmentDuration),
			EnablePartial:   false,
			InitFilename:    cfg.InitFilename,
			PlaylistName:    cfg.RewindPlaylistName,
		}
		p.rewind = hls.New(rewindCfg, storage, streamID)
	}
	p.resumeFromExisting()
	return p
}

func (p *Packager) resumeFromExisting() {
	livePath := filepath.Join(p.storage.StreamDir(p.streamID), p.cfg.PlaylistName)
	lastSeq, hasSegments, err := p.playlist.LoadFromFile(livePath, true)
	if err != nil {
		log.Printf("resume live playlist error: %v", err)
	}
	if p.rewind != nil {
		rewindPath := filepath.Join(p.storage.RewindDir(p.streamID), p.cfg.RewindPlaylistName)
		rewindLast, rewindHas, err := p.rewind.LoadFromFile(rewindPath, true)
		if err != nil {
			log.Printf("resume rewind playlist error: %v", err)
		}
		if !hasSegments && rewindHas {
			lastSeq = rewindLast
			hasSegments = true
		}
	}
	if hasSegments {
		p.segmentOffset = lastSeq
		p.lastSegmentSeq = lastSeq
		p.pendingDiscontinuity = true
	}
}

func (p *Packager) UpdateVideoConfig(cfg util.AVCConfig) error {
	if p.initWritten && !util.EqualAVCConfig(p.avcConfig, cfg) {
		p.reset(false)
	}
	p.avcConfig = cfg
	p.videoState.sampleIsVideo = true
	return p.maybeWriteInit()
}

func (p *Packager) UpdateAudioConfig(cfg util.AACConfig) error {
	if p.initWritten && ((p.aacConfig.SampleRate != 0 && p.aacConfig.SampleRate != cfg.SampleRate) || p.audioID == 0) {
		p.reset(false)
	}
	p.aacConfig = cfg
	p.audioTS = uint32(cfg.SampleRate)
	p.audioState.defaultDurMS = int64(math.Round(1024.0 * 1000.0 / float64(cfg.SampleRate)))
	if p.audioState.defaultDurMS == 0 {
		p.audioState.defaultDurMS = 20
	}
	return p.maybeWriteInit()
}

func (p *Packager) AddVideoSample(tsMS int64, ctsMS int64, data []byte, isKey bool) error {
	return p.addSample(true, pendingSample{dtsMS: tsMS, ctsMS: ctsMS, data: data, isKey: isKey})
}

func (p *Packager) AddAudioSample(tsMS int64, data []byte) error {
	return p.addSample(false, pendingSample{dtsMS: tsMS, ctsMS: 0, data: data, isKey: false})
}

func (p *Packager) Flush() error {
	if err := p.flushTrack(&p.videoState); err != nil {
		return err
	}
	if err := p.flushTrack(&p.audioState); err != nil {
		return err
	}
	return p.finalizeSegment()
}

func (p *Packager) addSample(isVideo bool, sample pendingSample) error {
	if !p.initWritten {
		return nil
	}
	if !isVideo && p.aacConfig.SampleRate == 0 {
		return nil
	}
	p.ensureStart(sample.dtsMS)
	if sample.dtsMS < p.startTSMS {
		sample.dtsMS = p.startTSMS
	}

	statePtr := &p.audioState
	if isVideo {
		statePtr = &p.videoState
	}

	full, err := p.ingestSample(statePtr, sample)
	if err != nil {
		return err
	}
	if full == nil {
		return nil
	}
	return p.appendToPart(*full)
}

func (p *Packager) ingestSample(state *trackState, sample pendingSample) (*trackSample, error) {
	if state.timescale == 0 {
		if state.sampleIsVideo {
			state.timescale = p.videoTS
			state.trackID = p.videoID
			if state.defaultDurMS == 0 {
				state.defaultDurMS = 33
			}
		} else {
			state.timescale = p.audioTS
			state.trackID = p.audioID
		}
	}
	if state.hasStarted && state.lastDTSMS != 0 {
		if absInt64(sample.dtsMS-state.lastDTSMS) > 5000 {
			p.reset(true)
			state.pending = nil
			state.hasStarted = false
			state.lastDTSMS = 0
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

func (p *Packager) flushTrack(state *trackState) error {
	if state.pending == nil || !p.initWritten {
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
	return p.appendToPart(trackSample{trackID: state.trackID, sample: full})
}

func (p *Packager) appendToPart(ts trackSample) error {
	partIdx, segSeq, partStartMS := p.computePartIndex(ts.sample.DecodeTime, ts.trackID == p.videoID)
	partEndMS := partStartMS + p.partDurationMS

	if p.currentPart == nil || partIdx != p.currentPart.partIdx || segSeq != p.currentPart.segSeq {
		if err := p.finalizePart(); err != nil {
			return err
		}
		p.currentPart = &partBuilder{
			segSeq:  segSeq,
			partIdx: partIdx,
			startMS: partStartMS,
			endMS:   partEndMS,
		}
	}
	p.currentPart.samples = append(p.currentPart.samples, ts)
	return nil
}

func (p *Packager) computePartIndex(decodeTime uint64, isVideo bool) (int, uint64, int64) {
	var tsMS int64
	if isVideo {
		tsMS = timescaleToMS(decodeTime, p.videoTS)
	} else {
		tsMS = timescaleToMS(decodeTime, p.audioTS)
	}
	rel := tsMS - p.startTSMS
	if rel < 0 {
		rel = 0
	}
	partsPerSeg := int(p.segmentDurationMS / p.partDurationMS)
	if partsPerSeg <= 0 {
		partsPerSeg = 1
	}
	globalPartIdx := int(rel / p.partDurationMS)
	segIdx := int(rel / p.segmentDurationMS)
	segSeq := uint64(segIdx) + 1 + p.segmentOffset
	segStartMS := p.startTSMS + int64(segIdx)*p.segmentDurationMS
	partIdx := globalPartIdx - segIdx*partsPerSeg
	if partIdx < 0 {
		partIdx = 0
	}
	partStartMS := segStartMS + int64(partIdx)*p.partDurationMS
	return partIdx, segSeq, partStartMS
}

func (p *Packager) finalizePart() error {
	if p.currentPart == nil || len(p.currentPart.samples) == 0 {
		p.currentPart = nil
		return nil
	}
	segSeq := p.currentPart.segSeq
	if err := p.ensureSegmentBuilder(segSeq, p.currentPart.startMS); err != nil {
		return err
	}

	frag, err := p.createFragment(uint32(p.fragmentSeq+1), p.currentPart.samples)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := frag.Encode(&buf); err != nil {
		return err
	}
	partName := fmt.Sprintf(p.cfg.PartFilenameTmpl, segSeq, p.currentPart.partIdx)
	partPath := filepath.Join(p.storage.StreamDir(p.streamID), partName)
	if err := storage.WriteFileAtomic(partPath, buf.Bytes()); err != nil {
		return err
	}

	p.playlist.AddPart(segSeq, partName, time.Duration(p.partDurationMS)*time.Millisecond)
	p.currentSegment.parts = append(p.currentSegment.parts, partName)
	p.currentSegment.buffer.Write(buf.Bytes())
	p.currentSegment.durationMS += p.partDurationMS
	p.fragmentSeq++

	if p.rewind != nil {
		// No parts for rewind
	}

	if p.currentPart.endMS >= p.currentSegment.startMS+p.segmentDurationMS {
		if err := p.finalizeSegment(); err != nil {
			return err
		}
	}

	p.currentPart = nil
	return p.playlist.Write()
}

func (p *Packager) finalizeSegment() error {
	if p.currentSegment == nil || p.currentSegment.buffer.Len() == 0 {
		return nil
	}
	segName := fmt.Sprintf(p.cfg.SegmentFilenameTmpl, p.currentSegment.seq)
	segPath := filepath.Join(p.storage.StreamDir(p.streamID), segName)
	if err := storage.WriteFileAtomic(segPath, p.currentSegment.buffer.Bytes()); err != nil {
		return err
	}
	p.playlist.FinalizeSegment(p.currentSegment.seq, segName, time.Duration(p.currentSegment.durationMS)*time.Millisecond)
	if p.rewind != nil {
		rewindDir := p.storage.RewindDir(p.streamID)
		rewindPath := filepath.Join(rewindDir, segName)
		_ = storage.CopyOrLink(segPath, rewindPath)
		p.rewind.FinalizeSegment(p.currentSegment.seq, segName, time.Duration(p.currentSegment.durationMS)*time.Millisecond)
		removedRewind := p.rewind.Prune()
		for _, seg := range removedRewind {
			if seg.URI != "" {
				_ = storage.RemoveFile(filepath.Join(rewindDir, seg.URI))
			}
		}
		_ = p.rewind.WriteTo(rewindDir)
	}
	removed := p.playlist.Prune()
	_ = p.playlist.RemoveFiles(removed)
	if err := p.playlist.Write(); err != nil {
		return err
	}
	p.lastSegmentSeq = p.currentSegment.seq
	p.currentSegment = nil
	return nil
}

func (p *Packager) ensureSegmentBuilder(segSeq uint64, startMS int64) error {
	if p.currentSegment != nil && p.currentSegment.seq == segSeq {
		return nil
	}
	if p.currentSegment != nil {
		if err := p.finalizeSegment(); err != nil {
			return err
		}
	}
	segStartMS := startMS - (startMS-p.startTSMS)%p.segmentDurationMS
	p.currentSegment = &segmentBuilder{
		seq:     segSeq,
		startMS: segStartMS,
	}
	if p.pendingDiscontinuity {
		p.playlist.MarkDiscontinuityNext()
		if p.rewind != nil {
			p.rewind.MarkDiscontinuityNext()
		}
		p.pendingDiscontinuity = false
	}
	return nil
}

func (p *Packager) createFragment(seqNumber uint32, samples []trackSample) (*mp4.Fragment, error) {
	var trackIDs []uint32
	if p.videoID != 0 {
		trackIDs = append(trackIDs, p.videoID)
	}
	if p.audioID != 0 {
		trackIDs = append(trackIDs, p.audioID)
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

func addEmptyTrack(initSeg *mp4.InitSegment, timeScale uint32, mediaType, language string) *mp4.TrakBox {
	moov := initSeg.Moov
	trackID := uint32(len(moov.Traks) + 1)
	moov.Mvhd.NextTrackID = trackID + 1
	newTrak := mp4.CreateEmptyTrak(trackID, timeScale, mediaType, language)
	moov.AddChild(newTrak)
	moov.Mvex.AddChild(mp4.CreateTrex(trackID))
	return newTrak
}

func (p *Packager) maybeWriteInit() error {
	if len(p.avcConfig.SPS) == 0 || len(p.avcConfig.PPS) == 0 {
		return nil
	}
	if p.initWritten {
		return nil
	}
	init := mp4.CreateEmptyInit()
	videoTrak := addEmptyTrack(init, p.videoTS, "video", "und")
	if err := videoTrak.SetAVCDescriptor("avc1", p.avcConfig.SPS, p.avcConfig.PPS, true); err != nil {
		return err
	}
	p.videoID = videoTrak.Tkhd.TrackID
	p.videoState.trackID = p.videoID
	p.videoState.timescale = p.videoTS
	p.videoState.defaultDurMS = 33
	if p.aacConfig.SampleRate > 0 {
		p.audioTS = uint32(p.aacConfig.SampleRate)
		audioTrak := addEmptyTrack(init, p.audioTS, "audio", "und")
		esds := mp4.CreateEsdsBox(p.aacConfig.ASC)
		channels := p.aacConfig.Channels
		if channels == 0 {
			channels = 2
		}
		mp4a := mp4.CreateAudioSampleEntryBox("mp4a", uint16(channels), 16, uint16(p.aacConfig.SampleRate), esds)
		audioTrak.Mdia.Minf.Stbl.Stsd.AddChild(mp4a)
		p.audioID = audioTrak.Tkhd.TrackID
		p.audioState.trackID = p.audioID
		p.audioState.timescale = p.audioTS
	}

	var buf bytes.Buffer
	if err := init.Encode(&buf); err != nil {
		return err
	}
	livePath := filepath.Join(p.storage.StreamDir(p.streamID), p.cfg.InitFilename)
	if err := storage.WriteFileAtomic(livePath, buf.Bytes()); err != nil {
		return err
	}
	if p.storage.EnableRewind {
		rewindPath := filepath.Join(p.storage.RewindDir(p.streamID), p.cfg.InitFilename)
		_ = storage.WriteFileAtomic(rewindPath, buf.Bytes())
		if p.rewind != nil {
			rewindDir := p.storage.RewindDir(p.streamID)
			_ = p.rewind.WriteTo(rewindDir)
		}
	}
	p.initWritten = true
	return p.playlist.Write()
}

func (p *Packager) ensureStart(tsMS int64) {
	if p.started {
		return
	}
	p.started = true
	p.startTSMS = tsMS
}

func (p *Packager) reset(reinit bool) {
	_ = p.finalizePart()
	_ = p.finalizeSegment()
	p.pendingDiscontinuity = true
	p.segmentOffset = p.lastSegmentSeq
	p.startTSMS = 0
	p.started = false
	p.currentPart = nil
	p.currentSegment = nil
	p.videoState.pending = nil
	p.audioState.pending = nil
	p.initWritten = false
	p.videoID = 0
	p.audioID = 0
	p.fragmentSeq = 0
	if reinit {
		_ = p.maybeWriteInit()
	}
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

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
