package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type AVCConfig struct {
	Profile       byte
	Compatibility byte
	Level         byte
	LengthSize    int
	SPS           [][]byte
	PPS           [][]byte
}

func ParseAVCDecoderConfig(data []byte) (AVCConfig, error) {
	cfg := AVCConfig{}
	if len(data) < 7 {
		return cfg, fmt.Errorf("avc config too short")
	}
	if data[0] != 1 {
		return cfg, fmt.Errorf("avc config version not 1")
	}
	cfg.Profile = data[1]
	cfg.Compatibility = data[2]
	cfg.Level = data[3]
	cfg.LengthSize = int((data[4] & 0x03) + 1)
	if cfg.LengthSize != 4 {
		return cfg, fmt.Errorf("unsupported nalu length size %d", cfg.LengthSize)
	}

	numSPS := int(data[5] & 0x1f)
	offset := 6
	for i := 0; i < numSPS; i++ {
		if len(data) < offset+2 {
			return cfg, fmt.Errorf("invalid sps length")
		}
		spsLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if len(data) < offset+spsLen {
			return cfg, fmt.Errorf("invalid sps data")
		}
		cfg.SPS = append(cfg.SPS, append([]byte(nil), data[offset:offset+spsLen]...))
		offset += spsLen
	}
	if len(data) < offset+1 {
		return cfg, fmt.Errorf("invalid pps count")
	}
	numPPS := int(data[offset])
	offset++
	for i := 0; i < numPPS; i++ {
		if len(data) < offset+2 {
			return cfg, fmt.Errorf("invalid pps length")
		}
		ppsLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if len(data) < offset+ppsLen {
			return cfg, fmt.Errorf("invalid pps data")
		}
		cfg.PPS = append(cfg.PPS, append([]byte(nil), data[offset:offset+ppsLen]...))
		offset += ppsLen
	}
	return cfg, nil
}

func EqualAVCConfig(a, b AVCConfig) bool {
	if a.Profile != b.Profile || a.Compatibility != b.Compatibility || a.Level != b.Level || a.LengthSize != b.LengthSize {
		return false
	}
	if len(a.SPS) != len(b.SPS) || len(a.PPS) != len(b.PPS) {
		return false
	}
	for i := range a.SPS {
		if !bytes.Equal(a.SPS[i], b.SPS[i]) {
			return false
		}
	}
	for i := range a.PPS {
		if !bytes.Equal(a.PPS[i], b.PPS[i]) {
			return false
		}
	}
	return true
}
