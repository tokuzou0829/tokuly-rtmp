package util

import (
	"bytes"
	"fmt"

	"github.com/Eyevinn/mp4ff/aac"
)

type AACConfig struct {
	ASC        []byte
	ObjectType byte
	SampleRate int
	Channels   int
}

func ParseAudioSpecificConfig(data []byte) (AACConfig, error) {
	cfg := AACConfig{}
	if len(data) == 0 {
		return cfg, fmt.Errorf("aac config empty")
	}
	asc, err := aac.DecodeAudioSpecificConfig(bytes.NewReader(data))
	if err != nil {
		return cfg, err
	}
	cfg.ASC = append([]byte(nil), data...)
	cfg.ObjectType = asc.ObjectType
	cfg.SampleRate = asc.SamplingFrequency
	cfg.Channels = int(asc.ChannelConfiguration)
	return cfg, nil
}
