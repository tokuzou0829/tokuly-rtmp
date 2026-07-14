package policy

import "testing"

func TestIsSupportedResolution(t *testing.T) {
	cfg := Config{
		MinWidth:  240,
		MinHeight: 240,
		MaxWidth:  1920,
		MaxHeight: 1920,
		MaxPixels: 1920 * 1080,
	}
	tests := []struct {
		name          string
		width, height int
		want          bool
	}{
		{name: "full HD landscape", width: 1920, height: 1080, want: true},
		{name: "HD landscape", width: 1280, height: 720, want: true},
		{name: "480p landscape rounded down", width: 852, height: 480, want: true},
		{name: "480p landscape rounded up", width: 854, height: 480, want: true},
		{name: "full HD portrait", width: 1080, height: 1920, want: true},
		{name: "HD portrait", width: 720, height: 1280, want: true},
		{name: "four by three", width: 640, height: 480, want: true},
		{name: "square", width: 1080, height: 1080, want: true},
		{name: "four by three near pixel limit", width: 1600, height: 1200, want: true},
		{name: "square at pixel limit", width: 1440, height: 1440, want: true},
		{name: "landscape larger than full HD", width: 2560, height: 1440, want: false},
		{name: "portrait larger than full HD", width: 1440, height: 2560, want: false},
		{name: "too many pixels", width: 1920, height: 1200, want: false},
		{name: "width too small", width: 200, height: 1920, want: false},
		{name: "height too small", width: 1920, height: 200, want: false},
		{name: "tiny square", width: 100, height: 100, want: false},
		{name: "unknown", width: 0, height: 0, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSupportedResolution(tt.width, tt.height, cfg); got != tt.want {
				t.Fatalf("isSupportedResolution(%d, %d) = %t, want %t", tt.width, tt.height, got, tt.want)
			}
		})
	}
}
