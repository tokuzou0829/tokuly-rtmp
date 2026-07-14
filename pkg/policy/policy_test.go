package policy

import "testing"

func TestIsSupportedResolution(t *testing.T) {
	tests := []struct {
		name          string
		width, height int
		want          bool
	}{
		{name: "full HD landscape", width: 1920, height: 1080, want: true},
		{name: "HD landscape", width: 1280, height: 720, want: true},
		{name: "full HD portrait", width: 1080, height: 1920, want: true},
		{name: "HD portrait", width: 720, height: 1280, want: true},
		{name: "landscape larger than full HD", width: 2560, height: 1440, want: false},
		{name: "portrait larger than full HD", width: 1440, height: 2560, want: false},
		{name: "square", width: 1080, height: 1080, want: false},
		{name: "four by three", width: 1440, height: 1080, want: false},
		{name: "unknown", width: 0, height: 0, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSupportedResolution(tt.width, tt.height, 1920, 1080); got != tt.want {
				t.Fatalf("isSupportedResolution(%d, %d) = %t, want %t", tt.width, tt.height, got, tt.want)
			}
		})
	}
}
