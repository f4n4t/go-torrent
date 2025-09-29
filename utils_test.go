package torrent_test

import (
	"testing"

	"github.com/f4n4t/go-torrent"
	"github.com/stretchr/testify/assert"
)

func TestGetPieceLength(t *testing.T) {
	tests := []struct {
		name string
		size int64
		want int64
	}{
		{"10MB", 10485760, 262144},
		{"1GB", 1073741824, 1048576},
		{"3GB", 3221225472, 2097152},
		{"5GB", 5368709120, 4194304},
		{"21GB", 22548578304, 8388608},
		{"62GB", 66571993088, 16777216},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := torrent.GetPieceLength(tt.size)
			assert.Equal(t, tt.want, got)
		})
	}
}
