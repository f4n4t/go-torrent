package torrent_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/f4n4t/go-torrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_IncludeFastResume(t *testing.T) {
	tests := []struct {
		name        string
		torrentFile string
		baseDir     string
		files       []string
		expected    torrent.File
	}{
		{
			name:        "singlefile",
			torrentFile: "./testdata/fastresume_singlefile.torrent",
			baseDir:     "./testdata",
			files:       []string{"./testdata/fastresume_singlefile.data"}, // need to be in the right order, to set timestamps correctly
			expected: torrent.File{
				RTorrent: torrent.RTorrent{
					ChunksDone:        1,
					ChunksWanted:      0,
					Complete:          1,
					Directory:         "./testdata",
					Hashing:           0,
					State:             1,
					StateChanged:      time.Now().Unix(),
					StateCounter:      1,
					TimestampFinished: 0,
					TimestampStarted:  time.Now().Unix(),
				},
				Resume: torrent.Resume{
					Bitfield: 1,
					Files: []torrent.ResumeFile{
						{Completed: 1, Priority: 0}, // we need to set the mtime from the file
					},
					UncertainPiecesTimestamp: time.Now().Unix(),
				},
			},
		},
		{
			name:        "multifile",
			torrentFile: "./testdata/fastresume_multifile.torrent",
			baseDir:     "./testdata",
			files:       []string{"./testdata/fastresume_multifile/filea", "./testdata/fastresume_multifile/fileb"},
			expected: torrent.File{
				RTorrent: torrent.RTorrent{
					ChunksDone:        1,
					ChunksWanted:      0,
					Complete:          1,
					Directory:         "./testdata/fastresume_multifile",
					Hashing:           0,
					State:             1,
					StateChanged:      time.Now().Unix(),
					StateCounter:      1,
					TimestampFinished: 0,
					TimestampStarted:  time.Now().Unix(),
				},
				Resume: torrent.Resume{
					Bitfield: 1,
					Files: []torrent.ResumeFile{
						{Completed: 1, Priority: 0},
						{Completed: 1, Priority: 0},
					},
					UncertainPiecesTimestamp: time.Now().Unix(),
				},
			},
		},
	}

	torrentService := torrent.NewServiceBuilder().Build()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set timestamps
			for i, f := range tt.files {
				fileInfo, err := os.Stat(f)
				require.NoError(t, err)

				// set mtime
				tt.expected.Resume.Files[i].Mtime = fileInfo.ModTime().Unix()
			}

			torrentContent, err := os.ReadFile(tt.torrentFile)
			require.NoError(t, err)

			newContent, err := torrentService.IncludeFastResume(torrentContent, tt.baseDir)
			require.NoError(t, err)

			file := torrent.File{}

			err = bencode.Unmarshal(newContent, &file)
			require.NoError(t, err)

			CheckEqual(t, tt.expected, file)
		})
	}
}

// CheckEqual compares two torrent.File structs
func CheckEqual(t *testing.T, expected, actual torrent.File) {
	assert.Equal(t, expected.RTorrent.ChunksDone, actual.RTorrent.ChunksDone)
	assert.Equal(t, expected.RTorrent.ChunksWanted, actual.RTorrent.ChunksWanted)
	assert.Equal(t, expected.RTorrent.Complete, actual.RTorrent.Complete)

	// get absoulte path
	absExpectedDirectory, err := filepath.Abs(expected.RTorrent.Directory)
	require.NoError(t, err)
	absActualDirectory, err := filepath.Abs(actual.RTorrent.Directory)
	require.NoError(t, err)

	assert.Equal(t, absExpectedDirectory, absActualDirectory)

	assert.Equal(t, expected.RTorrent.Hashing, actual.RTorrent.Hashing)
	assert.Equal(t, expected.RTorrent.State, actual.RTorrent.State)
	assert.Equal(t, expected.RTorrent.StateChanged, actual.RTorrent.StateChanged)
	assert.LessOrEqual(t, expected.RTorrent.StateChanged, actual.RTorrent.StateChanged)
	assert.Equal(t, expected.RTorrent.StateCounter, actual.RTorrent.StateCounter)

	assert.Equal(t, expected.RTorrent.TimestampFinished, actual.RTorrent.TimestampFinished)

	assert.LessOrEqual(t, expected.RTorrent.TimestampStarted, actual.RTorrent.TimestampStarted)
	assert.Equal(t, expected.Resume.Bitfield, actual.Resume.Bitfield)
	assert.LessOrEqual(t, expected.Resume.UncertainPiecesTimestamp, actual.Resume.UncertainPiecesTimestamp)
	assert.Len(t, expected.Resume.Files, len(actual.Resume.Files))

	// check each file
	for i := range expected.Resume.Files {
		assert.Equal(t, expected.Resume.Files[i].Completed, actual.Resume.Files[i].Completed)
		assert.LessOrEqual(t, expected.Resume.Files[i].Mtime, actual.Resume.Files[i].Mtime)
		assert.Equal(t, expected.Resume.Files[i].Priority, actual.Resume.Files[i].Priority)
	}
}
