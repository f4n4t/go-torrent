package torrent_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/f4n4t/go-release"
	"github.com/f4n4t/go-torrent"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type file struct {
	name    string
	content []byte
}

func createTestDir(files ...file) (string, func(), error) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	tempDir, err := os.MkdirTemp("", "go-torrent")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	testDir := filepath.Join(tempDir, "testDir")
	if err := os.Mkdir(testDir, 0755); err != nil {
		return "", cleanup, err
	}

	for _, file := range files {
		f, err := os.Create(filepath.Join(testDir, file.name))
		if err != nil {
			return "", cleanup, err
		}

		if _, err := f.Write(file.content); err != nil {
			return "", cleanup, err
		}
	}

	return testDir, cleanup, nil
}

func removeDate(input []byte) []byte {
	// Suche nach "13:creation datei"
	if idx := bytes.Index(input, []byte("13:creation datei")); idx != -1 {
		return append(input[:idx], input[idx+28:]...)
	}
	return input
}

func TestCreateMultiFile(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	var files []file
	files = append(files, file{
		name:    "test1.txt",
		content: []byte("Hello\nWorld\n"),
	})
	files = append(files, file{
		name:    "test2.txt",
		content: []byte("This\nIs\nMe!\n"),
	})

	dirName, cleanup, err := createTestDir(files...)
	require.NoError(t, err)
	defer cleanup()

	releaseService := release.NewServiceBuilder().WithSkipPre(true).WithSkipMediaInfo(true).Build()
	torrentService := torrent.NewServiceBuilder().WithCreatedBy("go-torrent").Build()

	rel, _ := releaseService.Parse(dirName)
	got, err := torrentService.Create(rel)
	assert.NoError(t, err)

	expectedTorrent, err := os.ReadFile("./testdata/testDir.torrent")
	assert.NoError(t, err)

	got = removeDate(got)
	expectedTorrent = removeDate(expectedTorrent)

	assert.Equal(t, got, expectedTorrent)
}

func TestCreateSingleFile(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	testFile := file{
		name:    "test1.txt",
		content: []byte("Hello\nWorld\n"),
	}

	dirName, cleanup, err := createTestDir(testFile)
	require.NoError(t, err)
	defer cleanup()

	releaseService := release.NewServiceBuilder().WithSkipPre(true).WithSkipMediaInfo(true).Build()
	torrentService := torrent.NewServiceBuilder().WithCreatedBy("go-torrent").Build()

	rel, _ := releaseService.Parse(filepath.Join(dirName, testFile.name))
	got, err := torrentService.Create(rel)
	assert.NoError(t, err)

	expectedTorrent, err := os.ReadFile("./testdata/testFile.torrent")
	assert.NoError(t, err)

	got = removeDate(got)
	expectedTorrent = removeDate(expectedTorrent)

	assert.Equal(t, got, expectedTorrent)
}

func TestMarshalInfo(t *testing.T) {
	var info torrent.Info
	b, err := bencode.Marshal(info)
	assert.NoError(t, err)
	assert.EqualValues(t, "d4:name0:12:piece lengthi0e6:pieces0:e", string(b))
}
