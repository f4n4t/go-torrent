package torrent_test

import (
	"os"
	"testing"

	"github.com/f4n4t/go-torrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTorrent_ReadSingleFile(t *testing.T) {
	filename := "./testdata/singlefile.torrent"

	tFile, err := torrent.ReadFile(filename)
	require.NoError(t, err)

	assert.Equal(t, "movie.mkv", tFile.Info.Name)
	assert.Equal(t, int64(1048576000), tFile.Info.Length)
	assert.Equal(t, int64(524288), tFile.Info.PieceLength)
	assert.Equal(t, "https://example.com/announce.php?torrent_pass=asdf", tFile.Announce)
	assert.ElementsMatch(t,
		[]string{"https://example.com/announce.php?torrent_pass=asdf",
			"https://example2.com/announce.php?torrent_pass=asdf"},
		tFile.AnnounceList[0],
	)
	assert.Equal(t, "Best movie", tFile.Comment)
	// assert.Equal(t, "UTF-8", tFile.Encoding)
	assert.Equal(t, true, *tFile.Info.Private)
	assert.Equal(t, "somewhere", tFile.Info.Source)
}

func TestTorrent_ReadMultiFile(t *testing.T) {
	filename := "./testdata/multifile.torrent"

	tFile, err := torrent.ReadFile(filename)
	require.NoError(t, err)

	assert.Equal(t, "movie", tFile.Info.Name)
	assert.Equal(t, int64(524288), tFile.Info.PieceLength)

	assert.Equal(t, "movie.mkv", tFile.Info.Files[0].Path[0])
	assert.Equal(t, int64(1048576000), tFile.Info.Files[0].Length)
	assert.Equal(t, "movie.nfo", tFile.Info.Files[1].Path[0])
	assert.Equal(t, int64(16), tFile.Info.Files[1].Length)
	assert.ElementsMatch(t, tFile.AnnounceList[0], []string{"https://example.com/announce.php?torrent_pass=asdf", "https://example2.com/announce.php?torrent_pass=asdf"})
	assert.Equal(t, tFile.Comment, "Best movie")
	assert.Equal(t, *tFile.Info.Private, true)
	assert.Equal(t, tFile.Info.Source, "somewhere")
}

func TestTorrent_WriteFile(t *testing.T) {
	sourceFile := "./testdata/singlefile.torrent"
	targetFile, err := os.CreateTemp("testdata", "test.*.torrent")
	require.NoError(t, err)

	targetFileName := targetFile.Name()
	_ = targetFile.Close()
	defer func(f string) {
		_ = os.Remove(f)
	}(targetFileName)

	tFile, err := torrent.ReadFile(sourceFile)
	require.NoError(t, err)

	assert.Equal(t, tFile.Section, "")

	tFile.Section = "Movies"

	err = torrent.WriteFile(targetFileName, tFile)
	require.NoError(t, err)

	tFile2, err := torrent.ReadFile(targetFileName)
	require.NoError(t, err)

	assert.Equal(t, tFile2.Section, "Movies")
}
