package torrent

import (
	"fmt"
	"os"

	"github.com/anacrolix/torrent/bencode"
)

type File struct {
	Info         Info       `bencode:"info"`
	Announce     string     `bencode:"announce"`
	AnnounceList [][]string `bencode:"announce-list,omitempty"`
	Comment      string     `bencode:"comment,omitempty"`
	UserID       string     `bencode:"uid,omitempty"`
	Section      string     `bencode:"section,omitempty"`
	CreationDate int64      `bencode:"creation date,omitempty,ignore_unmarshal_type_error"`
	CreatedBy    string     `bencode:"created by,omitempty"`
	Resume       Resume     `bencode:"libtorrent_resume,omitempty"`
	RTorrent     RTorrent   `bencode:"rtorrent,omitempty"`
	Encoding     string     `bencode:"encoding,omitempty"`
}

func ReadFile(filename string) (*File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	tFile := &File{}

	dec := bencode.NewDecoder(file)

	if err := dec.Decode(tFile); err != nil {
		return nil, fmt.Errorf("decode torrent file %s: %w", filename, err)
	}

	return tFile, nil
}

func ReadFileFromBytes(data []byte) (*File, error) {
	tFile := &File{}

	if err := bencode.Unmarshal(data, &tFile); err != nil {
		return nil, fmt.Errorf("unmarshal torrent content: %w", err)
	}

	return tFile, nil
}

func WriteFile(filename string, tFile *File) error {
	file, err := os.OpenFile(filename, os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	dec := bencode.NewEncoder(file)

	if err := dec.Encode(tFile); err != nil {
		return fmt.Errorf("encode torrent content to file %s: %w", filename, err)
	}

	return nil
}
