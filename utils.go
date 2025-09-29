package torrent

// GetPieceLength returns the fitting piece length for the given size.
func GetPieceLength(totalLength int64) int64 {
	switch {
	case totalLength > 53687091200: // > 50GB
		return 16777216 // 2^24
	case totalLength >= 21474836480: // >= 20GB
		return 8388608 // 2^23
	case totalLength >= 5368709120: // >= 5GB
		return 4194304 // 2^22
	case totalLength >= 3221225472: // >= 3GB
		return 2097152 // 2^21
	case totalLength >= 1073741824: // >= 1GB
		return 1048576 // 2^20
	default: // < 1GB
		return 262144 // 2^18
	}
}
