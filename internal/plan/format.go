package plan

import "fmt"

// DefaultBlockSize is PostgreSQL's default page size in bytes (BLCKSZ).
// Custom builds can compile with a different block size, hence --block-size.
const DefaultBlockSize int64 = 8192

// FormatBytes renders a byte count using PostgreSQL's pg_size_pretty-style
// units (kB, MB, GB, ... on a 1024 base).
func FormatBytes(n int64) string {
	const unit = 1024.0

	units := [...]string{"bytes", "kB", "MB", "GB", "TB", "PB"}
	size := float64(n)

	i := 0
	for size >= unit && i < len(units)-1 {
		size /= unit
		i++
	}

	if i == 0 {
		return fmt.Sprintf("%d bytes", n)
	}
	return fmt.Sprintf("%.1f %s", size, units[i])
}
