package plan

import "testing"

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0 bytes"},
		{1023, "1023 bytes"},
		{1024, "1.0 kB"},
		{1536, "1.5 kB"},
		{1024 * 1024, "1.0 MB"},
		{6_373_908_480, "5.9 GB"}, // 778065 blocks * 8192 bytes, from the real plan that motivated this feature
		{1024 * 1024 * 1024 * 1024, "1.0 TB"},
	}

	for _, c := range cases {
		if got := FormatBytes(c.n); got != c.want {
			t.Errorf("FormatBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestFormatBytes_BlockSizeConversion(t *testing.T) {
	// 778065 blocks at the default 8kB page size is what a real HashAggregate
	// disk spill looked like - this pins the block-size -> bytes conversion.
	got := FormatBytes(778065 * DefaultBlockSize)
	want := "5.9 GB"
	if got != want {
		t.Errorf("FormatBytes(778065 * DefaultBlockSize) = %q, want %q", got, want)
	}
}
