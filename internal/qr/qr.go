// Package qr renders a QR code for the terminal.
package qr

import (
	"strings"

	qrcode "github.com/skip2/go-qrcode"
)

// Render returns the QR for text as lines of half-block characters (two
// module rows per text line) with a quiet zone. When invert is true, light
// modules are drawn as blocks — the right choice on a dark terminal so the
// code appears as dark modules on a light square, which every scanner reads.
func Render(text string, invert bool) (string, error) {
	q, err := qrcode.New(text, qrcode.Medium)
	if err != nil {
		return "", err
	}
	q.DisableBorder = false
	bm := q.Bitmap() // includes a 4-module quiet zone
	dark := func(x, y int) bool {
		if y < 0 || y >= len(bm) || x < 0 || x >= len(bm[y]) {
			return false
		}
		return bm[y][x] != invert // XOR: invert flips which modules are "on"
	}
	var b strings.Builder
	for y := 0; y < len(bm); y += 2 {
		for x := 0; x < len(bm[0]); x++ {
			top, bottom := dark(x, y), dark(x, y+1)
			switch {
			case top && bottom:
				b.WriteString("█")
			case top:
				b.WriteString("▀")
			case bottom:
				b.WriteString("▄")
			default:
				b.WriteString(" ")
			}
		}
		b.WriteString("\n")
	}
	return b.String(), nil
}
