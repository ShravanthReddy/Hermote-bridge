package update

import (
	"regexp"
	"strings"
)

// maxNotes bounds the notes a check returns; More counts the rest (§3).
const maxNotes = 50

var (
	// commitPrefix is GoReleaser's "<sha>: " before each subject.
	commitPrefix = regexp.MustCompile(`^[0-9a-f]{7,40}: `)
	// authorSuffix is GoReleaser's trailing " (<author> <email>)".
	authorSuffix = regexp.MustCompile(` \([^()]*\)$`)
)

// releaseNotes extracts commit subjects from a release body: each "* " or
// "- " list item, without its commit hash prefix and trailing author. The
// body's headings, blank lines and install footer are not subjects and are
// dropped (F17: "* <sha>: <subject> (<author> <email>)").
func releaseNotes(body string) (notes []string, more int) {
	notes = []string{}
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		item, ok := strings.CutPrefix(line, "* ")
		if !ok {
			item, ok = strings.CutPrefix(line, "- ")
		}
		if !ok {
			continue
		}
		subject := strings.TrimSpace(authorSuffix.ReplaceAllString(commitPrefix.ReplaceAllString(strings.TrimSpace(item), ""), ""))
		if subject == "" {
			continue
		}
		if len(notes) == maxNotes {
			more++
			continue
		}
		notes = append(notes, subject)
	}
	return notes, more
}
