package helpers

import "strings"

func ReplaceSp(qString string) string {
	s := strings.Replace(string(qString), "{", "\\{", -1)
	s = strings.Replace(s, "}", "\\}", -1)
	s = strings.Replace(s, ":", "\\:", -1)
	s = strings.Replace(s, "[", "\\[", -1)
	s = strings.Replace(s, "]", "\\]", -1)
	return s
}
