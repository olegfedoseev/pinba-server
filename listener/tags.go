package listener

import (
	"sort"
	"bytes"
	"regexp"
)

type Tag struct {
	Key   string
	Value string
}

type Tags []Tag

func (t Tags) Len() int           { return len(t) }
func (t Tags) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t Tags) Less(i, j int) bool { return t[i].Key < t[j].Key }

func (t Tags) String() string {
	sort.Sort(t)
	var b bytes.Buffer
	for i, tag := range t {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(tag.Key)
		b.WriteString("=")
		b.WriteString(tag.Value)
	}
	return b.String()
}

func (t Tags) Get(key string) (string, error) {
	for _, tag := range t {
		if tag.Key == "server" {
			return tag.Value, nil
		}
	}
	return nil, errors.New("no such tag")
}

var tags_buffer map[string]string

func (t Tags) Filter(filter *map[string]bool) string {
	sort.Sort(t)
	var b bytes.Buffer
	// may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'.

	if tags_buffer == nil {
		tags_buffer = make(map[string]string, 0)
	}

	re := regexp.MustCompile("[^\\w\\d\\.\\/\\-\\_]")
	for i, tag := range t {
		if filter != nil {
			if val, ok := (*filter)[tag.Key]; !ok || !val {
				continue
			}
		}
		// Fuck... duplicated tag name
		if tag.Value == "dbwrite-proxy" || tag.Value == "dbread-proxy" {
			continue
		}
		if i > 0 && b.Len() > 0 {
			b.WriteString(" ")
		}

		//var value string
		if _, ok := tags_buffer[tag.Value]; !ok {
			tags_buffer[tag.Value] = re.ReplaceAllString(tag.Value, "_")
		}

		b.WriteString(tag.Key)
		b.WriteString("=")
		b.WriteString(tags_buffer[tag.Value])
	}
	return b.String()
}
