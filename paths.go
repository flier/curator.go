package curator

import (
	"bytes"
	"strings"
)

const (
	PATH_SEPARATOR = "/"
)

type PathAndNode struct {
	Path, Node string
}

func NewPathAndNode(path string) *PathAndNode {
	if idx := strings.LastIndex(path, PATH_SEPARATOR); idx < 0 {
		return &PathAndNode{path, ""}
	} else if idx+1 >= len(path) {
		return &PathAndNode{PATH_SEPARATOR, ""}
	} else if idx > 0 {
		return &PathAndNode{path[:idx], path[idx+1:]}
	} else {
		return &PathAndNode{PATH_SEPARATOR, path[idx+1:]}
	}
}

func MakePath(parent string, children ...string) string {
	path := new(bytes.Buffer)

	if len(parent) > 0 {
		if !strings.HasPrefix(parent, PATH_SEPARATOR) {
			path.WriteString(PATH_SEPARATOR)
		}

		if strings.HasSuffix(parent, PATH_SEPARATOR) {
			path.WriteString(parent[:len(parent)-1])
		} else {
			path.WriteString(parent)
		}
	}

	for _, child := range children {
		if len(child) == 0 || child == PATH_SEPARATOR {
			if path.Len() == 0 {
				path.WriteString(PATH_SEPARATOR)
			}
		} else {
			path.WriteString(PATH_SEPARATOR)

			if strings.HasPrefix(child, PATH_SEPARATOR) {
				child = child[1:]
			}

			if strings.HasSuffix(child, PATH_SEPARATOR) {
				child = child[:len(child)-1]
			}

			path.WriteString(child)
		}
	}

	return path.String()
}

// Validate the provided znode path string
func ValidatePath(path string) error {
	return nil
}
