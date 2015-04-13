package curator

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	PATH_SEPARATOR = "/"
)

type PathAndNode struct {
	Path, Node string
}

func SplitPath(path string) (*PathAndNode, error) {
	if idx := strings.LastIndex(path, PATH_SEPARATOR); idx < 0 {
		return &PathAndNode{path, ""}, nil
	} else if idx > 0 {
		return &PathAndNode{path[:idx], path[idx+1:]}, nil
	} else {
		return &PathAndNode{PATH_SEPARATOR, path[idx+1:]}, nil
	}
}

func JoinPath(parent string, children ...string) string {
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

var (
	invalidCharaters = &unicode.RangeTable{
		R16: []unicode.Range16{
			{Lo: 0x0000, Hi: 0x001f, Stride: 1},
			{Lo: 0x007f, Hi: 0x009F, Stride: 1},
			{Lo: 0xd800, Hi: 0xf8ff, Stride: 1},
			{Lo: 0xfff0, Hi: 0xffff, Stride: 1},
		},
	}
)

// Validate the provided znode path string
func ValidatePath(path string) error {
	if len(path) == 0 {
		return errors.New("Path cannot be null")
	}

	if !strings.HasPrefix(path, PATH_SEPARATOR) {
		return errors.New("Path must start with / character")
	}

	if len(path) == 1 {
		return nil
	}

	if strings.HasSuffix(path, PATH_SEPARATOR) {
		return errors.New("Path must not end with / character")
	}

	lastc := '/'

	for i, c := range path {
		if i == 0 {
			continue
		} else if c == 0 {
			return fmt.Errorf("null character not allowed @ %d", i)
		} else if c == '/' && lastc == '/' {
			return fmt.Errorf("empty node name specified @ %d", i)
		} else if c == '.' && lastc == '.' {
			if path[i-2] == '/' && (i+1 == len(path) || path[i+1] == '/') {
				return fmt.Errorf("relative paths not allowed @ %d", i)
			}
		} else if c == '.' {
			if path[i-1] == '/' && (i+1 == len(path) || path[i+1] == '/') {
				return fmt.Errorf("relative paths not allowed @ %d", i)
			}
		} else if unicode.In(c, invalidCharaters) {
			return fmt.Errorf("invalid charater @ %d", i)
		}

		lastc = c
	}

	return nil
}

type ZookeeperConnection interface {
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)

	Exists(path string) (bool, *zk.Stat, error)
}

// Make sure all the nodes in the path are created
func MakeDirs(conn ZookeeperConnection, path string, makeLastNode bool, aclProvider ACLProvider) error {
	if err := ValidatePath(path); err != nil {
		return err
	}

	pos := 1 // skip first slash, root is guaranteed to exist

	for pos < len(path) {
		if idx := strings.Index(path[pos+1:], PATH_SEPARATOR); idx == -1 {
			if makeLastNode {
				pos = len(path)
			} else {
				return nil
			}
		} else {
			pos += idx + 1
		}

		subPath := path[:pos]

		if exists, _, err := conn.Exists(subPath); err != nil {
			return err
		} else if !exists {
			var acls []zk.ACL

			if aclProvider != nil {
				if acls = aclProvider.GetAclForPath(subPath); len(acls) == 0 {
					acls = aclProvider.GetDefaultAcl()
				}
			}

			if acls == nil {
				acls = zk.WorldACL(zk.PermAll)
			}

			if _, err := conn.Create(subPath, []byte{}, int32(PERSISTENT), acls); err != nil && err != zk.ErrNodeExists {
				return err
			}
		}
	}

	return nil
}
