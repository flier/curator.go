package curator

import (
	"github.com/samuel/go-zookeeper/zk"
)

type ACLProvider interface {
	// Return the ACL list to use by default
	GetDefaultAcl() []zk.ACL

	// Return the ACL list to use for the given path
	GetAclForPath(path string) []zk.ACL
}
