package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/flier/curator.go"
)

// The action type; any of create/delete/setvalue.
type ZkActionType int

const (
	NONE   ZkActionType = iota
	CREATE              // creates <zknode> recussively
	DELETE              // deletes <zknode> recursively
	VALUE               // sets <value> to <zknode>
)

type ZkAction struct {
	Type     ZkActionType // action of this instance
	Key      string       // ZK node key
	NewValue string       // value to be set, if action is setvalue
	OldValue string       // existing value of the ZK node key
}

type ZkActions []*ZkAction

type ZkActionHandler interface {
	Handle(action *ZkAction) error
}

type ZkActionPrinter struct {
	Out *os.File
}

func (p *ZkActionPrinter) Handle(action *ZkAction) error {
	var buf bytes.Buffer

	switch action.Type {
	case CREATE:
		fmt.Fprintf(&buf, "CREATE- key: %s\n", action.Key)
	case DELETE:
		fmt.Fprintf(&buf, "DELETE- key: %s\n", action.Key)
	case VALUE:
		fmt.Fprintf(&buf, "VALUE- key: %s value: %s", action.Key, action.NewValue)

		if len(action.OldValue) > 0 {
			fmt.Fprintf(&buf, " old: %s", action.OldValue)
		}

		fmt.Fprintln(&buf)
	}

	fmt.Print(buf.String())

	return nil
}

type ZkActionExecutor struct{}

func (e *ZkActionExecutor) Handle(action *ZkAction) error {
	return nil
}

type ZkActionInteractiveExecutor struct{}

func (e *ZkActionInteractiveExecutor) Handle(action *ZkAction) error {
	return nil
}

type ZkBaseNode struct {
	Path     string   `xml:"-"`
	Children []ZkNode `xml:"zknode"`
}

func (n *ZkBaseNode) Len() int {
	count := 1

	for _, child := range n.Children {
		count += child.Len()
	}

	return count
}

type ZkNode struct {
	ZkBaseNode

	XMLName xml.Name `xml:"zknode"`
	Name    string   `xml:"name,attr,omitempty"`
	Value   string   `xml:"value,attr,omitempty"`
	Ignore  *bool    `xml:"ignore,attr,omitempty"`
}

type ZkNodeVisitFunc func(node *ZkNode, first, last bool, siblings []bool) bool

func (n *ZkNode) Visit(visitor ZkNodeVisitFunc, first, last bool, siblings []bool) bool {
	if !visitor(n, first, last, siblings) {
		return true
	}

	for i, child := range n.Children {
		last := i == len(n.Children)-1

		if !child.Visit(visitor, i == 0, last, append(siblings, !last)) {
			return false
		}
	}

	return true
}

type ZkRootNode struct {
	ZkBaseNode

	XMLName xml.Name `xml:"root"`
}

func (n *ZkRootNode) Visit(visitor ZkNodeVisitFunc, first, last bool, siblings []bool) bool {
	for i, child := range n.Children {
		last := i == len(n.Children)-1

		if !child.Visit(visitor, i == 0, last, append(siblings, !last)) {
			return false
		}
	}

	return true
}

type ZkTree interface {
	Dump(depth int) (string, error)
}

type ZkBaseTree struct {
	getRoot func() (*ZkRootNode, error)
}

func (t *ZkBaseTree) Dump(depth int) (string, error) {
	if root, err := t.getRoot(); err != nil {
		return "", fmt.Errorf("fail to get root, %s", err)
	} else {
		var buf bytes.Buffer

		root.Visit(func(node *ZkNode, first, last bool, siblings []bool) bool {
			level := len(siblings)

			if len(node.Name) == 0 {
				return true // skip root
			}

			if depth > 0 && level > depth {
				return false // skip depth
			}

			for _, sibling := range siblings[:level-1] {
				if sibling {
					fmt.Fprint(&buf, "|   ")
				} else {
					fmt.Fprint(&buf, "    ")
				}
			}

			if first || last {
				fmt.Fprintf(&buf, "+--[%s", node.Name)
			} else {
				fmt.Fprintf(&buf, "|--[%s", node.Name)
			}

			if len(node.Value) > 0 {
				fmt.Fprintf(&buf, " => %s", node.Value)
			}

			fmt.Fprintln(&buf, "]")

			return true
		}, true, true, nil)

		return buf.String(), nil
	}
}

func (t *ZkBaseTree) Xml() ([]byte, error) {
	if root, err := t.getRoot(); err != nil {
		return nil, err
	} else if data, err := xml.MarshalIndent(root, "", "  "); err != nil {
		return nil, err
	} else {
		return []byte(fmt.Sprintf("%s%s\n", xml.Header, string(data))), nil
	}
}

type ZkLiveTree struct {
	ZkBaseTree

	client curator.CuratorFramework
}

func NewZkTree(hosts []string, base string) (*ZkLiveTree, error) {
	client := curator.NewClient(hosts[0], curator.NewRetryNTimes(3, time.Second))

	if err := client.Start(); err != nil {
		return nil, err
	}

	if len(base) > 0 {
		if base[0] == '/' {
			base = base[1:]
		}

		client = client.UsingNamespace(base)
	}

	tree := &ZkLiveTree{client: client}

	tree.getRoot = tree.Root

	return tree, nil
}

// writes the in-memory ZK tree on to ZK server
func (t *ZkLiveTree) Write(tree ZkTree, force bool) error {
	return nil
}

// returns a list of actions after taking a diff of in-memory ZK tree and live ZK tree.
func (t *ZkLiveTree) Diff(tree ZkTree) (ZkActions, error) {
	return nil, nil
}

// performs create/delete/setvalue by executing a set of ZkActions on a live ZK tree.
func (t *ZkLiveTree) Execute(actions ZkActions, handler ZkActionHandler) error {
	return nil
}

func (t *ZkLiveTree) Node(znodePath string) (*ZkNode, error) {
	if data, err := t.client.GetData().ForPath(znodePath); err != nil {
		return nil, fmt.Errorf("fail to get data of node `%s`, %s", znodePath, err)
	} else if children, err := t.client.GetChildren().ForPath(znodePath); err != nil {
		return nil, fmt.Errorf("fail to get children of node `%s`, %s", znodePath, err)
	} else {
		var nodes []ZkNode

		for _, child := range children {
			if node, err := t.Node(path.Join(znodePath, child)); err != nil {
				return nil, err
			} else {
				nodes = append(nodes, *node)
			}
		}

		return &ZkNode{
			ZkBaseNode: ZkBaseNode{
				Path:     znodePath,
				Children: nodes,
			},
			Name:  path.Base(znodePath),
			Value: string(data),
		}, nil
	}
}

func (t *ZkLiveTree) Root() (*ZkRootNode, error) {
	if children, err := t.client.GetChildren().ForPath("/"); err != nil {
		return nil, fmt.Errorf("fail to get children of root, %s", err)
	} else {
		var nodes []ZkNode

		for _, child := range children {
			if node, err := t.Node(path.Join("/", child)); err != nil {
				return nil, err
			} else {
				nodes = append(nodes, *node)
			}
		}

		return &ZkRootNode{
			ZkBaseNode: ZkBaseNode{
				Path:     "/",
				Children: nodes,
			},
		}, nil
	}
}

type ZkLoadedTree struct {
	ZkBaseTree

	file *os.File
	root *ZkRootNode
}

func LoadZkTree(filename string) (*ZkLoadedTree, error) {
	if file, err := os.Open(filename); err != nil {
		return nil, fmt.Errorf("fail to open file `%s`, %s", filename, err)
	} else if data, err := ioutil.ReadFile(filename); err != nil {
		return nil, fmt.Errorf("fail to read file `%s`, %s", filename, err)
	} else {
		var root ZkRootNode

		if err := xml.Unmarshal(data, &root); err != nil {
			return nil, fmt.Errorf("fail to parse file `%s`, %s", filename, err)
		}

		return &ZkLoadedTree{
			ZkBaseTree: ZkBaseTree{
				getRoot: func() (*ZkRootNode, error) {
					return &root, nil
				},
			},
			file: file,
			root: &root,
		}, nil
	}
}

func (t *ZkLoadedTree) Execute(actions ZkActions, handler ZkActionHandler) error {
	return nil
}

func (t *ZkLoadedTree) Root() *ZkRootNode {
	return t.root
}

func (t *ZkLoadedTree) String() (string, error) {
	return t.Dump(-1)
}

func (t *ZkLoadedTree) Diff(tree ZkTree) (ZkActions, error) {
	return nil, nil
}
