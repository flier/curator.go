package curator

import (
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTransaction(t *testing.T) {
	newMockContainer().WithNamespace("parent").Test(t, func(client CuratorFramework, conn *mockConn, compress *mockCompressionProvider) {
		acls := zk.AuthACL(zk.PermRead)

		compress.On("Compress", "/node1", []byte("default")).Return([]byte("compressed(default)"), nil).Once()
		compress.On("Compress", "/node3", []byte("data")).Return([]byte("compressed(data)"), nil).Once()

		conn.On("Exists", "/parent").Return(true, nil, nil).Once()
		conn.On("Multi", mock.Anything).Return([]zk.MultiResponse{
			{Stat: nil, String: "/parent/node1"},
			{Stat: nil, String: ""},
			{Stat: &zk.Stat{}, String: ""},
			{Stat: nil, String: ""},
		}, nil).Once()

		results, err := client.InTransaction().
			Create().WithMode(PERSISTENT_SEQUENTIAL).WithACL(acls...).Compressed().ForPath("/node1").
			Delete().WithVersion(3).ForPath("/node2").
			SetData().WithVersion(5).Compressed().ForPathWithData("/node3", []byte("data")).
			Check().WithVersion(7).ForPath("/node4").
			Commit()

		assert.NoError(t, err)
		assert.Equal(t, conn.operations, []interface{}{
			&zk.CreateRequest{
				Path:  "/parent/node1",
				Data:  []byte("compressed(default)"),
				Acl:   acls,
				Flags: int32(PERSISTENT_SEQUENTIAL),
			},
			&zk.DeleteRequest{
				Path:    "/parent/node2",
				Version: int32(3),
			},
			&zk.SetDataRequest{
				Path:    "/parent/node3",
				Data:    []byte("compressed(data)"),
				Version: int32(5),
			},
			&zk.CheckVersionRequest{
				Path:    "/parent/node4",
				Version: int32(7),
			},
		})
		assert.Equal(t, results, []TransactionResult{
			{
				Type:       OP_CREATE,
				ForPath:    "/parent/node1",
				ResultPath: "/node1",
			},
			{
				Type:    OP_DELETE,
				ForPath: "/parent/node2",
			},
			{
				Type:       OP_SET_DATA,
				ForPath:    "/parent/node3",
				ResultStat: &zk.Stat{},
			},
			{
				Type:    OP_CHECK,
				ForPath: "/parent/node4",
			},
		})
	})
}
