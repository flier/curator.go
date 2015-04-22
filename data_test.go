package curator

import (
	"sync"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type GetDataBuilderTestSuite struct {
	mockClientTestSuite
}

func TestGetDataBuilder(t *testing.T) {
	suite.Run(t, new(GetDataBuilderTestSuite))
}

func (s *GetDataBuilderTestSuite) TestGetData() {
	s.WithClient(func(builder *CuratorFrameworkBuilder, client CuratorFramework, conn *mockConn, compress *mockCompressionProvider) {
		conn.On("Get", "/node").Return([]byte("compressed(data)"), &zk.Stat{Version: 1}, nil).Once()
		compress.On("Decompress", "/node", []byte("compressed(data)")).Return([]byte("data"), nil).Once()

		var stat zk.Stat

		data, err := client.GetData().StoringStatIn(&stat).Decompressed().ForPath("/node")

		assert.Equal(s.T(), "data", string(data))
		assert.Equal(s.T(), 1, stat.Version)
		assert.NoError(s.T(), err)
	})
}

func (s *GetDataBuilderTestSuite) TestNamespace() {
	s.WithClientAndNamespace("parent", func(client CuratorFramework, conn *mockConn) {
		conn.On("Exists", "/parent").Return(true, nil, nil).Once()
		conn.On("Get", "/parent/child").Return([]byte("data"), (*zk.Stat)(nil), nil).Once()

		data, err := client.GetData().ForPath("/child")

		assert.Equal(s.T(), "data", string(data))
		assert.NoError(s.T(), err)
	})
}

func (s *GetDataBuilderTestSuite) TestBackground() {
	s.WithClientAndNamespace("parent", func(client CuratorFramework, conn *mockConn, wg *sync.WaitGroup) {
		data := []byte("data")
		ctxt := "context"

		conn.On("Exists", "/parent").Return(true, nil, nil).Once()
		conn.On("Get", "/parent/child").Return(data, (*zk.Stat)(nil), nil).Once()

		_, err := client.GetData().InBackgroundWithCallbackAndContext(
			func(client CuratorFramework, event CuratorEvent) error {
				defer wg.Done()

				assert.Equal(s.T(), GET_DATA, event.Type())
				assert.Equal(s.T(), "/child", event.Path())
				assert.Equal(s.T(), data, event.Data())
				assert.NoError(s.T(), event.Err())
				assert.Equal(s.T(), "child", event.Name())
				assert.Equal(s.T(), ctxt, event.Context())

				return nil
			}, ctxt).ForPath("/child")

		assert.NoError(s.T(), err)
	})
}

func (s *GetDataBuilderTestSuite) TestWatcher() {
	s.WithClient(func(client CuratorFramework, conn *mockConn, wg *sync.WaitGroup) {
		events := make(chan zk.Event)

		defer close(events)

		conn.On("GetW", "/node").Return([]byte("data"), (*zk.Stat)(nil), events, nil).Once()
		data, err := client.GetData().UsingWatcher(NewWatcher(func(event *zk.Event) {
			defer wg.Done()

			assert.NotNil(s.T(), event)
			assert.Equal(s.T(), zk.EventNodeDataChanged, event.Type)
			assert.Equal(s.T(), "/node", event.Path)

		})).ForPath("/node")

		assert.Equal(s.T(), "data", string(data))
		assert.NoError(s.T(), err)

		events <- zk.Event{
			Type: zk.EventNodeDataChanged,
			Path: "/node",
		}
	})
}

type SetDataBuilderTestSuite struct {
	mockClientTestSuite
}

func TestSetDataBuilder(t *testing.T) {
	suite.Run(t, new(SetDataBuilderTestSuite))
}
