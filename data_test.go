package curator

import (
	"sync"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type GetDataBuilderTestSuite struct {
	mockContainerTestSuite
}

func TestGetDataBuilder(t *testing.T) {
	suite.Run(t, new(GetDataBuilderTestSuite))
}

func (s *GetDataBuilderTestSuite) TestGetData() {
	s.With(func(builder *CuratorFrameworkBuilder, client CuratorFramework, conn *mockConn, compress *mockCompressionProvider) {
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
	s.WithNamespace("parent", func(client CuratorFramework, conn *mockConn) {
		conn.On("Exists", "/parent").Return(true, nil, nil).Once()
		conn.On("Get", "/parent/child").Return([]byte("data"), (*zk.Stat)(nil), nil).Once()

		data, err := client.GetData().ForPath("/child")

		assert.Equal(s.T(), "data", string(data))
		assert.NoError(s.T(), err)
	})
}

func (s *GetDataBuilderTestSuite) TestBackground() {
	s.WithNamespace("parent", func(client CuratorFramework, conn *mockConn, wg *sync.WaitGroup) {
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
	s.With(func(client CuratorFramework, conn *mockConn, wg *sync.WaitGroup) {
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
	mockContainerTestSuite
}

func TestSetDataBuilder(t *testing.T) {
	suite.Run(t, new(SetDataBuilderTestSuite))
}

func (s *SetDataBuilderTestSuite) TestSetData() {
	s.With(func(builder *CuratorFrameworkBuilder, client CuratorFramework, conn *mockConn, compress *mockCompressionProvider) {
		compress.On("Compress", "/node", []byte("data")).Return([]byte("compressed(data)"), nil).Once()
		conn.On("Set", "/node", []byte("compressed(data)"), int32(3)).Return(&zk.Stat{Version: 4}, nil).Once()

		stat, err := client.SetData().WithVersion(3).Compressed().ForPathWithData("/node", []byte("data"))

		assert.NotNil(s.T(), stat)
		assert.Equal(s.T(), 4, stat.Version)
		assert.NoError(s.T(), err)
	})
}

func (s *SetDataBuilderTestSuite) TestNamespace() {
	s.WithNamespace("parent", func(client CuratorFramework, conn *mockConn) {
		data := []byte("data")

		conn.On("Exists", "/parent").Return(true, nil, nil).Once()
		conn.On("Set", "/parent/child", data, int32(-1)).Return(&zk.Stat{Version: 5}, nil).Once()

		stat, err := client.SetData().ForPathWithData("/child", data)

		assert.NotNil(s.T(), stat)
		assert.Equal(s.T(), 5, stat.Version)
		assert.NoError(s.T(), err)
	})
}

func (s *SetDataBuilderTestSuite) TestBackground() {
	s.WithNamespace("parent", func(client CuratorFramework, conn *mockConn, wg *sync.WaitGroup) {
		data := []byte("data")
		ctxt := "context"

		conn.On("Exists", "/parent").Return(true, nil, nil).Once()
		conn.On("Set", "/parent/child", data, int32(-1)).Return((*zk.Stat)(nil), zk.ErrAPIError).Once()

		_, err := client.SetData().InBackgroundWithCallbackAndContext(
			func(client CuratorFramework, event CuratorEvent) error {
				defer wg.Done()

				assert.Equal(s.T(), SET_DATA, event.Type())
				assert.Equal(s.T(), "/child", event.Path())
				assert.Equal(s.T(), data, event.Data())
				assert.EqualError(s.T(), event.Err(), zk.ErrAPIError.Error())
				assert.Equal(s.T(), "child", event.Name())
				assert.Equal(s.T(), ctxt, event.Context())

				return nil
			}, ctxt).ForPathWithData("/child", data)

		assert.NoError(s.T(), err)
	})
}
