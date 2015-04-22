package curator

import (
	"sync"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CheckExistsBuilderTestSuite struct {
	mockClientTestSuite
}

func TestCheckExistsBuilder(t *testing.T) {
	suite.Run(t, new(CheckExistsBuilderTestSuite))
}

func (s *CheckExistsBuilderTestSuite) TestCheckExists() {
	s.WithClient(func(client CuratorFramework, conn *mockConn) {
		conn.On("Exists", "/node").Return(true, &zk.Stat{}, nil).Once()

		stat, err := client.CheckExists().ForPath("/node")

		assert.NotNil(s.T(), stat)
		assert.NoError(s.T(), err)
	})

	s.WithClient(func(client CuratorFramework, conn *mockConn) {
		conn.On("Exists", "/node").Return(false, nil, nil).Once()

		stat, err := client.CheckExists().ForPath("/node")

		assert.Nil(s.T(), stat)
		assert.EqualError(s.T(), err, zk.ErrNoNode.Error())
	})

	s.WithClient(func(client CuratorFramework, conn *mockConn) {
		conn.On("Exists", "/node").Return(false, nil, zk.ErrAPIError).Once()

		stat, err := client.CheckExists().ForPath("/node")

		assert.Nil(s.T(), stat)
		assert.EqualError(s.T(), err, zk.ErrAPIError.Error())
	})
}

func (s *CheckExistsBuilderTestSuite) TestNamespace() {
	s.WithClientAndNamespace("parent", func(client CuratorFramework, conn *mockConn) {
		conn.On("Exists", "/parent").Return(true, nil, nil).Once()
		conn.On("Exists", "/parent/child").Return(false, nil, nil).Once()

		stat, err := client.CheckExists().ForPath("/child")

		assert.Nil(s.T(), stat)
		assert.EqualError(s.T(), err, zk.ErrNoNode.Error())
	})
}

func (s *CheckExistsBuilderTestSuite) TestBackground() {
	s.WithClientAndNamespace("parent", func(client CuratorFramework, conn *mockConn, wg *sync.WaitGroup) {
		ctxt := "context"

		conn.On("Exists", "/parent").Return(true, nil, nil).Once()
		conn.On("Exists", "/parent/child").Return(true, &zk.Stat{}, nil).Once()

		stat, err := client.CheckExists().InBackgroundWithCallbackAndContext(
			func(client CuratorFramework, event CuratorEvent) error {
				defer wg.Done()

				assert.Equal(s.T(), EXISTS, event.Type())
				assert.Equal(s.T(), "/child", event.Path())
				assert.NotNil(s.T(), event.Stat())
				assert.NoError(s.T(), event.Err())
				assert.Equal(s.T(), "child", event.Name())
				assert.Equal(s.T(), ctxt, event.Context())

				return nil
			}, ctxt).ForPath("/child")

		assert.Nil(s.T(), stat)
		assert.NoError(s.T(), err)
	})
}

func (s *CheckExistsBuilderTestSuite) TestWatcher() {
	s.WithClient(func(client CuratorFramework, conn *mockConn, wg *sync.WaitGroup) {
		events := make(chan zk.Event)

		defer close(events)

		conn.On("ExistsW", "/node").Return(true, &zk.Stat{}, events, nil).Once()

		stat, err := client.CheckExists().UsingWatcher(NewWatcher(func(event *zk.Event) {
			defer wg.Done()

			assert.NotNil(s.T(), event)
			assert.Equal(s.T(), zk.EventNodeDeleted, event.Type)
			assert.Equal(s.T(), "/node", event.Path)

		})).ForPath("/node")

		assert.NotNil(s.T(), stat)
		assert.NoError(s.T(), err)

		events <- zk.Event{
			Type: zk.EventNodeDeleted,
			Path: "/node",
		}
	})
}
