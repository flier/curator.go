package curator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockCloseable struct {
	mock.Mock

	crash bool
}

func (c *MockCloseable) Close() error {
	if c.crash {
		panic(errors.New("panic"))
	}

	return c.Called().Error(0)
}

func TestCloseQuietly(t *testing.T) {
	// No Error
	c := &MockCloseable{}

	c.On("Close").Return(nil).Once()

	assert.Nil(t, CloseQuietly(c))

	c.AssertExpectations(t)

	// Return Error
	c = &MockCloseable{}

	c.On("Close").Return(errors.New("test")).Once()

	assert.EqualError(t, CloseQuietly(c), "test")

	c.AssertExpectations(t)

	// Panic
	c = &MockCloseable{crash: true}

	assert.EqualError(t, CloseQuietly(c), "panic")

	c.AssertNotCalled(t, "Close")
}
