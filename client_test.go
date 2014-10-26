package riakpbc

import (
	"github.com/bmizerany/assert"
	"testing"
)

func clientTestSetupSingleNodeConnection(t *testing.T) (client *Client) {
	client, err := NewClient([]string{"127.0.0.1:8087"})
	assert.T(t, err == nil)

	return client
}
