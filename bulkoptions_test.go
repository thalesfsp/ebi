package ebi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBulkOptions(t *testing.T) {
	got, err := NewBulkOptions[*TestModel](
		"index",
		json.RawMessage(`{"id": "1", "group": 1}`),
	)

	assert.NoError(t, err)
	assert.NotNil(t, got)
}
