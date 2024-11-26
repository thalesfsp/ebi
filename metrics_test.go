package ebi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMetrics(t *testing.T) {
	got, err := NewMetrics()

	assert.NoError(t, err)
	assert.NotNil(t, got)
}
