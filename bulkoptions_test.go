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

// TestNewBulkOptions_EmptySampleDoc_ReturnsError pins a regression that hit
// the UVS project (proj-ringboost-vendor) on 2026-04-22. A caller passed
// json.RawMessage("") as the sampleDoc; ebi happily constructed BulkOptions
// and only panicked later inside BulkCreate at
// `targetBatchSizeBytes / docSize` in ebi.go (docSize = 0). The panic
// surfaced as `runtime error: integer divide by zero`, was caught by the
// caller's goroutine recover, and turned into a Failed task with no
// surfaced error message.
//
// NewBulkOptions now fails fast with ErrSampleDocRequired so callers get a
// clear, named error at the construction site.
func TestNewBulkOptions_EmptySampleDoc_ReturnsError(t *testing.T) {
	cases := []struct {
		name      string
		sampleDoc json.RawMessage
	}{
		{"nil RawMessage", nil},
		{"empty bytes RawMessage", json.RawMessage{}},
		{"empty string cast to RawMessage", json.RawMessage("")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewBulkOptions[*TestModel]("index", tc.sampleDoc)

			assert.Error(t, err, "empty sampleDoc must return an error, not panic later inside BulkCreate")
			assert.Nil(t, got)
			assert.Contains(t, err.Error(), "sample document",
				"error should reference the missing input by name so callers can self-diagnose")
		})
	}
}

// TestNewBulkOptions_NonEmptySampleDoc_NoError keeps the happy path covered
// alongside the new validation.
func TestNewBulkOptions_NonEmptySampleDoc_NoError(t *testing.T) {
	got, err := NewBulkOptions[*TestModel](
		"index",
		json.RawMessage(`{"id":"x"}`),
	)

	assert.NoError(t, err)
	assert.NotNil(t, got)
}
