package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

const (
	configPath    = "./test_files/config.json"
	catalogPath   = "./test_files/configured_catalog.json"
	inputDataPath = "./test_files/input_data.txt"
)

func TestDestination_Spec(t *testing.T) {
	c := require.New(t)
	d := NewMockDestination(airbyte.NewLogger(bytes.NewBufferString("")))

	spec := d.Spec()
	c.Equal("https://propeldata.com/docs", spec.DocumentationURL)
	c.Equal([]airbyte.DestinationSyncMode{"overwrite", "append", "append_dedup"}, spec.SupportedDestinationSyncModes)
}

func TestDestination_Check(t *testing.T) {
	tests := []struct {
		name            string
		configPath      string
		expectedStatus  airbyte.CheckStatus
		expectedMessage string
		expectedLogs    string
		mockError       string
	}{
		{
			name:            "Invalid config path",
			configPath:      "invalid/config/path",
			expectedStatus:  airbyte.CheckStatusFailed,
			expectedMessage: "Configuration for Propel is invalid.",
			expectedLogs:    `"level":"ERROR","message":"Configuration is invalid: open invalid/config/path: no such file or directory"`,
		},
		{
			name:           "Invalid Access token",
			configPath:     configPath,
			expectedStatus: airbyte.CheckStatusFailed,
			mockError:      "invalid oauth token",
			expectedLogs:   `"level":"ERROR","message":"Access token request failed:`,
		},
		{
			name:            "Successful check",
			configPath:      configPath,
			expectedStatus:  airbyte.CheckStatusSuccess,
			expectedMessage: "Successfully generated a Propel access token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			a := assert.New(st)

			if tt.mockError != "" {
				mockOAuthError = errors.New(tt.mockError)
			}

			st.Cleanup(func() { mockOAuthError = nil })

			stdoutBuffer := bytes.NewBufferString("")
			d := NewMockDestination(airbyte.NewLogger(stdoutBuffer))
			check := d.Check(tt.configPath)

			logsOutput := stdoutBuffer.String()
			a.Contains(logsOutput, `"level":"DEBUG","message":"Validating API connection"`)
			a.Contains(logsOutput, tt.expectedLogs)

			a.Equal(tt.expectedStatus, check.Status)

			if tt.mockError != "" {
				a.Equal(fmt.Sprintf("Generating a Propel access token failed: %s", tt.mockError), check.Message)
			}

			a.Contains(check.Message, tt.expectedMessage)
		})
	}
}

func TestDestination_Write(t *testing.T) {
	tests := []struct {
		name                string
		configPath          string
		catalogPath         string
		inputDataPath       string
		expectedLogs        []string
		maxBytesPerBatch    int
		maxRecordsBatchSize int
		mockOAuthError      error
		mockWebhookError    error
		mockApiError        error
		expectedError       string
	}{
		{
			name:          "Invalid config path",
			configPath:    "invalid/config/path",
			catalogPath:   catalogPath,
			inputDataPath: inputDataPath,
			expectedLogs:  []string{`"level":"ERROR","message":"Configuration is invalid: open invalid/config/path: no such file or directory"`},
			expectedError: "configuration for Propel is invalid. Unable to read connector configuration",
		},
		{
			name:          "Invalid configured catalog path",
			configPath:    configPath,
			catalogPath:   "invalid/catalog/path",
			inputDataPath: inputDataPath,
			expectedLogs:  []string{`"level":"ERROR","message":"Configured catalog is invalid: open invalid/catalog/path: no such file or directory"`},
			expectedError: "configured catalog is invalid. Unable to parse it",
		},
		{
			name:           "Invalid Access token",
			configPath:     configPath,
			catalogPath:    catalogPath,
			inputDataPath:  inputDataPath,
			mockOAuthError: errors.New("mock OAuth error"),
			expectedLogs:   []string{`"level":"ERROR","message":"Access token request failed:`},
			expectedError:  "generating a Propel access token failed",
		},
		{
			name:          "Fetch Data Source failure",
			configPath:    configPath,
			catalogPath:   catalogPath,
			inputDataPath: inputDataPath,
			mockApiError:  errors.New("mock API error"),
			expectedLogs:  []string{`"level":"ERROR","message":"Fetch Data Source \"_airlines\" failed:`},
			expectedError: "failed to get Data Source",
		},

		{
			name:             "Webhook post events failure",
			configPath:       configPath,
			catalogPath:      catalogPath,
			inputDataPath:    inputDataPath,
			mockWebhookError: errors.New("mock webhook error"),
			expectedLogs: []string{
				"failed to publish 2 events to url:",
			},
			expectedError: "publish batch failed after state",
		},
		{
			name:                "Successful write - batch per number of records",
			configPath:          configPath,
			catalogPath:         catalogPath,
			inputDataPath:       inputDataPath,
			maxRecordsBatchSize: 10,
			maxBytesPerBatch:    maxBytesPerBatch,
			expectedLogs: []string{
				"airlines state 1",
				"airlines state 2",
				"airlines state 3",
				"Max batch size reached",
				`Deletion Job \"DPJ1234567890\" succeeded`,
			},
		},
		{
			name:                "Successful write - batch per number of bytes",
			configPath:          configPath,
			catalogPath:         catalogPath,
			inputDataPath:       inputDataPath,
			maxRecordsBatchSize: maxRecordsBatchSize,
			maxBytesPerBatch:    2_500,
			expectedLogs: []string{
				"airlines state 1",
				"airlines state 2",
				"airlines state 3",
				"Max batch size reached",
				`Deletion Job \"DPJ1234567890\" succeeded`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			a := assert.New(st)

			if tt.maxBytesPerBatch > 0 {
				maxBytesPerBatch = tt.maxBytesPerBatch
			}

			if tt.maxRecordsBatchSize > 0 {
				maxRecordsBatchSize = tt.maxRecordsBatchSize
			}

			mockOAuthError, mockWebhookError, mockApiError = tt.mockOAuthError, tt.mockWebhookError, tt.mockApiError
			st.Cleanup(func() { mockOAuthError, mockWebhookError, mockApiError = nil, nil, nil })

			stdoutBuffer := bytes.NewBufferString("")
			d := NewMockDestination(airbyte.NewLogger(stdoutBuffer))

			file, err := os.Open(tt.inputDataPath)
			a.NoError(err)

			defer file.Close()

			reader := io.Reader(file)

			err = d.Write(context.Background(), tt.configPath, tt.catalogPath, reader)
			if tt.expectedError == "" {
				a.NoError(err)
			} else {
				a.Contains(err.Error(), tt.expectedError)
			}

			logsOutput := stdoutBuffer.String()
			a.Contains(logsOutput, `"level":"DEBUG","message":"Write records"`)
			for _, log := range tt.expectedLogs {
				a.Contains(logsOutput, log)
			}
		})
	}
}

func TestGetAirbyteRawID(t *testing.T) {
	tests := []struct {
		name        string
		namespace   string
		streamName  string
		recordIndex int
		emittedAt   int64
		expectedID  string
	}{
		{
			name:        "No types",
			namespace:   "namespace",
			streamName:  "stream",
			recordIndex: 1,
			emittedAt:   int64(123456789),
			expectedID:  "64835b23-1e43-d091-c9b0-de411c0d4364",
		},
		{
			name:        "No types",
			namespace:   "namespace",
			streamName:  "stream",
			recordIndex: 2,
			emittedAt:   int64(123456789),
			expectedID:  "8b7e81a5-412e-3f3e-f045-bc0c440bdc02",
		},
		{
			name:        "No types",
			namespace:   "namespace",
			streamName:  "stream",
			recordIndex: 1,
			emittedAt:   int64(1323456789),
			expectedID:  "245b33d5-9c69-cdfb-ae06-d1b753d62f1c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			a := assert.New(st)

			id := getAirbyteRawID(tt.namespace, tt.streamName, tt.recordIndex, tt.emittedAt)
			a.Equal(tt.expectedID, id)
		})
	}
}
