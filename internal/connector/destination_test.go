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
	c.Equal([]airbyte.DestinationSyncMode{"overwrite", "append"}, spec.SupportedDestinationSyncModes)
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
			name:           "Invalid OAuth token",
			configPath:     configPath,
			expectedStatus: airbyte.CheckStatusFailed,
			mockError:      "invalid oauth token",
			expectedLogs:   `"level":"ERROR","message":"OAuth token request failed:`,
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
		name             string
		configPath       string
		catalogPath      string
		inputDataPath    string
		expectedLogs     []string
		mockOAuthError   error
		mockWebhookError error
		mockApiError     error
		expectedError    string
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
			name:           "Invalid OAuth token",
			configPath:     configPath,
			catalogPath:    catalogPath,
			inputDataPath:  inputDataPath,
			mockOAuthError: errors.New("mock OAuth error"),
			expectedLogs:   []string{`"level":"ERROR","message":"OAuth token request failed:`},
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
				"Reading data for Data Source",
				"failed to publish 2 events to url:",
			},
			expectedError: "publish batch failed after state",
		},
		{
			name:          "Successful write",
			configPath:    configPath,
			catalogPath:   catalogPath,
			inputDataPath: inputDataPath,
			expectedLogs: []string{
				"airlines state 1",
				"airlines state 2",
				"airlines state 3",
				"Max batch size reached",
				"Reading data for Data Source",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			a := assert.New(st)

			maxRecordsBatchSize = 10
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
