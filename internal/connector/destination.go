package connector

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/propeldata/go-client"
	"github.com/propeldata/go-client/models"
	"github.com/sethvargo/go-password/password"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

const (
	airbyteExtractedAtColumn = "_airbyte_extracted_at"
	airbyteRawIdColumn       = "_airbyte_raw_id"
)

var (
	maxRecordsBatchSize   = 300
	defaultAirbyteColumns = []*models.WebhookDataSourceColumnInput{
		{
			Name:         airbyteRawIdColumn,
			Type:         models.StringPropelType,
			Nullable:     false,
			JsonProperty: airbyteRawIdColumn,
		},
		{
			Name:         airbyteExtractedAtColumn,
			Type:         models.TimestampPropelType,
			Nullable:     false,
			JsonProperty: airbyteExtractedAtColumn,
		},
	}
)

type PropelOAuthClient interface {
	OAuthToken(ctx context.Context, applicationID, applicationSecret string) (*client.OAuthToken, error)
}

type PropelWebhookClient interface {
	PostEvents(ctx context.Context, input *client.PostEventsInput) ([]error, error)
}

type PropelApiClient interface {
	CreateDataSource(ctx context.Context, opts client.CreateDataSourceOpts) (*models.DataSource, error)
	FetchDataSource(ctx context.Context, uniqueName string) (*models.DataSource, error)
}

type Destination struct {
	logger        airbyte.Logger
	oauthClient   PropelOAuthClient
	webhookClient PropelWebhookClient
}

func NewDestination(logger airbyte.Logger) *Destination {
	return &Destination{
		logger:        logger,
		oauthClient:   client.NewOauthClient(),
		webhookClient: client.NewWebhookClient(),
	}
}

func newApiClient(accessToken string) PropelApiClient {
	if accessToken == mockAccessToken {
		return NewMockApiClient(accessToken)
	}

	return client.NewApiClient(accessToken)
}

func (d *Destination) Spec() *airbyte.ConnectorSpecification {
	d.logger.Log(airbyte.LogLevelDebug, "Running spec")

	return &airbyte.ConnectorSpecification{
		DocumentationURL:      "https://propeldata.com/docs",
		ChangeLogURL:          "https://propeldata.com/docs",
		SupportsIncremental:   true,
		SupportsNormalization: false,
		SupportsDBT:           false,
		SupportedDestinationSyncModes: []airbyte.DestinationSyncMode{
			airbyte.DestinationSyncModeOverwrite,
			airbyte.DestinationSyncModeAppend,
		},
		ConnectionSpecification: airbyte.ConnectionSpecification{
			Title:    "Propel Destination Spec",
			Type:     "object",
			Required: []string{"application_id", "application_secret"},
			Properties: airbyte.Properties{
				Properties: map[string]airbyte.PropertySpec{
					"application_id": {
						Title:       "Application ID",
						Description: "Propel Application ID",
						Examples:    []string{"APP00000000000000000000000000"},
						PropertyType: airbyte.PropertyType{
							Type: airbyte.String,
						},
					},
					"application_secret": {
						Title:       "Application secret",
						Description: "Propel Application secret",
						PropertyType: airbyte.PropertyType{
							Type: airbyte.String,
						},
						IsSecret: true,
					},
				},
			},
		},
	}
}

func (d *Destination) Check(dstCfgPath string) *airbyte.ConnectionStatus {
	d.logger.Log(airbyte.LogLevelDebug, "Validating API connection")

	var dstCfg Config
	if err := UnmarshalFromPath(dstCfgPath, &dstCfg); err != nil {
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Configuration is invalid: %v", err))
		return &airbyte.ConnectionStatus{
			Status:  airbyte.CheckStatusFailed,
			Message: fmt.Sprintf("Configuration for Propel is invalid. Unable to read connector configuration: %v", err),
		}
	}

	_, err := d.oauthClient.OAuthToken(context.Background(), dstCfg.ApplicationID, dstCfg.ApplicationSecret)
	if err != nil {
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("OAuth token request failed: %v", err))
		return &airbyte.ConnectionStatus{
			Status:  airbyte.CheckStatusFailed,
			Message: fmt.Sprintf("Generating a Propel access token failed: %v", err),
		}
	}

	return &airbyte.ConnectionStatus{
		Status:  airbyte.CheckStatusSuccess,
		Message: "Successfully generated a Propel access token",
	}
}

func (d *Destination) Write(ctx context.Context, dstCfgPath string, cfgCatalogPath string, input io.Reader) error {
	d.logger.Log(airbyte.LogLevelDebug, "Write records")

	var dstCfg Config
	if err := UnmarshalFromPath(dstCfgPath, &dstCfg); err != nil {
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Configuration is invalid: %v", err))
		return fmt.Errorf("configuration for Propel is invalid. Unable to read connector configuration: %w", err)
	}

	var configuredCatalog airbyte.ConfiguredCatalog
	if err := UnmarshalFromPath(cfgCatalogPath, &configuredCatalog); err != nil {
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Configured catalog is invalid: %v", err))
		return fmt.Errorf("configured catalog is invalid. Unable to parse it %w", err)
	}

	oauthToken, err := d.oauthClient.OAuthToken(context.Background(), dstCfg.ApplicationID, dstCfg.ApplicationSecret)
	if err != nil {
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("OAuth token request failed: %v", err))
		return fmt.Errorf("generating a Propel access token failed: %w", err)
	}

	apiClient := newApiClient(oauthToken.AccessToken)

	for _, configuredStream := range configuredCatalog.Streams {
		dataSourceUniqueName := fmt.Sprintf("%s_%s", configuredStream.Stream.Namespace, configuredStream.Stream.Name)

		dataSource, err := apiClient.FetchDataSource(ctx, dataSourceUniqueName)
		if err != nil {
			if !client.NotFoundError("Data Source", err) {
				d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Fetch Data Source %q failed: %v", dataSourceUniqueName, err))
				return fmt.Errorf("failed to get Data Source: %w", err)
			}

			// Generates a password of 18 chars length with 2 digits, 2 symbols and uppercase letters.
			authPassword, err := password.Generate(18, 2, 2, false, false)
			if err != nil {
				d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Password generation failed: %v", err))
				return fmt.Errorf("failed to generate Basic auth password for Data Source %q: %w", dataSourceUniqueName, err)
			}

			columns := make([]*models.WebhookDataSourceColumnInput, 0, len(configuredStream.Stream.JSONSchema.Properties)+len(defaultAirbyteColumns))

			for propertyName, propertySpec := range configuredStream.Stream.JSONSchema.Properties {
				columnType, err := ConvertAirbyteTypeToPropelType(propertySpec.PropertyType)
				if err != nil {
					d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Airbyte to Propel data type conversion failed for Data Source %q: %v", dataSourceUniqueName, err))
					return fmt.Errorf("failed to convert Airbyte to Propel data type: %w", err)
				}

				columns = append(columns, &models.WebhookDataSourceColumnInput{
					Name:         propertyName,
					Type:         columnType,
					Nullable:     true,
					JsonProperty: propertyName,
				})
			}

			columns = append(columns, defaultAirbyteColumns...)

			dataSource, err = apiClient.CreateDataSource(ctx, client.CreateDataSourceOpts{
				Name: dataSourceUniqueName,
				BasicAuth: &models.HttpBasicAuthInput{
					Username: configuredStream.Stream.Namespace,
					Password: authPassword,
				},
				Columns:   columns,
				Timestamp: airbyteExtractedAtColumn,
				UniqueID:  airbyteRawIdColumn,
			})
			if err != nil {
				d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Data Source creation failed: %v", err))
				return fmt.Errorf("failed to create Data Source %q: %w", dataSourceUniqueName, err)
			}

			waitForStateOps := client.StateChangeOps[models.DataSource]{
				Pending: []string{"CREATED", "CONNECTING"},
				Target:  []string{"CONNECTED"},
				Refresh: func() (*models.DataSource, string, error) {
					resp, err := apiClient.FetchDataSource(ctx, dataSourceUniqueName)
					if err != nil {
						return nil, "", fmt.Errorf("failed to check the status of table %q: %w", dataSourceUniqueName, err)
					}

					return resp, resp.Status, nil
				},
				Timeout: 3 * time.Minute,
				Delay:   3 * time.Second,
			}

			if _, err = client.WaitForState(waitForStateOps); err != nil {
				d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Failed status transition of Data Source %q: %v", dataSourceUniqueName, err))
				return err
			}
		}

		d.logger.Log(airbyte.LogLevelDebug, fmt.Sprintf("Reading data for Data Source %q", dataSource.ID))

		batchedRecords := make([]map[string]any, 0, maxRecordsBatchSize)
		eventsInput := &client.PostEventsInput{
			WebhookURL:   dataSource.ConnectionSettings.WebhookConnectionSettings.WebhookURL,
			AuthUsername: dataSource.ConnectionSettings.WebhookConnectionSettings.BasicAuth.Username,
			AuthPassword: dataSource.ConnectionSettings.WebhookConnectionSettings.BasicAuth.Password,
		}

		scanner := bufio.NewScanner(input)
		for scanner.Scan() {
			var airbyteMessage airbyte.Message
			if err := json.Unmarshal(scanner.Bytes(), &airbyteMessage); err != nil {
				d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Failed to parse record: %v", err))
				return fmt.Errorf("failed to parse record for Data Source %q: %w", dataSource.ID, err)
			}

			switch airbyteMessage.Type {
			case airbyte.MessageTypeState:
				if err = d.publishBatch(ctx, dataSource, configuredStream, eventsInput, batchedRecords); err != nil {
					return fmt.Errorf("publish batch failed after state message for Data Source %q: %w", dataSource.ID, err)
				}

				batchedRecords = batchedRecords[:0]
				d.logger.State(airbyteMessage.State)

			case airbyte.MessageTypeRecord:
				recordMap := airbyteMessage.Record.Data
				recordMap[airbyteRawIdColumn] = uuid.New().String()
				recordMap[airbyteExtractedAtColumn] = airbyteMessage.Record.EmittedAt

				if len(batchedRecords) == maxRecordsBatchSize {
					d.logger.Log(airbyte.LogLevelDebug, fmt.Sprintf("Max batch size reached for Data Source %q", dataSource.ID))
					if err = d.publishBatch(ctx, dataSource, configuredStream, eventsInput, batchedRecords); err != nil {
						return fmt.Errorf("publish batch failed after max batch size was reached for Data Source %q: %w", dataSource.ID, err)
					}

					batchedRecords = batchedRecords[:0]
				}

				batchedRecords = append(batchedRecords, recordMap)
			}
		}

		if err = d.publishBatch(ctx, dataSource, configuredStream, eventsInput, batchedRecords); err != nil {
			return fmt.Errorf("publish batch failed for remaining records in Data Source %q: %w", dataSource.ID, err)
		}
	}

	return nil
}

func (d *Destination) publishBatch(ctx context.Context, dataSource *models.DataSource, configuredStream airbyte.ConfiguredStream, eventsInput *client.PostEventsInput, events []map[string]any) error {
	if len(events) == 0 {
		return nil
	}

	eventsInput.Events = events

	eventErrors, err := d.webhookClient.PostEvents(ctx, eventsInput)
	if err != nil {
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("failed to publish %d events to %s: %v", len(events), dataSource.ConnectionSettings.WebhookConnectionSettings.WebhookURL, err))
		return err
	}

	for _, eventError := range eventErrors {
		if eventError != nil {
			d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("failed to store stream %s: %v", configuredStream.Stream.Name, eventError))
		}
	}

	return nil
}
