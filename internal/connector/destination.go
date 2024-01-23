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
	maxRecordsBatchSize      = 300
)

var (
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

type Destination struct {
	logger        airbyte.Logger
	oauthClient   *client.OauthClient
	webhookClient *client.WebhookClient
}

func NewDestination(logger airbyte.Logger) *Destination {
	return &Destination{
		logger:        logger,
		oauthClient:   client.NewOauthClient(),
		webhookClient: client.NewWebhookClient(),
	}
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
		return &airbyte.ConnectionStatus{
			Status:  airbyte.CheckStatusFailed,
			Message: fmt.Sprintf("configuration for Propel is invalid. Unable to read connector configuration: %v", err),
		}
	}

	_, err := d.oauthClient.OAuthToken(context.Background(), dstCfg.ApplicationID, dstCfg.ApplicationSecret)
	if err != nil {
		return &airbyte.ConnectionStatus{
			Status:  airbyte.CheckStatusFailed,
			Message: fmt.Sprintf("Generate a Propel Access Token failed: %v", err),
		}
	}

	return &airbyte.ConnectionStatus{
		Status:  airbyte.CheckStatusSuccess,
		Message: "Successfully generated an OAuth token for Propel API",
	}
}

func (d *Destination) Write(ctx context.Context, dstCfgPath string, cfgCatalogPath string, input io.Reader) error {
	d.logger.Log(airbyte.LogLevelDebug, "Write records")

	var dstCfg Config
	if err := UnmarshalFromPath(dstCfgPath, &dstCfg); err != nil {
		return fmt.Errorf("configuration for Propel is invalid. Unable to read connector configuration: %w", err)
	}

	var configuredCatalog airbyte.ConfiguredCatalog
	if err := UnmarshalFromPath(cfgCatalogPath, &configuredCatalog); err != nil {
		return fmt.Errorf("configured catalog is invalid. Unable to parse it %w", err)
	}

	oauthToken, err := d.oauthClient.OAuthToken(context.Background(), dstCfg.ApplicationID, dstCfg.ApplicationSecret)
	if err != nil {
		return fmt.Errorf("generate an Access token for Propel API failed: %w", err)
	}

	apiClient := client.NewApiClient(oauthToken.AccessToken)

	for _, configuredStream := range configuredCatalog.Streams {
		dataSourceUniqueName := fmt.Sprintf("%s_%s", configuredStream.Stream.Namespace, configuredStream.Stream.Name)

		dataSource, err := apiClient.FetchDataSource(ctx, dataSourceUniqueName)
		if err != nil {
			if !client.NotFoundError("Data Source", err) {
				return fmt.Errorf("failed to get Data Source: %w", err)
			}

			// Generates a password of 18 chars length with 2 digits, 2 symbols and uppercase letters.
			authPassword, err := password.Generate(18, 2, 2, false, false)
			if err != nil {
				return fmt.Errorf("failed to generate Basic auth password for Data Source: %w", err)
			}

			columns := make([]*models.WebhookDataSourceColumnInput, 0, len(configuredStream.Stream.JSONSchema.Properties)+len(defaultAirbyteColumns))

			for propertyName, propertySpec := range configuredStream.Stream.JSONSchema.Properties {
				columnType, err := ConvertAirbyteTypeToPropelType(propertySpec.PropertyType)
				if err != nil {
					return fmt.Errorf("failed to generate Basic auth password for Data Source: %w", err)
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
				return fmt.Errorf("failed to create Data Source %q: %w", dataSourceUniqueName, err)
			}

			waitForStateOps := stateChangeOps[models.DataSource]{
				pending: []string{"CREATED", "CONNECTING"},
				target:  []string{"CONNECTED"},
				refresh: func() (*models.DataSource, string, error) {
					resp, err := apiClient.FetchDataSource(ctx, dataSourceUniqueName)
					if err != nil {
						return nil, "", fmt.Errorf("failed to check the status of table %q: %w", dataSourceUniqueName, err)
					}

					return resp, resp.Status, nil
				},
				timeout: 3 * time.Minute,
				delay:   3 * time.Second,
			}

			if _, err = waitForState(waitForStateOps); err != nil {
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
				return fmt.Errorf("failed to parse Record: %w", err)
			}

			switch airbyteMessage.Type {
			case airbyte.MessageTypeState:
				if err = d.publishBatch(ctx, dataSource, configuredStream, eventsInput, batchedRecords); err != nil {
					continue
				}

				batchedRecords = batchedRecords[:0]
				d.logger.State(airbyteMessage.State)

			case airbyte.MessageTypeRecord:
				recordMap := airbyteMessage.Record.Data
				recordMap[airbyteRawIdColumn] = uuid.New().String()
				recordMap[airbyteExtractedAtColumn] = airbyteMessage.Record.EmittedAt

				if len(batchedRecords) == maxRecordsBatchSize {
					if err = d.publishBatch(ctx, dataSource, configuredStream, eventsInput, batchedRecords); err != nil {
						return err
					}

					batchedRecords = batchedRecords[:0]
				}

				batchedRecords = append(batchedRecords, recordMap)
			}
		}

		if err = d.publishBatch(ctx, dataSource, configuredStream, eventsInput, batchedRecords); err != nil {
			return err
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
