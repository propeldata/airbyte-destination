package connector

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

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
	maxBytesPerBatch      = 1_047_000 // less than 1 MiB
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
	FetchDataPool(ctx context.Context, uniqueName string) (*models.DataPool, error)
	CreateDeletionJob(ctx context.Context, dataPoolId string, filters []models.FilterInput) (*models.Job, error)
	FetchDeletionJob(ctx context.Context, id string) (*models.Job, error)
	DeleteDataPool(ctx context.Context, uniqueName string) (string, error)
	DeleteDataSource(ctx context.Context, uniqueName string) (string, error)
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
	return &airbyte.ConnectorSpecification{
		DocumentationURL:      "https://propeldata.com/docs",
		ChangeLogURL:          "https://propeldata.com/docs",
		SupportsIncremental:   true,
		SupportsNormalization: false,
		SupportsDBT:           false,
		SupportedDestinationSyncModes: []airbyte.DestinationSyncMode{
			airbyte.DestinationSyncModeOverwrite,
			airbyte.DestinationSyncModeAppend,
			airbyte.DestinationSyncModeAppendDedup,
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
							TypeSet: &airbyte.PropTypes{
								Types: []airbyte.PropType{airbyte.String},
							},
						},
					},
					"application_secret": {
						Title:       "Application secret",
						Description: "Propel Application secret",
						PropertyType: airbyte.PropertyType{
							TypeSet: &airbyte.PropTypes{
								Types: []airbyte.PropType{airbyte.String},
							},
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
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Access token request failed: %v", err))
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
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Access token request failed: %v", err))
		return fmt.Errorf("generating a Propel access token failed: %w", err)
	}

	apiClient := newApiClient(oauthToken.AccessToken)
	dataSources := map[string]*models.DataSource{}
	isFullReset := true

	for _, configuredStream := range configuredCatalog.Streams {
		isFullReset = isFullReset && configuredStream.DestinationSyncMode == airbyte.DestinationSyncModeOverwrite
		dataSourceUniqueName := getDataSourceUniqueName(configuredStream.Stream.Namespace, configuredStream.Stream.Name)

		dataSource, fetchDataSourceErr := apiClient.FetchDataSource(ctx, dataSourceUniqueName)
		if fetchDataSourceErr != nil {
			if !client.NotFoundError("Data Source", fetchDataSourceErr) {
				d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Fetch Data Source %q failed: %v", dataSourceUniqueName, err))
				return fmt.Errorf("failed to get Data Source: %w", err)
			}

			dataSource, err = d.buildAndCreateDataSource(ctx, configuredStream, dataSourceUniqueName, apiClient)
			if err != nil {
				return err
			}
		} else if configuredStream.DestinationSyncMode == airbyte.DestinationSyncModeOverwrite {
			dataPool, err := apiClient.FetchDataPool(ctx, dataSourceUniqueName)
			if err != nil {
				d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Fetch Data Pool %q failed: %v", dataSourceUniqueName, err))
				return fmt.Errorf("failed to get Data Pool: %w", err)
			}

			deletionJob, err := apiClient.CreateDeletionJob(ctx, dataPool.ID, []models.FilterInput{{
				Column:   airbyteExtractedAtColumn,
				Operator: "LESS_THAN_OR_EQUAL_TO",
				Value:    ptr(time.Now().UTC().Format(time.RFC3339Nano)),
			}})
			if err != nil {
				d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Deletion Job creation failed: %v", err))
				return fmt.Errorf("failed to create Deletion Job for Data Pool %q: %w", dataPool.ID, err)
			}

			deletionJobUpdated, err := client.WaitForState(client.StateChangeOps[models.Job]{
				Pending: []string{"CREATED", "IN_PROGRESS"},
				Target:  []string{"SUCCEEDED", "FAILED"},
				Refresh: func() (*models.Job, string, error) {
					resp, err := apiClient.FetchDeletionJob(ctx, deletionJob.ID)
					if err != nil {
						d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Fetch Deletion Job %q failed: %v", deletionJob.ID, err))
						return nil, "", fmt.Errorf("failed to get Deletion Job: %w", err)
					}

					return resp, resp.Status, nil
				},
				Timeout: 20 * time.Minute,
				Delay:   3 * time.Second,
			})
			if err != nil {
				d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Deletion Job %q state transition failed: %v", deletionJob.ID, err))
				return fmt.Errorf("state transition for deletion job %q failed: %w", deletionJob.ID, err)
			}

			if deletionJobUpdated.Status == "FAILED" {
				d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Deletion Job %q failed", deletionJob.ID))
				return fmt.Errorf("deletion job %q failed", deletionJob.ID)
			}

			d.logger.Log(airbyte.LogLevelDebug, fmt.Sprintf("Deletion Job %q succeeded for Data Pool %q", deletionJob.ID, dataPool.ID))
		}

		dataSources[dataSourceUniqueName] = dataSource
		uniqueID := dataSource.ConnectionSettings.WebhookConnectionSettings.UniqueID

		if uniqueID == airbyteRawIdColumn && configuredStream.DestinationSyncMode == airbyte.DestinationSyncModeAppendDedup {
			d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Dedup destination sync mode is not compatible with Data Pool %q unique ID", dataSource.ID))
			return fmt.Errorf("append_dedup destination sync mode is not compatible with Data Pool %q unique ID", dataSource.ID)
		}

		if uniqueID != airbyteRawIdColumn && configuredStream.DestinationSyncMode == airbyte.DestinationSyncModeAppend {
			d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Append destination sync mode is not compatible with Data Pool %q ORDER BY statement", dataSource.ID))
			return fmt.Errorf("append destination sync mode is not compatible with Data Pool %q ORDER BY statement", dataSource.ID)
		}
	}

	recordsWritten, err := d.writeRecords(ctx, input, dataSources)
	if err != nil {
		return err
	}

	if isFullReset && recordsWritten == 0 {
		// Full reset syncs occur when a sync mode is changed for an existing stream in a connector.
		// Data Sources must then be removed, so they can later be created with the appropriate ORDER BY statement.
		// This type of syncs set all stream sync modes as "overwrite" and write no records.
		d.logger.Log(airbyte.LogLevelInfo, fmt.Sprintf("Full reset sync, all Data Pools will be deleted."))
		return deleteAllDataSources(ctx, apiClient, dataSources)
	}

	return nil
}

func (d *Destination) buildAndCreateDataSource(ctx context.Context, configuredStream airbyte.ConfiguredStream, dataSourceUniqueName string, apiClient PropelApiClient) (*models.DataSource, error) {
	// Generates a password of 18 chars length with 2 digits, 2 symbols and uppercase letters.
	authPassword, err := password.Generate(18, 2, 2, false, false)
	if err != nil {
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Password generation failed: %v", err))
		return nil, fmt.Errorf("failed to generate Basic auth password for Data Source %q: %w", dataSourceUniqueName, err)
	}

	orderByColumns := make([]string, 0, len(configuredStream.PrimaryKey))
	for _, pk := range configuredStream.PrimaryKey {
		if len(pk) != 1 {
			d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Unexpected primary key length %d for Data Source %q", len(pk), dataSourceUniqueName))
			return nil, fmt.Errorf("unexpected primary key length %d for Data Source %q", len(pk), dataSourceUniqueName)
		}

		orderByColumns = append(orderByColumns, pk[0])
	}

	var cursorField string
	if len(configuredStream.CursorField) > 0 {
		cursorField = configuredStream.CursorField[0]
	}

	columns := make([]*models.WebhookDataSourceColumnInput, 0, len(configuredStream.Stream.JSONSchema.Properties)+len(defaultAirbyteColumns))

	for propertyName, propertySpec := range configuredStream.Stream.JSONSchema.Properties {
		columnType, err := ConvertAirbyteTypeToPropelType(propertySpec.PropertyType)
		if err != nil {
			d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Airbyte to Propel data type conversion failed for Data Source %q: %v", dataSourceUniqueName, err))
			return nil, fmt.Errorf("failed to convert Airbyte to Propel data type: %w", err)
		}

		columns = append(columns, &models.WebhookDataSourceColumnInput{
			Name:         propertyName,
			Type:         columnType,
			Nullable:     !slices.Contains(orderByColumns, propertyName) && propertyName != cursorField,
			JsonProperty: propertyName,
		})
	}

	createDataSourceOpts := client.CreateDataSourceOpts{
		Name: dataSourceUniqueName,
		BasicAuth: &models.HttpBasicAuthInput{
			Username: configuredStream.Stream.Namespace,
			Password: authPassword,
		},
		Columns: append(columns, defaultAirbyteColumns...),
	}

	if len(orderByColumns) == 0 && configuredStream.DestinationSyncMode == airbyte.DestinationSyncModeAppendDedup {
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Append Dedup sync mode requires at least 1 primary key column"))
		return nil, fmt.Errorf("no primary keys were found for Data Source %q", dataSourceUniqueName)
	}

	if configuredStream.DestinationSyncMode == airbyte.DestinationSyncModeAppend || cursorField == "" || len(orderByColumns) == 0 {
		// Create Data Source that allows duplicates
		createDataSourceOpts.Timestamp = ptr(airbyteExtractedAtColumn)
		createDataSourceOpts.UniqueID = ptr(airbyteRawIdColumn)

		return d.createDataSource(ctx, apiClient, createDataSourceOpts)
	}

	// Create de-duplicating Data Source by ORDER BY and ver columns
	createDataSourceOpts.UniqueID = ptr(orderByColumns[0])
	createDataSourceOpts.TableSettings = &models.TableSettingsInput{
		PrimaryKey:  []string{}, // these must be explicitly empty
		PartitionBy: []string{},
		OrderBy:     orderByColumns,
		Engine: &models.TableEngineInput{
			ReplacingMergeTree: &models.ReplacingMergeTree{
				Type: models.TableEngineReplacingMergeTree,
				Ver:  cursorField,
			},
		},
	}

	return d.createDataSource(ctx, apiClient, createDataSourceOpts)
}

func (d *Destination) createDataSource(ctx context.Context, apiClient PropelApiClient, createDataSourceOpts client.CreateDataSourceOpts) (*models.DataSource, error) {
	dataSource, err := apiClient.CreateDataSource(ctx, createDataSourceOpts)
	if err != nil {
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Data Source creation failed: %v", err))
		return nil, fmt.Errorf("failed to create Data Source %q: %w", createDataSourceOpts.Name, err)
	}

	waitForStateOps := client.StateChangeOps[models.DataSource]{
		Pending: []string{"CREATED", "CONNECTING"},
		Target:  []string{"CONNECTED"},
		Refresh: func() (*models.DataSource, string, error) {
			resp, err := apiClient.FetchDataSource(ctx, createDataSourceOpts.Name)
			if err != nil {
				return nil, "", fmt.Errorf("failed to check the status of table %q: %w", createDataSourceOpts.Name, err)
			}

			return resp, resp.Status, nil
		},
		Timeout: 3 * time.Minute,
		Delay:   3 * time.Second,
	}

	if _, err = client.WaitForState(waitForStateOps); err != nil {
		d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Failed status transition of Data Source %q: %v", createDataSourceOpts.Name, err))
		return nil, err
	}

	return dataSource, nil
}

func (d *Destination) writeRecords(ctx context.Context, input io.Reader, dataSources map[string]*models.DataSource) (int, error) {
	batchByteSizePerDataSource := make(map[string]int, len(dataSources))

	batchedRecordsPerDataSource := make(map[string][]map[string]any)
	for dataSourceName := range dataSources {
		batchedRecordsPerDataSource[dataSourceName] = make([]map[string]any, 0)
		batchByteSizePerDataSource[dataSourceName] = 0
	}

	recordIndex := 0
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		var airbyteMessage airbyte.Message
		if err := json.Unmarshal(scanner.Bytes(), &airbyteMessage); err != nil {
			d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("Failed to parse record: %v", err))
			return recordIndex, fmt.Errorf("failed to parse record: %w", err)
		}

		switch airbyteMessage.Type {
		case airbyte.MessageTypeState:
			for dataSourceName, dataSource := range dataSources {
				eventsInput := &client.PostEventsInput{
					WebhookURL:   dataSource.ConnectionSettings.WebhookConnectionSettings.WebhookURL,
					AuthUsername: dataSource.ConnectionSettings.WebhookConnectionSettings.BasicAuth.Username,
					AuthPassword: dataSource.ConnectionSettings.WebhookConnectionSettings.BasicAuth.Password,
				}
				if err := d.publishBatch(ctx, dataSource, eventsInput, batchedRecordsPerDataSource[dataSourceName]); err != nil {
					return recordIndex, fmt.Errorf("publish batch failed after state message for Data Source %q: %w", dataSource.ID, err)
				}

				batchedRecordsPerDataSource[dataSourceName] = batchedRecordsPerDataSource[dataSourceName][:0]
			}

			d.logger.State(airbyteMessage.State)
		case airbyte.MessageTypeRecord:
			recordMap := airbyteMessage.Record.Data
			recordMap[airbyteRawIdColumn] = getAirbyteRawID(airbyteMessage.Record.Namespace, airbyteMessage.Record.Stream, recordIndex, airbyteMessage.Record.EmittedAt)
			recordMap[airbyteExtractedAtColumn] = airbyteMessage.Record.EmittedAt

			dataSource := dataSources[getDataSourceUniqueName(airbyteMessage.Record.Namespace, airbyteMessage.Record.Stream)]

			recordJsonEncoded, err := json.Marshal(recordMap)
			if err != nil {
				return recordIndex, fmt.Errorf("failed to encode record for Data Source %q: %w", dataSource.ID, err)
			}

			recordJsonBytesSize := len(recordJsonEncoded) + 1

			if batchByteSizePerDataSource[dataSource.UniqueName]+recordJsonBytesSize > maxBytesPerBatch {
				eventsInput := &client.PostEventsInput{
					WebhookURL:   dataSource.ConnectionSettings.WebhookConnectionSettings.WebhookURL,
					AuthUsername: dataSource.ConnectionSettings.WebhookConnectionSettings.BasicAuth.Username,
					AuthPassword: dataSource.ConnectionSettings.WebhookConnectionSettings.BasicAuth.Password,
				}

				d.logger.Log(airbyte.LogLevelDebug, fmt.Sprintf("Max batch size reached for Data Source %q", dataSource.ID))
				if err := d.publishBatch(ctx, dataSource, eventsInput, batchedRecordsPerDataSource[dataSource.UniqueName]); err != nil {
					return recordIndex, fmt.Errorf("publish batch failed after max batch size was reached for Data Source %q: %w", dataSource.ID, err)
				}

				batchedRecordsPerDataSource[dataSource.UniqueName] = batchedRecordsPerDataSource[dataSource.UniqueName][:0]
				batchByteSizePerDataSource[dataSource.UniqueName] = 0
			}

			batchedRecordsPerDataSource[dataSource.UniqueName] = append(batchedRecordsPerDataSource[dataSource.UniqueName], recordMap)
			batchByteSizePerDataSource[dataSource.UniqueName] += recordJsonBytesSize
			recordIndex++
		}
	}

	for dataSourceName, dataSource := range dataSources {
		eventsInput := &client.PostEventsInput{
			WebhookURL:   dataSource.ConnectionSettings.WebhookConnectionSettings.WebhookURL,
			AuthUsername: dataSource.ConnectionSettings.WebhookConnectionSettings.BasicAuth.Username,
			AuthPassword: dataSource.ConnectionSettings.WebhookConnectionSettings.BasicAuth.Password,
		}

		if err := d.publishBatch(ctx, dataSource, eventsInput, batchedRecordsPerDataSource[dataSourceName]); err != nil {
			return recordIndex, fmt.Errorf("publish batch failed for remaining records in Data Source %q: %w", dataSource.ID, err)
		}
	}

	return recordIndex, nil
}

func (d *Destination) publishBatch(ctx context.Context, dataSource *models.DataSource, eventsInput *client.PostEventsInput, events []map[string]any) error {
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
			d.logger.Log(airbyte.LogLevelError, fmt.Sprintf("failed to store events in Data Pool %q: %v", dataSource.UniqueName, eventError))
		}
	}

	return nil
}

func getDataSourceUniqueName(namespace, streamName string) string {
	return fmt.Sprintf("%s_%s", namespace, streamName)
}

func getAirbyteRawID(namespace, streamName string, recordIndex int, emittedAt int64) string {
	hash := sha256.New()
	hash.Write([]byte(strings.Join([]string{namespace, streamName, strconv.Itoa(recordIndex), strconv.FormatInt(emittedAt, 10)}, "\000")))
	hashBytes := hash.Sum(nil)
	hexString := hex.EncodeToString(hashBytes)
	uuid := hexString[:8] + "-" + hexString[8:12] + "-" + hexString[12:16] + "-" + hexString[16:20] + "-" + hexString[20:32]

	return uuid
}

func ptr[T any](value T) *T {
	return &value
}

func deleteAllDataSources(ctx context.Context, apiClient PropelApiClient, dataSources map[string]*models.DataSource) error {
	for dataSourceName := range dataSources {
		if _, err := apiClient.DeleteDataPool(ctx, dataSourceName); err != nil {
			return fmt.Errorf("failed to delete Data Pool %q: %w", dataSourceName, err)
		}
	}

	for dataSourceName := range dataSources {
		if _, err := client.WaitForState(client.StateChangeOps[models.DataPool]{
			Pending: []string{"DELETING"},
			Target:  []string{"DELETED"},
			Refresh: func() (*models.DataPool, string, error) {
				resp, err := apiClient.FetchDataPool(ctx, dataSourceName)
				if err != nil {
					if client.NotFoundError("Data Pool", err) {
						return nil, "DELETED", nil
					}

					return nil, "", fmt.Errorf("failed to get Data Pool: %w", err)
				}

				return resp, resp.Status, nil
			},
			Timeout: 20 * time.Minute,
			Delay:   3 * time.Second,
		}); err != nil {
			return fmt.Errorf(`transition to "DELETED" failed for Data Pool %q: %w`, dataSourceName, err)
		}

		if _, err := apiClient.DeleteDataSource(ctx, "_comments"); err != nil {
			return fmt.Errorf("failed to delete Data Source %q: %w", dataSourceName, err)
		}
	}

	for dataSourceName := range dataSources {
		if _, err := client.WaitForState(client.StateChangeOps[models.DataSource]{
			Pending: []string{"DELETING"},
			Target:  []string{"DELETED"},
			Refresh: func() (*models.DataSource, string, error) {
				resp, err := apiClient.FetchDataSource(ctx, dataSourceName)
				if err != nil {
					if client.NotFoundError("Data Source", err) {
						return nil, "DELETED", nil
					}

					return nil, "", fmt.Errorf("failed to get Data Source: %w", err)
				}

				return resp, resp.Status, nil
			},
			Timeout: 20 * time.Minute,
			Delay:   3 * time.Second,
		}); err != nil {
			return fmt.Errorf(`transition to "DELETED" failed for Data Source %q: %w`, dataSourceName, err)
		}
	}

	return nil
}
