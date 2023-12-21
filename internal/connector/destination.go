package destination

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/propeldata/fivetran-destination/pkg/client"
	"github.com/sethvargo/go-password/password"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

const (
	airbyteExtractedAtColumn = "_airbyte_extracted_at"
	airbyteRawIdColumn       = "_airbyte_raw_id"
)

var (
	defaultAirbyteColumns = []*client.WebhookDataSourceColumnInput{
		{
			Name:         airbyteRawIdColumn,
			Type:         client.StringPropelType,
			Nullable:     false,
			JsonProperty: airbyteRawIdColumn,
		},
		{
			Name:         airbyteExtractedAtColumn,
			Type:         client.TimestampPropelType,
			Nullable:     false,
			JsonProperty: airbyteExtractedAtColumn,
		},
	}
)

type Propel struct {
	oauthClient   *client.OauthClient
	webhookClient *client.WebhookClient
}

func NewPropel() *Propel {
	return &Propel{
		oauthClient:   client.NewOauthClient(),
		webhookClient: client.NewWebhookClient(),
	}
}

func (p *Propel) Spec() *airbyte.ConnectorSpecification {
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
						Description: "Propel Application ID",
						Examples:    []string{"APP00000000000000000000000000"},
						PropertyType: airbyte.PropertyType{
							Type: airbyte.String,
						},
					},
					"application_secret": {
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

func (p *Propel) Check(dstCfgPath string, logTracker airbyte.LogTracker) error {
	if err := logTracker.Log(airbyte.LogLevelDebug, "Validating API connection"); err != nil {
		return err
	}

	var dstCfg Config
	if err := airbyte.UnmarshalFromPath(dstCfgPath, &dstCfg); err != nil {
		return fmt.Errorf("configuration for Propel is invalid, unable to read destination configuration: %w", err)
	}

	_, err := p.oauthClient.OAuthToken(context.Background(), dstCfg.ApplicationID, dstCfg.ApplicationSecret)
	if err != nil {
		return fmt.Errorf("generate an Access Token for Propel failed: %w", err)
	}

	return nil
}

func (p *Propel) Write(dstCfgPath string, configuredCat *airbyte.ConfiguredCatalog, input io.Reader, tracker airbyte.MessageTracker) error {
	ctx := context.Background()

	if err := tracker.Log(airbyte.LogLevelDebug, "Write records"); err != nil {
		return err
	}

	var dstCfg Config
	if err := airbyte.UnmarshalFromPath(dstCfgPath, &dstCfg); err != nil {
		return fmt.Errorf("configuration for Propel is invalid, unable to read destination configuration: %w", err)
	}

	oauthToken, err := p.oauthClient.OAuthToken(context.Background(), dstCfg.ApplicationID, dstCfg.ApplicationSecret)
	if err != nil {
		tracker.Log(airbyte.LogLevelError, fmt.Sprintf("Fetching token %s", err.Error()))
		return fmt.Errorf("generate an Access Token for Propel failed: %w", err)
	}

	apiClient := client.NewApiClient(oauthToken.AccessToken)

	for _, configuredStream := range configuredCat.Streams {
		dataSourceUniqueName := fmt.Sprintf("%s_%s", configuredStream.Stream.Namespace, configuredStream.Stream.Name)

		dataSource, err := apiClient.FetchDataSource(ctx, dataSourceUniqueName)
		if err != nil {
			tracker.Log(airbyte.LogLevelError, fmt.Sprintf("Fetching DS %s", err.Error()))
			if !client.NotFoundError("Data Source", err) {
				return err
			}

			// Generates a password of 18 chars length with 2 digits, 2 symbols and uppercase letters.
			authPassword, err := password.Generate(18, 2, 2, false, false)
			if err != nil {
				return err
			}

			columns := make([]*client.WebhookDataSourceColumnInput, 0, len(configuredStream.Stream.JSONSchema.Properties)+len(defaultAirbyteColumns))

			for name, propertySpec := range configuredStream.Stream.JSONSchema.Properties {
				columnType, err := ConvertAirbyteTypeToPropelType(propertySpec.PropertyType)
				if err != nil {
					return err
				}

				columns = append(columns, &client.WebhookDataSourceColumnInput{
					Name:         string(name),
					Type:         columnType,
					Nullable:     true,
					JsonProperty: string(name),
				})
			}

			columns = append(columns, defaultAirbyteColumns...)

			tracker.Log(airbyte.LogLevelDebug, fmt.Sprintf("Writing Data Source %s", dataSourceUniqueName))

			dataSource, err = apiClient.CreateDataSource(ctx, client.CreateDataSourceOpts{
				Name: dataSourceUniqueName,
				BasicAuth: &client.HttpBasicAuthInput{
					Username: configuredStream.Stream.Namespace,
					Password: authPassword,
				},
				Columns:   columns,
				Timestamp: airbyteExtractedAtColumn,
				UniqueID:  airbyteRawIdColumn,
			})

			if err != nil {
				tracker.Log(airbyte.LogLevelError, err.Error())
				return err
			}
		}

		tracker.Log(airbyte.LogLevelDebug, fmt.Sprintf("using Data Source %s", dataSource.ID))

		scanner := bufio.NewScanner(input)
		for scanner.Scan() {
			record := scanner.Text()

			tracker.Log(airbyte.LogLevelDebug, fmt.Sprintf("Record received %s", record))

			tracker.State(&LastSyncTime{
				Stream:    configuredStream.Stream.Name,
				Timestamp: time.Now().UnixMilli(),
				Data:      record,
			})
		}
	}

	return nil
}
