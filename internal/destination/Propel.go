package destination

import (
	"context"
	"fmt"
	"io"

	"github.com/bitstrapped/airbyte"
	"github.com/propeldata/fivetran-destination/pkg/client"
)

type Propel struct {
	oauthClient   *client.OauthClient
	webhookClient *client.WebhookClient
}

var _ airbyte.Destination = &Propel{}

func NewPropel() *Propel {
	return &Propel{
		oauthClient:   client.NewOauthClient(),
		webhookClient: client.NewWebhookClient(),
	}
}

func (p *Propel) Spec(logTracker airbyte.LogTracker) (*airbyte.ConnectorSpecification, error) {
	if err := logTracker.Log(airbyte.LogLevelInfo, "Running spec"); err != nil {
		return nil, err
	}

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
			Required: []airbyte.PropertyName{"application_id", "application_secret"},
			Properties: airbyte.Properties{
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"application_id": {
						Description: "Propel Application ID",
						Examples:    []string{"APP00000000000000000000000000"},
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{
								airbyte.String,
							},
						},
					},
					"application_secret": {
						Description: "Propel Application secret",
						Examples:    []string{"APP00000000000000000000000000"},
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{
								airbyte.String,
							},
						},
						IsSecret: true,
					},
				},
			},
		},
	}, nil
}

func (p *Propel) Check(dstCfgPath string, logTracker airbyte.LogTracker) error {
	if err := logTracker.Log(airbyte.LogLevelInfo, "Validating API connection"); err != nil {
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

func (p *Propel) Write(dstCfgPath string, configuredCat *airbyte.ConfiguredCatalog, input io.Reader, logTracker airbyte.LogTracker) error {
	return nil
}
