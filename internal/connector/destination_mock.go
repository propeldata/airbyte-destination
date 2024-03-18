package connector

import (
	"context"

	"github.com/hasura/go-graphql-client"
	"github.com/propeldata/go-client"
	"github.com/propeldata/go-client/models"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

const mockAccessToken = "mockAccessToken"

var (
	mockOAuthError   error = nil
	mockWebhookError error = nil
	mockApiError     error = nil
	requestCounter   int   = 0
)

type MockOauthClient struct{}
type MockWebhookClient struct{}
type MockApiClient struct{}

func NewMockDestination(logger airbyte.Logger) *Destination {
	return &Destination{
		logger:        logger,
		oauthClient:   NewMockOAuthClient(),
		webhookClient: NewMockWebhookClient(),
	}
}

func NewMockOAuthClient() *MockOauthClient {
	return &MockOauthClient{}
}

var _ PropelOAuthClient = (*MockOauthClient)(nil)

func (oc *MockOauthClient) OAuthToken(_ context.Context, _ string, _ string) (*client.OAuthToken, error) {
	if mockOAuthError != nil {
		return &client.OAuthToken{}, mockOAuthError
	}

	return &client.OAuthToken{
		AccessToken: mockAccessToken,
		ExpiresIn:   10,
	}, nil
}

func NewMockWebhookClient() *MockWebhookClient {
	return &MockWebhookClient{}
}

var _ PropelWebhookClient = (*MockWebhookClient)(nil)

func (wc *MockWebhookClient) PostEvents(_ context.Context, _ *client.PostEventsInput) ([]error, error) {
	if mockWebhookError != nil {
		return []error{mockWebhookError}, mockWebhookError
	}

	return []error{}, nil
}

func NewMockApiClient(_ string) *MockApiClient {
	return &MockApiClient{}
}

var _ PropelApiClient = (*MockApiClient)(nil)

func (ac *MockApiClient) CreateDataSource(_ context.Context, opts client.CreateDataSourceOpts) (*models.DataSource, error) {
	columns := make([]models.WebhookColumn, 0, len(opts.Columns))
	for _, col := range opts.Columns {
		columns = append(columns, models.WebhookColumn{
			Name:         col.Name,
			Type:         col.Type,
			Nullable:     col.Nullable,
			JsonProperty: col.JsonProperty,
		})
	}

	var tableSettings *models.TableSettings
	if opts.TableSettings != nil {
		tableSettings = &models.TableSettings{
			PrimaryKey:  opts.TableSettings.PrimaryKey,
			PartitionBy: opts.TableSettings.PartitionBy,
			OrderBy:     opts.TableSettings.OrderBy,
			Engine: &models.Engine{ReplacingMergeTreeTableEngine: models.ReplacingMergeTreeTableEngine{
				Ver: opts.TableSettings.Engine.ReplacingMergeTree.Ver,
			}},
		}
	}

	var timestamp string
	if opts.Timestamp != nil {
		timestamp = *opts.Timestamp
	}

	var uniqueID string
	if opts.UniqueID != nil {
		uniqueID = *opts.UniqueID
	}

	return &models.DataSource{
		UniqueName: opts.Name,
		ConnectionSettings: models.ConnectionSettings{
			WebhookConnectionSettings: models.WebhookConnectionSettings{
				WebhookURL: "https://mockURL.com/v1/WHK1234",
				BasicAuth: &models.HttpBasicAuth{
					Username: opts.BasicAuth.Username,
					Password: opts.BasicAuth.Password,
				},
				Columns:       columns,
				Timestamp:     timestamp,
				UniqueID:      uniqueID,
				TableSettings: tableSettings,
			},
		},
	}, nil
}

func (ac *MockApiClient) FetchDataSource(_ context.Context, uniqueName string) (*models.DataSource, error) {
	if mockApiError != nil {
		return nil, mockApiError
	}

	switch uniqueName {
	case "_tacos", "_airlines":
		return &models.DataSource{
			UniqueName: uniqueName,
			ID:         "DSO1234567890",
			ConnectionSettings: models.ConnectionSettings{
				WebhookConnectionSettings: models.WebhookConnectionSettings{
					WebhookURL: "url",
					BasicAuth: &models.HttpBasicAuth{
						Username: "username",
						Password: "password",
					},
					UniqueID: airbyteRawIdColumn,
				},
			},
		}, nil
	case "_deduped stream":
		if requestCounter > 0 {
			return &models.DataSource{
				UniqueName: uniqueName,
				ID:         "DSO9876543210",
				Status:     "CONNECTED",
				ConnectionSettings: models.ConnectionSettings{
					WebhookConnectionSettings: models.WebhookConnectionSettings{
						WebhookURL: "url",
						BasicAuth: &models.HttpBasicAuth{
							Username: "username",
							Password: "password",
						},
						TableSettings: &models.TableSettings{
							PrimaryKey:  []string{},
							PartitionBy: []string{},
							OrderBy:     []string{"id"},
							Engine: &models.Engine{ReplacingMergeTreeTableEngine: models.ReplacingMergeTreeTableEngine{
								Ver: "updated_at",
							}},
						},
					},
				},
			}, nil
		}
	}

	requestCounter += 1

	return nil, graphql.Errors{{
		Message:    "Data Source not found",
		Extensions: map[string]interface{}{"code": "NOT_FOUND"},
	}}
}

func (ac *MockApiClient) FetchDataPool(_ context.Context, _ string) (*models.DataPool, error) {
	return &models.DataPool{
		ID:        "DPO1234567890",
		Timestamp: models.Timestamp{ColumnName: airbyteExtractedAtColumn},
	}, nil
}

func (ac *MockApiClient) CreateDeletionJob(_ context.Context, _ string, _ []models.FilterInput) (*models.Job, error) {
	return &models.Job{
		ID:     "DPJ1234567890",
		Status: "CREATED",
	}, nil
}

func (ac *MockApiClient) FetchDeletionJob(_ context.Context, id string) (*models.Job, error) {
	return &models.Job{
		ID:     id,
		Status: "SUCCEEDED",
	}, nil
}
