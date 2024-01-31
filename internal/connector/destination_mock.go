package connector

import (
	"context"

	"github.com/propeldata/go-client"
	"github.com/propeldata/go-client/models"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

const mockAccessToken = "mockAccessToken"

var (
	mockOAuthError   error = nil
	mockWebhookError error = nil
	mockApiError     error = nil
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

func (ac *MockApiClient) CreateDataSource(ctx context.Context, opts client.CreateDataSourceOpts) (*models.DataSource, error) {
	return &models.DataSource{}, nil
}

func (ac *MockApiClient) FetchDataSource(ctx context.Context, uniqueName string) (*models.DataSource, error) {
	if mockApiError != nil {
		return nil, mockApiError
	}

	return &models.DataSource{
		UniqueName: "uniqueName",
		ID:         "DPO1234567890",
		ConnectionSettings: models.ConnectionSettings{
			WebhookConnectionSettings: models.WebhookConnectionSettings{
				WebhookURL: "url",
				BasicAuth: &models.HttpBasicAuth{
					Username: "username",
					Password: "password",
				},
			},
		},
	}, nil
}
