package e2e

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/propeldata/go-client"
	"github.com/propeldata/go-client/models"
	"github.com/stretchr/testify/require"
)

type Config struct {
	AppId     string `json:"application_id"`
	AppSecret string `json:"application_secret"`
}

const dataSourceUniqueName = "_airbyte_airlines"

var config Config

func TestMain(m *testing.M) {
	data, err := os.ReadFile("../secrets/config.json")
	if err != nil {
		log.Fatalf("read configuration file failed: %v", err)
	}

	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("unmarshal failed: %v", err)
	}

	code := m.Run()

	cleanup()

	os.Exit(code)
}

func TestWrite(t *testing.T) {
	c := require.New(t)

	ctx := context.Background()
	oauthClient := client.NewOauthClient()

	oauthToken, err := oauthClient.OAuthToken(ctx, config.AppId, config.AppSecret)
	c.NoError(err)

	apiClient := client.NewApiClient(oauthToken.AccessToken)

	dataSource, err := apiClient.FetchDataSource(ctx, dataSourceUniqueName)
	c.NoError(err)
	c.Len(dataSource.ConnectionSettings.WebhookConnectionSettings.Columns, 4)

	newRecords := []string{"1", "2", "3", "4", "5", "6", "7", "8"}

	_, err = client.WaitForState(client.StateChangeOps[models.RecordsByUniqueIdResponse]{
		Pending: []string{"0", "1", "2", "3", "4", "5", "6", "7"},
		Target:  []string{"8"},
		Refresh: func() (*models.RecordsByUniqueIdResponse, string, error) {
			records, err := apiClient.FetchRecordsByUniqueId(ctx, dataSource.UniqueName, newRecords, []string{"id", "name"})
			c.NoError(err)

			return records, strconv.Itoa(len(records.Values)), nil
		},
		Timeout: 5 * time.Minute,
		Delay:   30 * time.Second,
	})
	c.NoError(err)
}

func cleanup() {
	ctx := context.Background()
	oauthClient := client.NewOauthClient()

	oauthToken, err := oauthClient.OAuthToken(ctx, config.AppId, config.AppSecret)
	if err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	apiClient := client.NewApiClient(oauthToken.AccessToken)

	dataPool, err := apiClient.FetchDataPool(ctx, dataSourceUniqueName)
	if err != nil {
		log.Fatalf("fetch data pool failed: %v", err)
	}

	if _, err := apiClient.CreateDeletionJob(ctx, dataPool.ID, []models.FilterInput{
		{Column: "id", Operator: "IS_NOT_NULL"},
	}); err != nil {
		log.Fatalf("deletion job failed: %v", err)
	}
}
