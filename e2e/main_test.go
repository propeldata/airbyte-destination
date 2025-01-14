package e2e

import (
	"context"
	"encoding/json"
	"fmt"
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

const (
	overwriteDataSourceName = "airbyte_overwrite"
	dedupDataSourceName     = "airbyte_dedup"
)

var config Config

func TestMain(m *testing.M) {
	data, err := os.ReadFile("../secrets/config.json")
	if err != nil {
		log.Fatalf("read configuration file failed: %v", err)
	}

	if err := json.Unmarshal(data, &config); err != nil {
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

	overwriteDS, err := apiClient.FetchDataSource(ctx, overwriteDataSourceName)
	c.NoError(err)
	c.Len(overwriteDS.ConnectionSettings.WebhookConnectionSettings.Columns, 4)
	c.Equal("_airbyte_raw_id", overwriteDS.ConnectionSettings.WebhookConnectionSettings.UniqueID)

	dedupDS, err := apiClient.FetchDataSource(ctx, dedupDataSourceName)
	c.NoError(err)
	c.Len(dedupDS.ConnectionSettings.WebhookConnectionSettings.Columns, 5)
	c.Equal([]string{"id"}, dedupDS.ConnectionSettings.WebhookConnectionSettings.TableSettings.OrderBy)
	c.Equal("updated_at", dedupDS.ConnectionSettings.WebhookConnectionSettings.TableSettings.Engine.ReplacingMergeTreeTableEngine.Ver)

	_, err = client.WaitForState(client.StateChangeOps[models.DataGridResponse]{
		Pending: []string{"0", "1", "2", "3", "4", "5", "6", "7"}, // Record count before all records are ingested
		Target:  []string{"8"},                                    // Expected final record count
		Refresh: func() (*models.DataGridResponse, string, error) {
			dataGrid, err := apiClient.FetchDataGrid(ctx, models.DataGridInput{
				DataPool: models.DataPoolInput{Name: overwriteDataSourceName},
				Columns:  []string{"id", "name"},
			})
			c.NoError(err)

			return dataGrid, strconv.Itoa(len(dataGrid.Rows)), nil
		},
		Timeout: 5 * time.Minute,
		Delay:   30 * time.Second,
	})
	c.NoError(err)

	dataGrid, err := client.WaitForState(client.StateChangeOps[models.DataGridResponse]{
		Pending: []string{"0", "1"},
		Target:  []string{"2"},
		Refresh: func() (*models.DataGridResponse, string, error) {
			dataGrid, err := apiClient.FetchDataGrid(ctx, models.DataGridInput{
				DataPool: models.DataPoolInput{Name: dedupDataSourceName},
				Columns:  []string{"id", "name"},
			})
			c.NoError(err)

			return dataGrid, strconv.Itoa(len(dataGrid.Rows)), nil
		},
		Timeout: 5 * time.Minute,
		Delay:   30 * time.Second,
	})
	c.NoError(err)
	c.Equal("0", *dataGrid.Rows[0][0])
	c.Equal("delta", *dataGrid.Rows[0][1])
	c.Equal("1", *dataGrid.Rows[1][0])
	c.Equal("aeromexico", *dataGrid.Rows[1][1])
}

func cleanup() {
	ctx := context.Background()
	oauthClient := client.NewOauthClient()

	oauthToken, err := oauthClient.OAuthToken(ctx, config.AppId, config.AppSecret)
	if err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	apiClient := client.NewApiClient(oauthToken.AccessToken)

	if _, err := apiClient.DeleteDataPool(ctx, dedupDataSourceName); err != nil {
		log.Fatalf("delete Data Pool failed: %v", err)
	}

	if _, err := client.WaitForState(client.StateChangeOps[models.DataPool]{
		Pending: []string{"DELETING"},
		Target:  []string{"DELETED"},
		Refresh: func() (*models.DataPool, string, error) {
			resp, err := apiClient.FetchDataPool(ctx, dedupDataSourceName)
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
		log.Fatalf("deleted Data Pool status transition failed: %v", err)
	}

	if _, err := apiClient.DeleteDataSource(ctx, dedupDataSourceName); err != nil {
		log.Fatalf("delete Data Source failed: %v", err)
	}

	if _, err := client.WaitForState(client.StateChangeOps[models.DataSource]{
		Pending: []string{"DELETING"},
		Target:  []string{"DELETED"},
		Refresh: func() (*models.DataSource, string, error) {
			resp, err := apiClient.FetchDataSource(ctx, dedupDataSourceName)
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
		log.Fatalf("deleted Data Source status transition failed: %v", err)
	}
}
