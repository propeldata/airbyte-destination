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

const (
	overwriteDataSourceName = "_airbyte_overwrite"
	dedupDataSourceName     = "_airbyte_dedup"
)

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

	os.Exit(m.Run())
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
