package cmd

import (
	"github.com/spf13/cobra"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
	"github.com/propeldata/airbyte-destination/internal/connector"
)

func checkCommand() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "check",
		Short: "Validates the given configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := airbyte.NewLogger(cmd.OutOrStdout())
			destination := connector.NewDestination(logger)

			logger.ConnectionStatus(destination.Check(configPath))

			return nil
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "", "Configuration file path")
	cobra.CheckErr(cmd.MarkFlagRequired("config"))

	return cmd
}
