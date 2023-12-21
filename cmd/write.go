package cmd

import (
	"github.com/spf13/cobra"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
	"github.com/propeldata/airbyte-destination/internal/connector"
)

func writeCommand() *cobra.Command {
	var configPath string
	var catalogPath string

	cmd := &cobra.Command{
		Use:   "write",
		Short: "Write records at destination",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := airbyte.NewLogger(cmd.OutOrStdout())
			destination := connector.NewDestination(logger)

			state, err := destination.Write(cmd.Context(), configPath, catalogPath, cmd.InOrStdin())
			if err != nil {
				logger.Log(airbyte.LogLevelError, err.Error())
				return err
			}

			logger.State(state)

			return nil
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "", "Configuration file")
	cmd.Flags().StringVar(&catalogPath, "catalog", "", "Catalog file")

	cobra.CheckErr(cmd.MarkFlagRequired("config"))
	cobra.CheckErr(cmd.MarkFlagRequired("catalog"))

	return cmd
}
