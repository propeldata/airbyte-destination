package cmd

import (
	"github.com/spf13/cobra"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
	"github.com/propeldata/airbyte-destination/internal/connector"
)

func specCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "spec",
		Short: "Connector configuration schema",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := airbyte.NewLogger(cmd.OutOrStdout())
			destination := connector.NewDestination(logger)

			logger.Spec(destination.Spec())

			return nil
		},
	}

	return cmd
}
