package cmd

import (
	"github.com/spf13/cobra"
)

func RootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "destination",
		Short: "Propel Airbyte connector",
	}

	cmd.AddCommand(specCommand())
	cmd.AddCommand(checkCommand())
	cmd.AddCommand(writeCommand())

	return cmd
}
