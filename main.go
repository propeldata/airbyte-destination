package main

import (
	"os"

	"github.com/propeldata/airbyte-destination/cmd"
	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

func main() {
	rootCmd := cmd.RootCommand()
	logger := airbyte.NewLogger(rootCmd.OutOrStdout())

	if err := rootCmd.Execute(); err != nil {
		logger.Log(airbyte.LogLevelError, err.Error())
		os.Exit(1)
	}
}
