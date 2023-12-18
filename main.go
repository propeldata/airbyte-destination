package main

import (
	"log"
	"os"

	"github.com/bitstrapped/airbyte"

	"github.com/propeldata/airbyte-destination/internal/destination"
)

func main() {
	propel := destination.NewPropel()
	runner := airbyte.NewDestinationRunner(propel, os.Stdin, os.Stdout)

	if err := runner.Start(); err != nil {
		log.Fatal()
	}
}
