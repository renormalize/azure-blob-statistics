package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

const (
	azureAccountName   = "AZURE_ACCOUNT_NAME"
	azureAccountKey    = "AZURE_ACCOUNT_KEY"
	azureContainerName = "AZURE_CONTAINER_NAME"
	azureSeedName      = "AZURE_SEED_NAME"
)

func containerInfo() (string, string, string, error) {
	accountName, ok := os.LookupEnv(azureAccountName)
	if !ok {
		return "", "", "", fmt.Errorf("no account name was provided")
	}

	accountKey, ok := os.LookupEnv(azureAccountKey)
	if !ok {
		return "", "", "", fmt.Errorf("no account key was provided")
	}

	containerName, ok := os.LookupEnv(azureContainerName)
	if !ok {
		return "", "", "", fmt.Errorf("no container name was provided")
	}

	return accountName, accountKey, containerName, nil
}

func main() {
	fmt.Println("Container statistics!")
	fmt.Println()

	start := time.Now()

	accountName, accountKey, containerName, err := containerInfo()
	if err != nil {
		fmt.Println("Starting the program failed because:", err)
		return
	}

	seedName, ok := os.LookupEnv(azureSeedName)
	if !ok {
		fmt.Println("Seed name not provided")
		return
	}

	connection, err := createContainerConnection(accountName, accountKey, containerName)
	if err != nil {
		fmt.Println("Could not create a connection:", err)
		return
	}

	file, err := os.Create(strings.ReplaceAll(seedName, "/", "") + ".csv")
	if err != nil {
		fmt.Println("Could not create a file:", err)
	}

	ctx := context.Background()
	numberFull, numberDelta, totalFullSize, totalDeltaSize, maxFullSize, maxDeltaSize, shootPropMap := generateStatistics(ctx, connection)

	csv := csv.NewWriter(file)
	writeShootPropToCSV(csv, shootPropMap)
	writeSummaryToCSV(csv, numberFull, numberDelta, totalFullSize, totalDeltaSize, maxFullSize, maxDeltaSize, shootPropMap)

	fmt.Println("Running for account:", accountName)
	fmt.Println("Running for container:", containerName)
	fmt.Println("Running for seed:", seedName)

	fmt.Println("Number of full snapshots are:", humanize.Comma(numberFull))
	fmt.Println("Number of delta snapshots are:", humanize.Comma(numberDelta))
	fmt.Println("Total size is:", humanize.Bytes(uint64(totalFullSize+totalDeltaSize)))
	fmt.Println("Total size of fulls is:", humanize.Bytes(uint64(totalFullSize)))
	fmt.Println("Total size of deltas is:", humanize.Bytes(uint64(totalDeltaSize)))
	fmt.Println("Max size of fulls is:", humanize.Bytes(uint64(maxFullSize)))
	fmt.Println("Max size of deltas is:", humanize.Bytes(uint64(maxDeltaSize)))
	fmt.Println("Number of shoots is:", humanize.Comma(int64(len(shootPropMap))))
	fmt.Println()

	fmt.Println("Ran in:", time.Since(start).Seconds())
	fmt.Println()
}
