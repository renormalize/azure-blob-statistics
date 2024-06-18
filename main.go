package main

import (
	"context"
	"fmt"
	"os"
	"sync"
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

func generateStatistics(ctx context.Context, connection containerConnection) (numberFull, numberDelta, totalFullSize, totalDeltaSize, maxFullSize, maxDeltaSize int64) {
	// create all consumers for the full and the delta snapshots
	fullSnapshotCh := make(chan string)
	deltaSnapshotCh := make(chan string)

	fullSnapshotPropertyCh := make(chan snapshotProperties)
	deltaSnapshotPropertyCh := make(chan snapshotProperties)

	errCh := make(chan error)
	go connection.getSnapshots(ctx, fullSnapshotCh, deltaSnapshotCh, errCh)

	var wgFull, wgDelta sync.WaitGroup

	for range 1000 {
		wgFull.Add(1)
		go connection.getSnapshotProperties(ctx, &wgFull, fullSnapshotCh, fullSnapshotPropertyCh)
	}
	for range 1000 {
		wgDelta.Add(1)
		go connection.getSnapshotProperties(ctx, &wgDelta, deltaSnapshotCh, deltaSnapshotPropertyCh)
	}

	go func() {
		for fullSnapshotProperty := range fullSnapshotPropertyCh {
			totalFullSize += fullSnapshotProperty.size
			maxFullSize = max(maxFullSize, fullSnapshotProperty.size)
			numberFull++
		}
	}()

	go func() {
		for deltaSnapshotProperty := range deltaSnapshotPropertyCh {
			totalDeltaSize += deltaSnapshotProperty.size
			maxDeltaSize = max(maxDeltaSize, deltaSnapshotProperty.size)
			numberDelta++
		}
	}()

	var err error
	if err = <-errCh; err != nil {
		fmt.Println("Could not list the blob names:", err)
		return
	}
	close(fullSnapshotCh)
	close(deltaSnapshotCh)

	wgFull.Wait()
	wgDelta.Wait()
	close(fullSnapshotPropertyCh)
	close(deltaSnapshotPropertyCh)

	return
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
	}

	connection, err := createContainerConnection(accountName, accountKey, containerName)
	if err != nil {
		fmt.Println("Could not create a connection:", err)
		return
	}

	ctx := context.Background()
	numberFull, numberDelta, totalFullSize, totalDeltaSize, maxFullSize, maxDeltaSize := generateStatistics(ctx, connection)

	fmt.Println("Running for account:", accountName)
	fmt.Println("Running for container:", containerName)
	fmt.Println("Running for seed:", seedName)

	fmt.Println("Number of full snapshots are:", humanize.Comma(int64(numberFull)))
	fmt.Println("Number of delta snapshots are:", humanize.Comma(int64(numberDelta)))
	fmt.Println("Total size is:", humanize.Bytes(uint64(totalFullSize+totalDeltaSize)))
	fmt.Println("Total size of fulls is:", humanize.Bytes(uint64(totalFullSize)))
	fmt.Println("Total size of deltas is:", humanize.Bytes(uint64(totalDeltaSize)))
	fmt.Println("Max size of fulls is:", humanize.Bytes(uint64(maxFullSize)))
	fmt.Println("Max size of deltas is:", humanize.Bytes(uint64(maxDeltaSize)))
	fmt.Println()

	fmt.Println("Ran in:", time.Since(start).Seconds())
	fmt.Println()
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
