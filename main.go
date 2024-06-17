package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

func containerInfo() (string, string, string, error) {
	accountName, ok := os.LookupEnv("AZURE_ACCOUNT_NAME")
	if !ok {
		return "", "", "", fmt.Errorf("no account name was provided")
	}

	accountKey, ok := os.LookupEnv("AZURE_ACCOUNT_KEY")
	if !ok {
		return "", "", "", fmt.Errorf("no account key was provided")
	}

	containerName, ok := os.LookupEnv("AZURE_CONTAINER_NAME")
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

	connection, err := createContainerConnection(accountName, accountKey, containerName)
	if err != nil {
		fmt.Println("Could not create a connection:", err)
		return
	}

	fullSnapshots, deltaSnapshots, err := connection.getSnapshots()
	if err != nil {
		fmt.Println("Could not list the blob names:", err)
		return
	}

	var wg sync.WaitGroup

	deltaProperties := make([]snapshotProperties, len(deltaSnapshots))
	var totalDeltaSize, maxDeltaSize int64

	for i, delta := range deltaSnapshots {
		wg.Add(1)
		go connection.getSnapshotProperties(&wg, delta, deltaProperties, i)
	}
	// delta calls done
	wg.Wait()

	for _, delta := range deltaProperties {
		totalDeltaSize += delta.size
		maxDeltaSize = max(maxDeltaSize, delta.size)
	}

	fullProperties := make([]snapshotProperties, len(fullSnapshots))
	var totalFullSize, maxFullSize int64

	for i, full := range fullSnapshots {
		wg.Add(1)
		go connection.getSnapshotProperties(&wg, full, fullProperties, i)
	}
	// full calls done
	wg.Wait()

	for _, full := range fullProperties {
		totalFullSize += full.size
		maxFullSize = max(maxFullSize, full.size)
	}

	fmt.Println("Number of full snapshots are:", len(fullSnapshots))
	fmt.Println("Number of delta snapshots are:", len(deltaSnapshots))
	fmt.Println("Total size of deltas is:", totalDeltaSize)
	fmt.Println("Total size of fulls is:", totalFullSize)
	fmt.Println("Max size of deltas is:", maxDeltaSize)
	fmt.Println("Max size of fulls is:", maxFullSize)
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
