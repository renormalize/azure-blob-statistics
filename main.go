package main

import (
	"fmt"
	"os"
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

	var deltaProperties []snapshotProperties
	var totalDeltaSize, maxDeltaSize int64

	for _, delta := range deltaSnapshots {
		property, err := connection.getSnapshotProperties(delta)
		if err != nil {
			fmt.Printf("Errored while getting the snapshots properties for blob %s, error: %v", delta, err)
		}
		deltaProperties = append(deltaProperties, property)
		totalDeltaSize += property.size
		maxDeltaSize = max(maxDeltaSize, property.size)

	}

	var fullProperties []snapshotProperties
	var totalFullSize, maxFullSize int64
	for _, full := range fullSnapshots {
		property, err := connection.getSnapshotProperties(full)
		if err != nil {
			fmt.Printf("Errored while getting the snapshots properties for blob %s, error: %v", full, err)
		}
		fullProperties = append(fullProperties, property)
		totalFullSize += property.size
		maxFullSize = max(maxFullSize, property.size)
	}

	fmt.Println("Number of full snapshots are:", len(fullSnapshots))
	fmt.Println("Number of delta snapshots are:", len(deltaSnapshots))
	fmt.Println("Total size of deltas is:", totalDeltaSize)
	fmt.Println("Total size of fulls is:", totalFullSize)
	fmt.Println("Max size of deltas is:", maxDeltaSize)
	fmt.Println("Max size of fulls is:", maxFullSize)

	fmt.Println()

}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
