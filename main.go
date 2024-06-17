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

	connection, err := createAzConnection(accountName, accountKey, containerName)
	if err != nil {
		fmt.Println("Could not create a connection:", err)
		return
	}

	fullSnapshots, deltaSnapshots, err := connection.listBlobs()
	if err != nil {
		fmt.Println("Could not list the blob names:", err)
		return
	}

	fmt.Println("The full snapshots are:")
	for _, snapshot := range fullSnapshots {
		fmt.Printf("%s\n", snapshot)
	}

	fmt.Println("The delta snapshots are:")
	for _, snapshot := range deltaSnapshots {
		fmt.Printf("%s\n", snapshot)
	}

	fmt.Println()

	fmt.Println("Number of full snapshots are:", len(fullSnapshots))
	fmt.Println("Number of delta snapshots are:", len(deltaSnapshots))

	fmt.Println()

}
