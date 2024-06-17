package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type azConnection struct {
	containerURL *azblob.ContainerURL
}

// returns the full snapshots and delta snapshots
func (connection azConnection) listBlobs() ([]string, []string, error) {
	var fullSnapshots, deltaSnapshots []string
	opts := azblob.ListBlobsSegmentOptions{}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := connection.containerURL.ListBlobsFlatSegment(context.TODO(), marker, opts)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to list the blobs, error: %v", err)
		}
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment
		for _, blob := range listBlob.Segment.BlobItems {
			if strings.Contains(blob.Name, "v2/Full") {
				fullSnapshots = append(fullSnapshots, blob.Name)
			} else if strings.Contains(blob.Name, "v2/Incr") {
				deltaSnapshots = append(deltaSnapshots, blob.Name)
			}
		}
	}
	return fullSnapshots, deltaSnapshots, nil
}

func createAzConnection(accountName, accountKey, containerName string) (azConnection, error) {
	credentials, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Println()
		return azConnection{}, fmt.Errorf("unable to create a credential object due to: %w", err)

	}

	pipeline := azblob.NewPipeline(credentials, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: 5 * time.Minute,
		},
	})

	blobURL, err := url.Parse(fmt.Sprintf("https://%s.%s", credentials.AccountName(), "blob.core.windows.net"))
	if err != nil {
		return azConnection{}, fmt.Errorf("unable to construct the blob url due to: %w", err)
	}

	serviceURL := azblob.NewServiceURL(*blobURL, pipeline)
	containerURL := serviceURL.NewContainerURL(containerName)

	return azConnection{
		containerURL: &containerURL,
	}, nil
}
