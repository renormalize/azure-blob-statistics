package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type containerConnection struct {
	containerURL *azblob.ContainerURL
}

type snapshotProperties struct {
	name string
	size int64
}

// returns the full snapshots and delta snapshots
func (connection containerConnection) getSnapshots() ([]string, []string, error) {
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

func (connection containerConnection) getSnapshotProperties(wg *sync.WaitGroup, blobName string, properties []snapshotProperties, i int) {
	defer wg.Done()
	blobURL := connection.containerURL.NewBlobURL(blobName)
	resp, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	// do not count snapshot if request errors
	if err != nil {
		return
	}
	properties[i] = snapshotProperties{
		blobName,
		resp.ContentLength(),
	}
}

func createContainerConnection(accountName, accountKey, containerName string) (containerConnection, error) {
	credentials, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Println()
		return containerConnection{}, fmt.Errorf("unable to create a credential object due to: %w", err)

	}

	pipeline := azblob.NewPipeline(credentials, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: 5 * time.Minute,
		},
	})

	blobURL, err := url.Parse(fmt.Sprintf("https://%s.%s", credentials.AccountName(), "blob.core.windows.net"))
	if err != nil {
		return containerConnection{}, fmt.Errorf("unable to construct the blob url due to: %w", err)
	}

	serviceURL := azblob.NewServiceURL(*blobURL, pipeline)
	containerURL := serviceURL.NewContainerURL(containerName)

	return containerConnection{
		containerURL: &containerURL,
	}, nil
}
