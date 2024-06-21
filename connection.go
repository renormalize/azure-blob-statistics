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

// returns the full snapshots and delta snapshots
func (connection containerConnection) getSnapshots(ctx context.Context, fullSnapshotCh, deltaSnapshotCh chan string, errorCh chan error) {
	opts := azblob.ListBlobsSegmentOptions{}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := connection.containerURL.ListBlobsFlatSegment(ctx, marker, opts)
		if err != nil {
			errorCh <- fmt.Errorf("failed to list the blobs, error: %v", err)
			return
		}
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment
		for _, blob := range listBlob.Segment.BlobItems {
			// send to the channel consumed by getSnapshotMetadata()
			if strings.Contains(blob.Name, "v2/Full") {
				fullSnapshotCh <- blob.Name
			} else if strings.Contains(blob.Name, "v2/Incr") {
				deltaSnapshotCh <- blob.Name
			}
		}
	}
	errorCh <- nil
}

func (connection containerConnection) getSnapshotMetadata(ctx context.Context, wg *sync.WaitGroup, blobNameCh chan string, metadataCh chan snapshotMetadata) {
	defer wg.Done()
	for blobName := range blobNameCh {
		blobURL := connection.containerURL.NewBlobURL(blobName)
		resp, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
		// do not count snapshot if request errors
		if err != nil {
			continue
		}
		metadataCh <- snapshotMetadata{
			blobName,
			resp.ContentLength(),
		}
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
