package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"strings"
	"sync"
)

type snapshotMetadata struct {
	name string
	size int64
}
type shootProperties struct {
	numFull   int64
	numDelta  int64
	sizeFull  int64
	sizeDelta int64
	total     int64
}

func generateStatistics(ctx context.Context, connection containerConnection) (numberFull, numberDelta, totalFullSize, totalDeltaSize, maxFullSize, maxDeltaSize int64, shootMap map[string]shootProperties) {
	// create all consumers for the full and the delta snapshots
	fullSnapshotCh := make(chan string)
	deltaSnapshotCh := make(chan string)

	fullSnapshotMetadataCh := make(chan snapshotMetadata)
	deltaSnapshotMetadataCh := make(chan snapshotMetadata)

	shootMap = make(map[string]shootProperties)

	errCh := make(chan error)
	go connection.getSnapshots(ctx, fullSnapshotCh, deltaSnapshotCh, errCh)

	var wgFull, wgDelta sync.WaitGroup

	for range 100 {
		wgFull.Add(1)
		go connection.getSnapshotMetadata(ctx, &wgFull, fullSnapshotCh, fullSnapshotMetadataCh)
	}
	for range 100 {
		wgDelta.Add(1)
		go connection.getSnapshotMetadata(ctx, &wgDelta, deltaSnapshotCh, deltaSnapshotMetadataCh)
	}

	var mutex sync.Mutex

	go func() {
		for fullSnapshotMetadata := range fullSnapshotMetadataCh {
			mutex.Lock()
			shootName := fullSnapshotMetadata.name[:strings.Index(fullSnapshotMetadata.name, "/")]
			prop := shootMap[shootName]
			prop.numFull++
			prop.sizeFull += fullSnapshotMetadata.size
			prop.total += fullSnapshotMetadata.size
			shootMap[shootName] = prop
			totalFullSize += fullSnapshotMetadata.size
			maxFullSize = max(maxFullSize, fullSnapshotMetadata.size)
			numberFull++
			mutex.Unlock()
		}
	}()

	go func() {
		for deltaSnapshotMetadata := range deltaSnapshotMetadataCh {
			mutex.Lock()
			shootName := deltaSnapshotMetadata.name[:strings.Index(deltaSnapshotMetadata.name, "/")]
			prop := shootMap[shootName]
			prop.numDelta++
			prop.sizeDelta += deltaSnapshotMetadata.size
			prop.total += deltaSnapshotMetadata.size
			shootMap[shootName] = prop
			totalDeltaSize += deltaSnapshotMetadata.size
			maxDeltaSize = max(maxDeltaSize, deltaSnapshotMetadata.size)
			numberDelta++
			mutex.Unlock()
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
	close(fullSnapshotMetadataCh)
	close(deltaSnapshotMetadataCh)

	return
}

func writeShootPropToCSV(csv *csv.Writer, props map[string]shootProperties) {
	csv.Write([]string{"Shoot Name", "Number of full snapshots", "Number of delta snapshots", "Full snapshot consumption", "Delta snapshot consumption", "Total storage consumption"})
	for shoot, prop := range props {
		csv.Write([]string{shoot, fmt.Sprint(prop.numFull), fmt.Sprint(prop.numDelta), fmt.Sprint(prop.sizeFull), fmt.Sprint(prop.sizeDelta), fmt.Sprint(prop.total)})
		csv.Flush()
	}
}

func writeSummaryToCSV(csv *csv.Writer, numberFull, numberDelta, totalFullSize, totalDeltaSize, maxFullSize, maxDeltaSize int64, shootPropMap map[string]shootProperties) {
	csv.Write([]string{"", "", "", "", "", "", ""})
	csv.Write([]string{"Number of shoots", "Number of full snapshots", "Number of delta snapshots", "Total size of full snapshots", "Total size of delta snapshots", "Max size of full snapshots", "Max size of delta snapshots"})
	csv.Write([]string{fmt.Sprint(len(shootPropMap)), fmt.Sprint(numberFull), fmt.Sprint(numberDelta), fmt.Sprint(totalFullSize), fmt.Sprint(totalDeltaSize), fmt.Sprint(maxFullSize), fmt.Sprint(maxDeltaSize)})
	csv.Flush()
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
