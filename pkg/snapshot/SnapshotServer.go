package snapshot

import "io"
import "os"

import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/wal"


//=========================================== Snapshot Server


/*
	StreamSnapshotRPC
		grpc server implementation

		as snapshot chunks enter the stream
			1.) if first chunk, set the metadata for the snapshot (last included index, term, and file path) and open the file
				to stream the compressed chunks into
			2.) for each chunk, write to the snapshot file
			3.) on stream end, break
			4.) index the snapshot in the wal db
			5.) compact the logs up to the last included log
			6.) return a successful response to the leader
*/

func (snpService *SnapshotService) StreamSnapshotRPC(stream snapshotrpc.SnapshotService_StreamSnapshotRPCServer) error {
	var snapshotFile *os.File
	
	var lastIncludedIndex int64
	var lastIncludedTerm int64
	var snapshotFilePath string
  var firstChunkReceived bool
	
	for {
		snapshotChunk, streamErr := stream.Recv()
		if streamErr == io.EOF { break }
		if streamErr != nil { 
			snpService.Log.Error("error receiving chunk from stream:", streamErr.Error())
			return streamErr 
		}

		if ! firstChunkReceived {
			snapshotFilePath = snapshotChunk.SnapshotFilePath
			lastIncludedIndex = snapshotChunk.LastIncludedIndex
			lastIncludedTerm = snapshotChunk.LastIncludedTerm
			firstChunkReceived = true
			
			var openErr error
			snapshotFile, openErr = os.Create(snapshotFilePath)
			if openErr != nil { return openErr }
			
			defer snapshotFile.Close()
		}

		_, writeErr := snapshotFile.Write(snapshotChunk.SnapshotChunk)
		if writeErr != nil { 
			snpService.Log.Error("error writing to snapshot file:", writeErr.Error())
			return writeErr
		}
	}

	snpService.Log.Info("snapshot written, updating index and compacting logs")

	snapshotEntry := &wal.SnapshotEntry{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm: lastIncludedTerm,
		SnapshotFilePath: snapshotFilePath,
	}

	setErr := snpService.CurrentSystem.WAL.SetSnapshot(snapshotEntry)
	if setErr != nil { 
		snpService.Log.Error("error setting snapshot index:", setErr.Error())
		return setErr 
	}

	snpService.Log.Debug("attempting to compact from start to index:", lastIncludedIndex - 1)

	totBytesRem, totKeysRem, delErr := snpService.CurrentSystem.WAL.DeleteLogsUpToLastIncluded(lastIncludedIndex - 1)
	if delErr != nil { 
		snpService.Log.Error("error compacting logs:", delErr.Error())
		return delErr 
	}

	snpService.Log.Debug("total bytes removed:", totBytesRem, "total keys removed:", totKeysRem)

	snpService.Log.Info("snapshot processed log compacted, returning successful response to leader")

	res := &snapshotrpc.SnapshotStreamResponse{ Success: true }
	retStreamErr := stream.SendAndClose(res)
	if retStreamErr != nil { 
		snpService.Log.Error("error sending and closing stream:", retStreamErr.Error())
		return retStreamErr 
	}

	return nil
}