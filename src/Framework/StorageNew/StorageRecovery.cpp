#include "StorageRecovery.h"

bool StorageRecovery::TryRecovery()
{
    // TODO: open 'toc' or 'toc.new' file
    
    // try to open 'toc.new' file and check the checksum
    // if OK then rename to 'toc'
    // else try to open 'toc' and check the checksum
    // if neither exists, return false
    // if the checksums don't work out, stop the program
    
    // at this point, we have a valid toc
    // which is a list of shards, each of which contains a list of chunkIDs
    
    // next, create FileChunks for each chunks
    // and read each FileChunk's headerPage from disk
    // create a MemoChunk for each shard
    
    // compute the max. (logSegmentID, commandID) for each shard
    
    // at this point, start to replay the log segments
    // create a StorageLogSegment for each
    // and for each (logSegmentID, commandID) => (contextID, shardID)
    // look at that shard's computed max., if the log is bigger, then execute the command
    // against the MemoChunk

    return false;
}
