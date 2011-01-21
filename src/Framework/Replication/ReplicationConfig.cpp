#include "ReplicationConfig.h"
#include "System/Buffers/Buffer.h"
#include "Framework/Storage/StorageEnvironment.h"

#define WIDTH_NODEID                16
#define WIDTH_RUNID                 16

ReplicationConfig* replicationConfig = NULL;

ReplicationConfig* ReplicationConfig::Get()
{
    if (replicationConfig == NULL)
        replicationConfig = new ReplicationConfig();
    
    return replicationConfig;
}

ReplicationConfig::ReplicationConfig()
{
    runID = 0;
    nodeID = 0;
    clusterID = 0;
}

void ReplicationConfig::Init(StorageShardProxy* shard_)
{
    ReadBuffer  value;
    bool        ret;
    unsigned    nread;
    
    shard = shard_;

    // TODO: HACK
    shard->GetEnvironment()->CreateShard(shard->GetContextID(), shard->GetShardID(), 
     0, "", "", true, false);
    
    ret = shard->Get(ReadBuffer("nodeID"), value);
    nread = 0;
    nodeID = BufferToUInt64(value.GetBuffer(), value.GetLength(), &nread);
    if (!ret || nread != value.GetLength())
    {
        Log_Message("No nodeID read from database");
        nodeID = 0;
    }

    ret = shard->Get(ReadBuffer("runID"), value);
    nread = 0;
    runID = BufferToUInt64(value.GetBuffer(), value.GetLength(), &nread);
    if (!ret || nread != value.GetLength())
    {
        Log_Message("No runID read from database");
        runID = 0;
    }
    
    ret = shard->Get(ReadBuffer("clusterID"), value);
    nread = 0;
    clusterID = BufferToUInt64(value.GetBuffer(), value.GetLength(), &nread);
    if (!ret || nread != value.GetLength())
    {
        Log_Message("No clusterID read from database");
        clusterID = 0;
    }
}

void ReplicationConfig::Shutdown()
{
    delete replicationConfig;
    replicationConfig = NULL;
}

void ReplicationConfig::SetNodeID(uint64_t nodeID_)
{
    nodeID = nodeID_;
}

uint64_t ReplicationConfig::GetNodeID()
{
    return nodeID;
}

void ReplicationConfig::SetRunID(uint64_t runID_)
{
    runID = runID_;
}

uint64_t ReplicationConfig::GetRunID()
{
    return runID;
}

void ReplicationConfig::SetClusterID(uint64_t clusterID_)
{
    clusterID = clusterID_;
}

uint64_t ReplicationConfig::GetClusterID()
{
    return clusterID;
}

uint64_t ReplicationConfig::NextProposalID(uint64_t proposalID)
{
    // <proposal count since restart> <runID> <nodeID>
    
    assert(nodeID < (1 << WIDTH_NODEID));
    assert(runID < (1 << WIDTH_RUNID));
    
    uint64_t left, middle, right, nextProposalID;
    
    left = proposalID >> (WIDTH_NODEID + WIDTH_RUNID);
    left++;
    left = left << (WIDTH_NODEID + WIDTH_RUNID);
    middle = runID << WIDTH_NODEID;
    right = nodeID;
    nextProposalID = left | middle | right;
    
    assert(nextProposalID > proposalID);
    
    return nextProposalID;
}

void ReplicationConfig::Commit()
{
    Buffer      value;
    ReadBuffer  rbValue;
    bool        ret;
    
    value.Writef("%U", nodeID);
    rbValue.Wrap(value);
    ret = shard->Set(ReadBuffer("nodeID"), rbValue);
    assert(ret == true);

    value.Writef("%U", runID);
    rbValue.Wrap(value);
    ret = shard->Set(ReadBuffer("runID"), rbValue);
    assert(ret == true);

    value.Writef("%U", clusterID);
    rbValue.Wrap(value);
    ret = shard->Set(ReadBuffer("clusterID"), rbValue);
    assert(ret == true);
    
    shard->GetEnvironment()->Commit();
}
