#include "ClusterMessage.h"
#include "Framework/Messaging/MessageUtil.h"
#include "Version.h"

static inline bool LessThan(uint64_t a, uint64_t b)
{
    return a < b;
}

bool ClusterMessage::IsShardMigrationMessage()
{
    if (
     type == CLUSTERMESSAGE_SHARDMIGRATION_INITIATE     ||
     type == CLUSTERMESSAGE_SHARDMIGRATION_BEGIN        ||
     type == CLUSTERMESSAGE_SHARDMIGRATION_SET          ||
     type == CLUSTERMESSAGE_SHARDMIGRATION_DELETE       ||
     type == CLUSTERMESSAGE_SHARDMIGRATION_COMMIT       ||
     type == CLUSTERMESSAGE_SHARDMIGRATION_COMPLETE)
        return true;
    else
        return false;
}


bool ClusterMessage::SetNodeID(uint64_t clusterID_, uint64_t nodeID_)
{
    type = CLUSTERMESSAGE_SET_NODEID;
    clusterID = clusterID_;
    nodeID = nodeID_;
    return true;
}

bool ClusterMessage::Heartbeat(uint64_t nodeID_,
 List<QuorumInfo>& quorumInfos_, List<QuorumShardInfo>& quorumShardInfos_,
 unsigned httpPort_, unsigned sdbpPort_)
{
    type = CLUSTERMESSAGE_HEARTBEAT;
    nodeID = nodeID_;
    quorumInfos = quorumInfos_;
    quorumShardInfos = quorumShardInfos_;
    httpPort = httpPort_;
    sdbpPort = sdbpPort_;
    return true;
}

bool ClusterMessage::SetConfigState(ConfigState& configState_)
{
    type = CLUSTERMESSAGE_SET_CONFIG_STATE;
    configState = configState_;
    return true;
}

bool ClusterMessage::RequestLease(uint64_t nodeID_, uint64_t quorumID_,
 uint64_t proposalID_, uint64_t paxosID_, uint64_t configID_, unsigned duration_)
{
    type = CLUSTERMESSAGE_REQUEST_LEASE;
    nodeID = nodeID_;
    quorumID = quorumID_;
    proposalID = proposalID_;
    paxosID = paxosID_;
    configID = configID_;
    duration = duration_;
    return true;
}

bool ClusterMessage::ReceiveLease(uint64_t nodeID_, uint64_t quorumID_,
 uint64_t proposalID_, uint64_t configID_, unsigned duration_,
 bool watchingPaxosID_, SortedList<uint64_t>& activeNodes_, SortedList<uint64_t>& shards_)
{
    type = CLUSTERMESSAGE_RECEIVE_LEASE;
    nodeID = nodeID_;
    quorumID = quorumID_;
    proposalID = proposalID_;
    configID = configID_;
    duration = duration_;
    activeNodes = activeNodes_;
    shards = shards_;
    watchingPaxosID = watchingPaxosID_;
    return true;
}

bool ClusterMessage::ShardMigrationInitiate(uint64_t nodeID_,
 uint64_t quorumID_, uint64_t srcShardID_, uint64_t dstShardID_)
{
    type = CLUSTERMESSAGE_SHARDMIGRATION_INITIATE;
    nodeID = nodeID_;
    quorumID = quorumID_;
    srcShardID = srcShardID_;
    dstShardID = dstShardID_;
    return true;
}

bool ClusterMessage::ShardMigrationBegin(
 uint64_t quorumID_, uint64_t srcShardID_, uint64_t dstShardID_)
{
    type = CLUSTERMESSAGE_SHARDMIGRATION_BEGIN;
    quorumID = quorumID_;
    srcShardID = srcShardID_;
    dstShardID = dstShardID_;
    return true;
}

bool ClusterMessage::ShardMigrationSet(
 uint64_t quorumID_, uint64_t shardID_, ReadBuffer key_, ReadBuffer value_)
{
    type = CLUSTERMESSAGE_SHARDMIGRATION_SET;
    quorumID = quorumID_;
    shardID = shardID_;
    key = key_;
    value = value_;
    return true;
}

bool ClusterMessage::ShardMigrationDelete(
 uint64_t quorumID_, uint64_t shardID_, ReadBuffer key_)
{
    type = CLUSTERMESSAGE_SHARDMIGRATION_DELETE;
    quorumID = quorumID_;
    shardID = shardID_;
    key = key_;
    return true;
}

bool ClusterMessage::ShardMigrationCommit(
 uint64_t quorumID_, uint64_t shardID_)
{
    type = CLUSTERMESSAGE_SHARDMIGRATION_COMMIT;
    quorumID = quorumID_;
    shardID = shardID_;
    return true;
}

bool ClusterMessage::ShardMigrationComplete(
 uint64_t quorumID_, uint64_t shardID_)
{
    type = CLUSTERMESSAGE_SHARDMIGRATION_COMPLETE;
    quorumID = quorumID_;
    shardID = shardID_;
    return true;
}

bool ClusterMessage::Hello()
{
    type = CLUSTERMESSAGE_HELLO;
    return true;
}

bool ClusterMessage::HttpEndpoint(uint64_t nodeID_, ReadBuffer endpoint_)
{
    type = CLUSTERMESSAGE_HTTP_ENDPOINT;
    nodeID = nodeID_;
    endpoint = endpoint_;
    return true;
}

bool ClusterMessage::Read(ReadBuffer& buffer)
{
#define READ_SEPARATOR() \
    read = buffer.Readf(":"); \
    if (read != 1) \
        return false; \
    buffer.Advance(read); \

    int             read;
    ReadBuffer      tempBuffer;
        
    if (buffer.GetLength() < 1)
        return false;
    
    switch (buffer.GetCharAt(0))
    {
        case CLUSTERMESSAGE_SET_NODEID:
            read = buffer.Readf("%c:%U:%U",
             &type, &clusterID, &nodeID);
            break;
        case CLUSTERMESSAGE_HEARTBEAT:
            read = buffer.Readf("%c:%U:%u:%u",
             &type, &nodeID, &httpPort, &sdbpPort);
            if (read < 3)
                return false;
            buffer.Advance(read);
            READ_SEPARATOR();
            if (!QuorumInfo::ReadList(buffer, quorumInfos))
                return false;
            READ_SEPARATOR();
            if (!QuorumShardInfo::ReadList(buffer, quorumShardInfos))
                return false;
            return true;
            break;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            type = CLUSTERMESSAGE_SET_CONFIG_STATE;
            return configState.Read(buffer, true);
        case CLUSTERMESSAGE_REQUEST_LEASE:
            read = buffer.Readf("%c:%U:%U:%U:%U:%U:%u",
             &type, &nodeID, &quorumID, &proposalID, &paxosID, &configID, &duration);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            read = buffer.Readf("%c:%U:%U:%U:%U:%u:%b",
             &type, &nodeID, &quorumID, &proposalID, &configID, &duration, &watchingPaxosID);
             if (read < 9)
                return false;
            buffer.Advance(read);
            READ_SEPARATOR();
            if (!MessageUtil::ReadIDList(activeNodes, buffer))
                return false;
            READ_SEPARATOR();
            if (!MessageUtil::ReadIDList(shards, buffer))
                return false;
            return true;
        case CLUSTERMESSAGE_SHARDMIGRATION_INITIATE:
            read = buffer.Readf("%c:%U:%U:%U:%U",
             &type, &nodeID, &quorumID, &srcShardID, &dstShardID);
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_BEGIN:
            read = buffer.Readf("%c:%U:%U:%U",
             &type, &quorumID, &srcShardID, &dstShardID);
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_SET:
            read = buffer.Readf("%c:%U:%U:%#R:%#R",
             &type, &quorumID, &shardID, &key, &value);
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_DELETE:
            read = buffer.Readf("%c:%U:%U:%#R",
             &type, &quorumID, &shardID, &key);
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_COMMIT:
            read = buffer.Readf("%c:%U:%U",
             &type, &quorumID, &shardID);
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_COMPLETE:
            read = buffer.Readf("%c:%U:%U",
             &type, &quorumID, &shardID);
            break;
        case CLUSTERMESSAGE_HELLO:
            read = buffer.Readf("%c:%U:%#R",
             &type, &clusterID, &value);
            break;
        case CLUSTERMESSAGE_HTTP_ENDPOINT:
            read = buffer.Readf("%c:%U:%#R",
             &type, &nodeID, &endpoint);
            break;
        default:
            return false;
    }
    
    return (read == (signed)buffer.GetLength());
    
#undef READ_SEPARATOR
}

bool ClusterMessage::Write(Buffer& buffer)
{
    Buffer          tempBuffer;
    
    switch (type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            buffer.Writef("%c:%U:%U",
             type, clusterID, nodeID);
            return true;
        case CLUSTERMESSAGE_HEARTBEAT:
            buffer.Writef("%c:%U:%u:%u",
             type, nodeID, httpPort, sdbpPort);
            buffer.Appendf(":");
            QuorumInfo::WriteList(buffer, quorumInfos);
            buffer.Appendf(":");
            QuorumShardInfo::WriteList(buffer, quorumShardInfos);
            return true;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            buffer.Clear();
            return configState.Write(buffer, true);
        case CLUSTERMESSAGE_REQUEST_LEASE:
            buffer.Writef("%c:%U:%U:%U:%U:%U:%u",
             type, nodeID, quorumID, proposalID, paxosID, configID, duration);
            return true;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            buffer.Writef("%c:%U:%U:%U:%U:%u:%b",
             type, nodeID, quorumID, proposalID, configID, duration, watchingPaxosID);
            buffer.Appendf(":");
            MessageUtil::WriteIDList(activeNodes, buffer);
            buffer.Appendf(":");
            MessageUtil::WriteIDList(shards, buffer);
            return true;
        case CLUSTERMESSAGE_SHARDMIGRATION_INITIATE:
            buffer.Writef("%c:%U:%U:%U:%U",
             type, nodeID, quorumID, srcShardID, dstShardID);
            return true;
        case CLUSTERMESSAGE_SHARDMIGRATION_BEGIN:
            buffer.Writef("%c:%U:%U:%U",
             type, quorumID, srcShardID, dstShardID);
            return true;
        case CLUSTERMESSAGE_SHARDMIGRATION_SET:
            buffer.Writef("%c:%U:%U:%#R:%#R",
             type, quorumID, shardID, &key, &value);
            return true;
        case CLUSTERMESSAGE_SHARDMIGRATION_DELETE:
            buffer.Writef("%c:%U:%U:%#R",
             type, quorumID, shardID, &key);
            return true;
        case CLUSTERMESSAGE_SHARDMIGRATION_COMMIT:
            buffer.Writef("%c:%U:%U",
             type, quorumID, shardID);
            return true;
        case CLUSTERMESSAGE_SHARDMIGRATION_COMPLETE:
            buffer.Writef("%c:%U:%U",
             type, quorumID, shardID);
            return true;
        case CLUSTERMESSAGE_HELLO:
            tempBuffer.Writef("ScalienDB cluster protocol, server version " VERSION_STRING);
            buffer.Writef("%c:%U:%#B",
             type, clusterID, &tempBuffer);
            return true;
        case CLUSTERMESSAGE_HTTP_ENDPOINT:
            buffer.Writef("%c:%U:%#R",
             type, nodeID, &endpoint);
            return true;
        default:
            return false;
    }
}
