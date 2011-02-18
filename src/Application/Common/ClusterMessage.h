#ifndef CLUSTERMESSAGE_H
#define CLUSTERMESSAGE_H

#include "System/Common.h"
#include "Framework/Messaging/Message.h"
#include "Application/ConfigState/ConfigState.h"

#define CLUSTERMESSAGE_SET_NODEID               'N' // master => shard server
#define CLUSTERMESSAGE_HEARTBEAT                'H' // shard server => controllers
#define CLUSTERMESSAGE_SET_CONFIG_STATE         'C' // master => shard server
#define CLUSTERMESSAGE_REQUEST_LEASE            'R' // shard server => master, also serves as heartbeat
#define CLUSTERMESSAGE_RECEIVE_LEASE            'r' // master => shard server
#define CLUSTERMESSAGE_CONFIG_CATCHUP           'c'
#define CLUSTERMESSAGE_SHARDMIGRATION_INITIATE  '0' // master => shard server
#define CLUSTERMESSAGE_SHARDMIGRATION_BEGIN     '1' // shard server => shard server
#define CLUSTERMESSAGE_SHARDMIGRATION_SET       '2' // shard server => shard server
#define CLUSTERMESSAGE_SHARDMIGRATION_DELETE    '3' // shard server => shard server
#define CLUSTERMESSAGE_SHARDMIGRATION_COMMIT    '4' // shard server => shard server
#define CLUSTERMESSAGE_SHARDMIGRATION_COMPLETE  '5' // shard server => master

/*
===============================================================================================

 ClusterMessage

===============================================================================================
*/

class ClusterMessage : public Message
{
public:
    char                    type;
    uint64_t                clusterID;
    uint64_t                nodeID;
    uint64_t                quorumID;
    uint64_t                shardID;
    uint64_t                proposalID;
    uint64_t                paxosID;
    uint64_t                configID;
    unsigned                duration;
    bool                    watchingPaxosID;
    List<uint64_t>          activeNodes;
    SortedList<uint64_t>    shards;
    List<QuorumPaxosID>     quorumPaxosIDs;
    List<QuorumShardInfo>   quorumShardInfos;
    ConfigState             configState;
    unsigned                httpPort;
    unsigned                sdbpPort;
    ReadBuffer              key;
    ReadBuffer              value;
    
    ClusterMessage();
    
    bool            SetNodeID(uint64_t clusterID, uint64_t nodeID);
    bool            Heartbeat(uint64_t nodeID,
                     List<QuorumPaxosID>& quorumPaxosIDs, List<QuorumShardInfo>& quorumShardInfos,
                     unsigned httpPort, unsigned sdbpPort);
    bool            SetConfigState(ConfigState& configState);
    bool            RequestLease(uint64_t nodeID, uint64_t quorumID,
                     uint64_t proposalID, uint64_t paxosID, uint64_t configID, unsigned duration);
    bool            ReceiveLease(uint64_t nodeID, uint64_t quorumID,
                     uint64_t proposalID, uint64_t configID, unsigned duration,
                     bool watchingPaxosID, List<uint64_t>& activeNodes,
                     SortedList<uint64_t>& shards);
    bool            ShardMigrationInitiate(uint64_t nodeID, uint64_t quorumID, uint64_t shardID);
    bool            ShardMigrationBegin(uint64_t quorumID, uint64_t shardID);
    bool            ShardMigrationSet(uint64_t shardID, ReadBuffer key, ReadBuffer value);
    bool            ShardMigrationDelete(uint64_t shardID, ReadBuffer key);
    bool            ShardMigrationCommit(uint64_t quorumID, uint64_t shardID);
    bool            ShardMigrationComplete(uint64_t quorumID, uint64_t shardID);
    
    bool            Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);
};

#endif
