#ifndef CONFIGMESSAGE_H
#define CONFIGMESSAGE_H

#include "System/Platform.h"
#include "System/IO/Endpoint.h"
#include "System/Containers/List.h"
#include "Framework/Messaging/Message.h"

#define CONFIGMESSAGE_MAX_NODES                 7

#define CONFIGMESSAGE_SET_CLUSTER_ID            's'
#define CONFIGMESSAGE_REGISTER_SHARDSERVER      'S'
#define CONFIGMESSAGE_CREATE_QUORUM             'Q'
#define CONFIGMESSAGE_DELETE_QUORUM             'W'
#define CONFIGMESSAGE_ADD_NODE                  'n'
#define CONFIGMESSAGE_REMOVE_NODE               'b'
#define CONFIGMESSAGE_ACTIVATE_SHARDSERVER      'p'
#define CONFIGMESSAGE_DEACTIVATE_SHARDSERVER    'm'

#define CONFIGMESSAGE_CREATE_DATABASE           'C'
#define CONFIGMESSAGE_RENAME_DATABASE           'R'
#define CONFIGMESSAGE_DELETE_DATABASE           'D'

#define CONFIGMESSAGE_CREATE_TABLE              'c'
#define CONFIGMESSAGE_RENAME_TABLE              'r'
#define CONFIGMESSAGE_DELETE_TABLE              'd'
#define CONFIGMESSAGE_FREEZE_TABLE              'F'
#define CONFIGMESSAGE_UNFREEZE_TABLE            'f'
#define CONFIGMESSAGE_TRUNCATE_TABLE_BEGIN      't'
#define CONFIGMESSAGE_TRUNCATE_TABLE_COMPLETE   'T'

#define CONFIGMESSAGE_SPLIT_SHARD_BEGIN         '1'
#define CONFIGMESSAGE_SPLIT_SHARD_COMPLETE      '2'

#define CONFIGMESSAGE_SHARD_MIGRATION_COMPLETE  'M'

/*
===============================================================================================

 ConfigMessage

===============================================================================================
*/

class ConfigMessage : public Message
{
public:
    ConfigMessage() { prev = next = this; }

    // Variables
    bool            fromClient;

    char            type;
    uint64_t        clusterID;
    uint64_t        nodeID;
    uint64_t        quorumID;
    uint64_t        databaseID;
    uint64_t        tableID;
    uint64_t        shardID;
    uint64_t        newShardID;
    uint64_t        srcShardID;
    uint64_t        dstShardID;
    ReadBuffer      name;
    ReadBuffer      firstKey;
    Buffer          splitKey;
    Endpoint        endpoint;
    List<uint64_t>  nodes;    
    
    // For InList<>
    ConfigMessage*  prev;
    ConfigMessage*  next;

    // Cluster management
    bool            SetClusterID(
                     uint64_t clusterID);
    bool            RegisterShardServer(
                     uint64_t nodeID, Endpoint& endpoint);
    bool            CreateQuorum(List<uint64_t>& nodes);
    bool            DeleteQuorum(
                     uint64_t quorumID);
    bool            AddNode(
                     uint64_t quorumID, uint64_t nodeID);
    bool            RemoveNode(
                     uint64_t quorumID, uint64_t nodeID);    
    bool            ActivateShardServer(
                     uint64_t quorumID, uint64_t nodeID);
    bool            DeactivateShardServer(
                     uint64_t quorumID, uint64_t nodeID);

    // Database management
    bool            CreateDatabase(
                     ReadBuffer& name);
    bool            RenameDatabase(
                     uint64_t databaseID, ReadBuffer& name);
    bool            DeleteDatabase(
                     uint64_t databaseID);

    // Table management
    bool            CreateTable(
                     uint64_t databaseID, uint64_t quorumID, ReadBuffer& name);
    bool            RenameTable(
                     uint64_t tableID, ReadBuffer& name);
    bool            DeleteTable(
                     uint64_t tableID);
    bool            FreezeTable(
                     uint64_t tableID);
    bool            UnfreezeTable(
                     uint64_t tableID);
    bool            TruncateTableBegin(
                     uint64_t tableID);
    bool            TruncateTableComplete(
                     uint64_t tableID);

    // Shard management
    bool            SplitShardBegin(uint64_t shardID, ReadBuffer& splitKey);
    bool            SplitShardComplete(uint64_t shardID);
    
    bool            ShardMigrationComplete(uint64_t quorumID,
                     uint64_t srcShardID, uint64_t dstShardID);
    
    // Serialization
    bool            Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);
};

#endif
