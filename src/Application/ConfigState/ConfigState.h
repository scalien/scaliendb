#ifndef CONFIGSTATE_H
#define CONFIGSTATE_H

#include "System/Common.h"
#include "System/Containers/InList.h"
#include "ConfigQuorum.h"
#include "ConfigDatabase.h"
#include "ConfigTable.h"
#include "ConfigShard.h"
#include "ConfigShardServer.h"
#include "Application/ConfigServer/ConfigMessage.h"

#define CONFIG_MIN_SHARD_NODE_ID    100
#define IS_SHARD_SERVER(nodeID)     ((nodeID) >= CONFIG_MIN_SHARD_NODE_ID)

#define LESS_THAN(a, b) \
    ((b).GetLength() == 0 || ReadBuffer::Cmp((a), (b)) < 0)

#define GREATER_THAN(a, b) \
    (ReadBuffer::Cmp((a), (b)) >= 0)

/*
===============================================================================================

 ConfigState

===============================================================================================
*/

class ConfigState
{
public:
    typedef InList<ConfigQuorum>        QuorumList;
    typedef InList<ConfigDatabase>      DatabaseList;
    typedef InList<ConfigTable>         TableList;
    typedef InList<ConfigShard>         ShardList;
    typedef InList<ConfigShardServer>   ShardServerList;

    // ========================================================================================
    //
    // Not replicated, only stored by the MASTER in-memory
    // 
    bool                hasMaster;
    uint64_t            masterID;
    uint64_t            paxosID;
    
    bool                isSplitting;    

    bool                isMigrating;
    uint64_t            migrateSrcShardID;
    uint64_t            migrateDstShardID;
    uint64_t            migrateQuorumID;
    // ========================================================================================
    
    QuorumList          quorums;
    DatabaseList        databases;
    TableList           tables;
    ShardList           shards;
    ShardServerList     shardServers;
    
    uint64_t            nextQuorumID;
    uint64_t            nextDatabaseID;
    uint64_t            nextTableID;
    uint64_t            nextShardID;
    uint64_t            nextNodeID;
        
    ConfigState();
    ConfigState(const ConfigState& other);
    ~ConfigState();
    
    ConfigState&        operator=(const ConfigState& other);
    
    void                Init();
    void                Transfer(ConfigState& other);
    
    ConfigQuorum*       GetQuorum(uint64_t quorumID);
    ConfigDatabase*     GetDatabase(uint64_t databaseID);
    ConfigDatabase*     GetDatabase(ReadBuffer name);
    ConfigTable*        GetTable(uint64_t tableID);
    ConfigTable*        GetTable(uint64_t databaseID, ReadBuffer name);
    ConfigShard*        GetShard(uint64_t tableID, ReadBuffer key);
    ConfigShard*        GetShard(uint64_t shardID);
    ConfigShardServer*  GetShardServer(uint64_t nodeID);

    bool                CompleteMessage(ConfigMessage& message);
    void                OnMessage(ConfigMessage& message);
    
    bool                Read(ReadBuffer& buffer, bool withVolatile = false);
    bool                Write(Buffer& buffer, bool withVolatile = false);

    void                OnAbortShardMigration();

    template<typename List>
    static bool         ReadIDList(List& numbers, ReadBuffer& buffer);
    
    template<typename List>
    static void         WriteIDList(List& numbers, Buffer& buffer);

private:
    bool                CompleteSetClusterID(ConfigMessage& message);
    bool                CompleteRegisterShardServer(ConfigMessage& message);
    bool                CompleteCreateQuorum(ConfigMessage& message);
    bool                CompleteDeleteQuorum(ConfigMessage& message);
    bool                CompleteAddNode(ConfigMessage& message);
    bool                CompleteRemoveNode(ConfigMessage& message);
    bool                CompleteActivateShardServer(ConfigMessage& message);
    bool                CompleteDeactivateShardServer(ConfigMessage& message);
    bool                CompleteCreateDatabase(ConfigMessage& message);
    bool                CompleteRenameDatabase(ConfigMessage& message);
    bool                CompleteDeleteDatabase(ConfigMessage& message);
    bool                CompleteCreateTable(ConfigMessage& message);
    bool                CompleteRenameTable(ConfigMessage& message);
    bool                CompleteDeleteTable(ConfigMessage& message);
    bool                CompleteFreezeTable(ConfigMessage& message);
    bool                CompleteUnfreezeTable(ConfigMessage& message);
    bool                CompleteTruncateTableBegin(ConfigMessage& message);
    bool                CompleteTruncateTableComplete(ConfigMessage& message);
    bool                CompleteSplitShardBegin(ConfigMessage& message);
    bool                CompleteSplitShardComplete(ConfigMessage& message);
    bool                CompleteShardMigrationBegin(ConfigMessage& message);
    bool                CompleteShardMigrationComplete(ConfigMessage& message);

    void                OnRegisterShardServer(ConfigMessage& message);
    void                OnCreateQuorum(ConfigMessage& message);
    void                OnDeleteQuorum(ConfigMessage& message);
    void                OnAddNode(ConfigMessage& message);
    void                OnRemoveNode(ConfigMessage& message);
    void                OnActivateShardServer(ConfigMessage& message);
    void                OnDeactivateShardServer(ConfigMessage& message);
    void                OnCreateDatabase(ConfigMessage& message);
    void                OnRenameDatabase(ConfigMessage& message);
    void                OnDeleteDatabase(ConfigMessage& message);
    void                OnCreateTable(ConfigMessage& message);
    void                OnRenameTable(ConfigMessage& message);
    void                OnDeleteTable(ConfigMessage& message);
    void                OnFreezeTable(ConfigMessage& message);
    void                OnUnfreezeTable(ConfigMessage& message);
    void                OnTruncateTableBegin(ConfigMessage& message);
    void                OnTruncateTableComplete(ConfigMessage& message);
    void                OnSplitShardBegin(ConfigMessage& message);
    void                OnSplitShardComplete(ConfigMessage& message);
    void                OnShardMigrationBegin(ConfigMessage& message);
    void                OnShardMigrationComplete(ConfigMessage& message);
    
    bool                ReadQuorums(ReadBuffer& buffer, bool withVolatile);
    void                WriteQuorums(Buffer& buffer, bool withVolatile);

    bool                ReadDatabases(ReadBuffer& buffer);
    void                WriteDatabases(Buffer& buffer);

    bool                ReadTables(ReadBuffer& buffer);
    void                WriteTables(Buffer& buffer);

    void                DeleteTable(ConfigTable* table);

    bool                ReadShards(ReadBuffer& buffer, bool withVolatile);
    void                WriteShards(Buffer& buffer, bool withVolatile);

    bool                ReadShardServers(ReadBuffer& buffer, bool withVolatile);
    void                WriteShardServers(Buffer& buffer, bool withVolatile);

    void                DeleteShard(ConfigShard* shard);
    
    bool                ReadQuorum(ConfigQuorum& quorum, ReadBuffer& buffer, bool withVolatile);
    void                WriteQuorum(ConfigQuorum& quorum, Buffer& buffer, bool withVolatile);

    bool                ReadDatabase(ConfigDatabase& database, ReadBuffer& buffer);
    void                WriteDatabase(ConfigDatabase& database, Buffer& buffer);

    bool                ReadTable(ConfigTable& database, ReadBuffer& buffer);
    void                WriteTable(ConfigTable& table, Buffer& buffer);

    bool                ReadShard(ConfigShard& database, ReadBuffer& buffer, bool withVolatile);
    void                WriteShard(ConfigShard& shard, Buffer& buffer, bool withVolatile);

    bool                ReadShardServer(ConfigShardServer& database,
                         ReadBuffer& buffer, bool withVolatile);
    void                WriteShardServer(ConfigShardServer& shardServer,
                         Buffer& buffer, bool withVolatile);
                         
    bool                ReadNextIDs(ReadBuffer& buffer);
    void                WriteNextIDs(Buffer& buffer);
};

#endif
