#ifndef CONFIGSTATE_H
#define CONFIGSTATE_H

#include "System/Common.h"
#include "System/Containers/InList.h"
#include "ConfigQuorum.h"
#include "ConfigDatabase.h"
#include "ConfigTable.h"
#include "ConfigShard.h"
#include "ConfigShardServer.h"
#include "Application/Controller/ConfigMessage.h"

#define CONFIG_MIN_SHARD_NODE_ID    100

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

    template<typename List>
    static bool         ReadIDList(List& numbers, ReadBuffer& buffer);
    
    template<typename List>
    static void         WriteIDList(List& numbers, Buffer& buffer);

private:
    bool                CompleteRegisterShardServer(ConfigMessage& message);
    bool                CompleteCreateQuorum(ConfigMessage& message);
    bool                CompleteIncreaseQuorum(ConfigMessage& message);
    bool                CompleteDecreaseQuorum(ConfigMessage& message);
    bool                CompleteActivateShardServer(ConfigMessage& message);
    bool                CompleteDeactivateShardServer(ConfigMessage& message);
    bool                CompleteCreateDatabase(ConfigMessage& message);
    bool                CompleteRenameDatabase(ConfigMessage& message);
    bool                CompleteDeleteDatabase(ConfigMessage& message);
    bool                CompleteCreateTable(ConfigMessage& message);
    bool                CompleteRenameTable(ConfigMessage& message);
    bool                CompleteDeleteTable(ConfigMessage& message);

    void                OnRegisterShardServer(ConfigMessage& message);
    void                OnCreateQuorum(ConfigMessage& message);
    void                OnIncreaseQuorum(ConfigMessage& message);
    void                OnDecreaseQuorum(ConfigMessage& message);
    void                OnActivateShardServer(ConfigMessage& message);
    void                OnDeactivateShardServer(ConfigMessage& message);
    void                OnCreateDatabase(ConfigMessage& message);
    void                OnRenameDatabase(ConfigMessage& message);
    void                OnDeleteDatabase(ConfigMessage& message);
    void                OnCreateTable(ConfigMessage& message);
    void                OnRenameTable(ConfigMessage& message);
    void                OnDeleteTable(ConfigMessage& message);

    bool                ReadQuorums(ReadBuffer& buffer, bool withVolatile);
    void                WriteQuorums(Buffer& buffer, bool withVolatile);

    bool                ReadDatabases(ReadBuffer& buffer);
    void                WriteDatabases(Buffer& buffer);

    bool                ReadTables(ReadBuffer& buffer);
    void                WriteTables(Buffer& buffer);

    bool                ReadShards(ReadBuffer& buffer);
    void                WriteShards(Buffer& buffer);

    bool                ReadShardServers(ReadBuffer& buffer, bool withVolatile);
    void                WriteShardServers(Buffer& buffer, bool withVolatile);
    
    bool                ReadQuorum(ConfigQuorum& quorum, ReadBuffer& buffer, bool withVolatile);
    void                WriteQuorum(ConfigQuorum& quorum, Buffer& buffer, bool withVolatile);

    bool                ReadDatabase(ConfigDatabase& database, ReadBuffer& buffer);
    void                WriteDatabase(ConfigDatabase& database, Buffer& buffer);

    bool                ReadTable(ConfigTable& database, ReadBuffer& buffer);
    void                WriteTable(ConfigTable& table, Buffer& buffer);

    bool                ReadShard(ConfigShard& database, ReadBuffer& buffer);
    void                WriteShard(ConfigShard& shard, Buffer& buffer);

    bool                ReadShardServer(ConfigShardServer& database,
                         ReadBuffer& buffer, bool withVolatile);
    void                WriteShardServer(ConfigShardServer& shardServer,
                         Buffer& buffer, bool withVolatile);
};

#endif
