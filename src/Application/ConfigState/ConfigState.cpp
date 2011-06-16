#include "ConfigState.h"
#include "System/Macros.h"
#include "Application/Common/DatabaseConsts.h"

#define CONFIG_MESSAGE_PREFIX   'C'

#define READ_SEPARATOR()        \
    read = buffer.Readf(":");   \
    if (read < 1) return false; \
    buffer.Advance(read);

#define CHECK_ADVANCE(n)    \
    if (read < n)           \
        return false;       \
    buffer.Advance(read)

inline bool LessThan(uint64_t a, uint64_t b)
{
    return (a < b);
}

ConfigState::ConfigState()
{
    Init();
}

ConfigState::ConfigState(const ConfigState& other)
{
    *this = other;
}

ConfigState::~ConfigState()
{
    quorums.DeleteList();
    databases.DeleteList();
    tables.DeleteList();
    shards.DeleteList();
    shardServers.DeleteList();
}

ConfigState& ConfigState::operator=(const ConfigState& other)
{
    ConfigQuorum*       quorum;
    ConfigDatabase*     database;
    ConfigTable*        table;
    ConfigShard*        shard;
    ConfigShardServer*  shardServer;
    
    Init();
    
    nextQuorumID = other.nextQuorumID;
    nextDatabaseID = other.nextDatabaseID;
    nextTableID = other.nextTableID;
    nextShardID = other.nextShardID;
    nextNodeID = other.nextNodeID;
    
    hasMaster = other.hasMaster;
    masterID = other.masterID;
    paxosID = other.paxosID;
    
    isSplitting = other.isSplitting;
    
    isMigrating = other.isMigrating;
    migrateQuorumID = other.migrateQuorumID;
    migrateSrcShardID = other.migrateSrcShardID;
    migrateDstShardID = other.migrateDstShardID;
    
    FOREACH (quorum, other.quorums)
        quorums.Append(new ConfigQuorum(*quorum));
    
    FOREACH (database, other.databases)
        databases.Append(new ConfigDatabase(*database));
    
    FOREACH (table, other.tables)
        tables.Append(new ConfigTable(*table));
    
    FOREACH (shard, other.shards)
        shards.Append(new ConfigShard(*shard));
    
    FOREACH (shardServer, other.shardServers)
        shardServers.Append(new ConfigShardServer(*shardServer));
        
    return *this;
}

void ConfigState::Init()
{
    nextQuorumID = 1;
    nextDatabaseID = 1;
    nextTableID = 1;
    nextShardID = 1;
    nextNodeID = CONFIG_MIN_SHARD_NODE_ID;
    
    hasMaster = false;
    masterID = 0;
    paxosID = 0;
    
    isSplitting = false;
    
    isMigrating = false;
    migrateQuorumID = 0;
    migrateSrcShardID = 0;
    migrateDstShardID = 0;
    
    quorums.DeleteList();
    databases.DeleteList();
    tables.DeleteList();
    shards.DeleteList();
    shardServers.DeleteList();
}

void ConfigState::Transfer(ConfigState& other)
{
    other.Init();

    other.nextQuorumID = nextQuorumID;
    other.nextDatabaseID = nextDatabaseID;
    other.nextTableID = nextTableID;
    other.nextShardID = nextShardID;
    other.nextNodeID = nextNodeID;

    other.hasMaster = hasMaster;
    other.masterID = masterID;
    other.paxosID = paxosID;
    
    other.isSplitting = isSplitting;
    
    other.isMigrating = isMigrating;
    other.migrateQuorumID = migrateQuorumID;
    other.migrateSrcShardID = migrateSrcShardID;
    other.migrateDstShardID = migrateDstShardID;
    
    other.quorums = quorums;
    quorums.ClearMembers();
    other.databases = databases;
    databases.ClearMembers();
    other.tables = tables;
    tables.ClearMembers();
    other.shards = shards;
    shards.ClearMembers();
    other.shardServers = shardServers;
    shardServers.ClearMembers();

    Init();
}

bool ConfigState::CompleteMessage(ConfigMessage& message)
{
    switch (message.type)
    {
        /* Cluster management */
        case CONFIGMESSAGE_SET_CLUSTER_ID:
            return CompleteSetClusterID(message);
        case CONFIGMESSAGE_REGISTER_SHARDSERVER:
            return CompleteRegisterShardServer(message);
        case CONFIGMESSAGE_CREATE_QUORUM:
            return CompleteCreateQuorum(message);
        case CONFIGMESSAGE_DELETE_QUORUM:
            return CompleteDeleteQuorum(message);
        case CONFIGMESSAGE_ADD_NODE:
            return CompleteAddNode(message);
        case CONFIGMESSAGE_REMOVE_NODE:
            return CompleteRemoveNode(message);
        case CONFIGMESSAGE_ACTIVATE_SHARDSERVER:
            return CompleteActivateShardServer(message);
        case CONFIGMESSAGE_DEACTIVATE_SHARDSERVER:
            return CompleteDeactivateShardServer(message);

        /* Database management */
        case CONFIGMESSAGE_CREATE_DATABASE:
            return CompleteCreateDatabase(message);
        case CONFIGMESSAGE_RENAME_DATABASE:
            return CompleteRenameDatabase(message);
        case CONFIGMESSAGE_DELETE_DATABASE:
            return CompleteDeleteDatabase(message);

        case CONFIGMESSAGE_SPLIT_SHARD_BEGIN:
            return CompleteSplitShardBegin(message);
        case CONFIGMESSAGE_SPLIT_SHARD_COMPLETE:
            return CompleteSplitShardComplete(message);

        case CONFIGMESSAGE_SHARD_MIGRATION_COMPLETE:
            return CompleteShardMigrationComplete(message);

        /* Table management */
        case CONFIGMESSAGE_CREATE_TABLE:
            return CompleteCreateTable(message);
        case CONFIGMESSAGE_RENAME_TABLE:
            return CompleteRenameTable(message);
        case CONFIGMESSAGE_DELETE_TABLE:
            return CompleteDeleteTable(message);
        case CONFIGMESSAGE_FREEZE_TABLE:
            return CompleteFreezeTable(message);
        case CONFIGMESSAGE_UNFREEZE_TABLE:
            return CompleteUnfreezeTable(message);
        case CONFIGMESSAGE_TRUNCATE_TABLE_BEGIN:
            return CompleteTruncateTableBegin(message);
        case CONFIGMESSAGE_TRUNCATE_TABLE_COMPLETE:
            return CompleteTruncateTableComplete(message);
        
        default:
            ASSERT_FAIL();
			return false;
    }
}

bool ConfigState::Read(ReadBuffer& buffer_, bool withVolatile)
{
    char        c;
    int         read;
    ReadBuffer  buffer;
    
    buffer = buffer_; // because of Advance()

    Init();

    hasMaster = false;
    if (withVolatile)
    {
        // HACK: in volatile mode the prefix is handled by ConfigState
        // TODO: change convention to Append in every Message::Write
        read = buffer.Readf("%c", &c);
        CHECK_ADVANCE(1);
        ASSERT(c == CONFIG_MESSAGE_PREFIX);

        READ_SEPARATOR();
        c = NO;
        read = buffer.Readf("%c", &c);
        CHECK_ADVANCE(1);
        if (c == YES)
        {
            READ_SEPARATOR();
            read = buffer.Readf("%U", &masterID);
            CHECK_ADVANCE(1);
            hasMaster = true;

            // paxosID is optional from version 0.9.8
            read = buffer.Readf(":P%U", &paxosID);
            if (read > 0)
            {
                CHECK_ADVANCE(3);
            }
        }
        
        READ_SEPARATOR();
        c = NO;
        read = buffer.Readf("%c", &c);
        CHECK_ADVANCE(1);
        if (c == YES)
        {
            READ_SEPARATOR();
            read = buffer.Readf("%U:%U:%U", &migrateSrcShardID, &migrateDstShardID, &migrateQuorumID);
            CHECK_ADVANCE(1);
            isMigrating = true;
        }

        READ_SEPARATOR();
    }
    
    if (!ReadNextIDs(buffer))
        return false;
    READ_SEPARATOR();
    if (!ReadQuorums(buffer, withVolatile))
        return false;
    READ_SEPARATOR();
    if (!ReadDatabases(buffer))
        return false;
    READ_SEPARATOR();
    if (!ReadTables(buffer))
        return false;
    READ_SEPARATOR();
    if (!ReadShards(buffer, withVolatile))
        return false;
    READ_SEPARATOR();
    if (!ReadShardServers(buffer, withVolatile))
        return false;

    return (buffer.GetLength() == 0);
}

bool ConfigState::Write(Buffer& buffer, bool withVolatile)
{
    if (withVolatile)
    {
        // HACK: in volatile mode the prefix is handled by ConfigState
        buffer.Appendf("%c", CONFIG_MESSAGE_PREFIX);

        buffer.Appendf(":");
        if (hasMaster)
            buffer.Appendf("%c:%U", YES, masterID);
        else
            buffer.Appendf("%c", NO);

        // as paxosID is optional from 0.9.8, it is prefixed with P
        buffer.Appendf(":P%U", paxosID);

        buffer.Appendf(":");        
        if (isMigrating)
            buffer.Appendf("%c:%U:%U:%U", YES, migrateSrcShardID, migrateDstShardID, migrateQuorumID);
        else
            buffer.Appendf("%c", NO);

        buffer.Appendf(":");        
    }

    WriteNextIDs(buffer);
    buffer.Appendf(":");
    WriteQuorums(buffer, withVolatile);
    buffer.Appendf(":");
    WriteDatabases(buffer);
    buffer.Appendf(":");
    WriteTables(buffer);
    buffer.Appendf(":");
    WriteShards(buffer, withVolatile);
    buffer.Appendf(":");
    WriteShardServers(buffer, withVolatile);
    
    //Log_Trace("buffer = %B", &buffer);
    
    return true;
}

void ConfigState::OnAbortShardMigration()
{
    ConfigShardServer*  shardServer;
    QuorumShardInfo*    info;

    isMigrating = false;
    migrateQuorumID = 0;
    migrateSrcShardID = 0;
    migrateDstShardID = 0;
    
    FOREACH (shardServer, shardServers)
    {
        FOREACH (info, shardServer->quorumShardInfos)
        {
            info->isSendingShard = false;
            info->migrationNodeID = 0;
            info->migrationQuorumID = 0;
            info->migrationBytesSent = 0;
            info->migrationBytesTotal = 0;
            info->migrationThroughput = 0;
        }
    }
}

void ConfigState::OnMessage(ConfigMessage& message)
{
    switch (message.type)
    {
        /* Cluster management */
        case CONFIGMESSAGE_REGISTER_SHARDSERVER:
            return OnRegisterShardServer(message);
        case CONFIGMESSAGE_CREATE_QUORUM:
            return OnCreateQuorum(message);
        case CONFIGMESSAGE_DELETE_QUORUM:
            return OnDeleteQuorum(message);
        case CONFIGMESSAGE_ADD_NODE:
            return OnAddNode(message);
        case CONFIGMESSAGE_REMOVE_NODE:
            return OnRemoveNode(message);
        case CONFIGMESSAGE_ACTIVATE_SHARDSERVER:
            return OnActivateShardServer(message);
        case CONFIGMESSAGE_DEACTIVATE_SHARDSERVER:
            return OnDeactivateShardServer(message);

        /* Database management */
        case CONFIGMESSAGE_CREATE_DATABASE:
            return OnCreateDatabase(message);
        case CONFIGMESSAGE_RENAME_DATABASE:
            return OnRenameDatabase(message);
        case CONFIGMESSAGE_DELETE_DATABASE:
            return OnDeleteDatabase(message);

        /* Table management */
        case CONFIGMESSAGE_CREATE_TABLE:
            return OnCreateTable(message);
        case CONFIGMESSAGE_RENAME_TABLE:
            return OnRenameTable(message);
        case CONFIGMESSAGE_DELETE_TABLE:
            return OnDeleteTable(message);
        case CONFIGMESSAGE_FREEZE_TABLE:
            return OnFreezeTable(message);
        case CONFIGMESSAGE_UNFREEZE_TABLE:
            return OnUnfreezeTable(message);
        case CONFIGMESSAGE_TRUNCATE_TABLE_BEGIN:
            return OnTruncateTableBegin(message);
        case CONFIGMESSAGE_TRUNCATE_TABLE_COMPLETE:
            return OnTruncateTableComplete(message);
        
        case CONFIGMESSAGE_SPLIT_SHARD_BEGIN:
            return OnSplitShardBegin(message);
        case CONFIGMESSAGE_SPLIT_SHARD_COMPLETE:
            return OnSplitShardComplete(message);
        case CONFIGMESSAGE_SHARD_MIGRATION_COMPLETE:
            return OnShardMigrationComplete(message);
     
        case CONFIGMESSAGE_SET_CLUSTER_ID:
            return;
              
        default:
            ASSERT_FAIL(); 
    }
}

ConfigQuorum* ConfigState::GetQuorum(uint64_t quorumID)
{
    ConfigQuorum* itQuorum;
    
    FOREACH (itQuorum, quorums)
    {
        if (itQuorum->quorumID == quorumID)
            return itQuorum;
    }
    
    return NULL;
}

ConfigDatabase* ConfigState::GetDatabase(uint64_t databaseID)
{
    ConfigDatabase* itDatabase;
    
    FOREACH (itDatabase, databases)
    {
        if (itDatabase->databaseID == databaseID)
            return itDatabase;
    }
    
    return NULL;
}

ConfigDatabase* ConfigState::GetDatabase(ReadBuffer name)
{
    ConfigDatabase* itDatabase;
    
    FOREACH (itDatabase, databases)
    {
        if (BUFCMP(&itDatabase->name, &name))
            return itDatabase;
    }
    
    return NULL;
}

ConfigTable* ConfigState::GetTable(uint64_t tableID)
{
    ConfigTable* itTable;
    
    FOREACH (itTable, tables)
    {
        if (itTable->tableID == tableID)
            return itTable;
    }
    
    return NULL;
}

ConfigTable* ConfigState::GetTable(uint64_t databaseID, ReadBuffer name)
{
    ConfigTable* itTable;
    
    FOREACH (itTable, tables)
    {
        if (itTable->databaseID == databaseID && BUFCMP(&itTable->name, &name))
            return itTable;
    }
    
    return NULL;
}

ConfigShard* ConfigState::GetShard(uint64_t tableID, ReadBuffer key)
{
    ConfigShard* itShard;
    
    FOREACH (itShard, shards)
    {
//        if (itShard->state == CONFIG_SHARD_STATE_SPLIT_CREATING)
//            continue;
        
        if (itShard->tableID == tableID)
        {
            if (GREATER_THAN(key, itShard->firstKey) && LESS_THAN(key, itShard->lastKey))
                return itShard;
        }
    }
    
    return NULL;
}

ConfigShard* ConfigState::GetShard(uint64_t shardID)
{
    ConfigShard* itShard;
    
    FOREACH (itShard, shards)
    {
        if (itShard->shardID == shardID)
            return itShard;
    }
    
    return NULL;
}

ConfigShardServer* ConfigState::GetShardServer(uint64_t nodeID)
{
    ConfigShardServer* itShardServer;
    
    FOREACH (itShardServer, shardServers)
    {
        if (itShardServer->nodeID == nodeID)
            return itShardServer;
    }
    
    return NULL;
}

bool ConfigState::CompleteSetClusterID(ConfigMessage& )
{
    return true;
}

bool ConfigState::CompleteRegisterShardServer(ConfigMessage&)
{
    return true;
}

bool ConfigState::CompleteCreateQuorum(ConfigMessage& message)
{
    uint64_t* itNodeID;
    
    FOREACH (itNodeID, message.nodes)
    {
        if (GetShardServer(*itNodeID) == NULL)
            return false; // no such shard server
    }

    return true;
}

bool ConfigState::CompleteDeleteQuorum(ConfigMessage& message)
{
    ConfigQuorum*   quorum;

    quorum = GetQuorum(message.quorumID);
    if (quorum == NULL)
        return false; // no such quorum

    if (quorum->shards.GetLength() > 0)
        return false; // quorum contains shards

    return true;
}

bool ConfigState::CompleteAddNode(ConfigMessage& message)
{
    ConfigQuorum* quorum;
    
    quorum = GetQuorum(message.quorumID);
    if (quorum == NULL)
        return false; // no such quorum
        
    if (quorum->IsMember(message.nodeID))
        return false;
    
    return true;
}

bool ConfigState::CompleteRemoveNode(ConfigMessage& message)
{
    ConfigQuorum*   quorum;
    bool            activeNode;
    bool            inactiveNode;
    
    quorum = GetQuorum(message.quorumID);
    if (quorum == NULL)
        return false; // no such quorum
        
    activeNode = quorum->IsActiveMember(message.nodeID);
    inactiveNode = quorum->IsInactiveMember(message.nodeID);

    if (!activeNode && !inactiveNode)
        return false;

    if (inactiveNode)
        return true;

    if (activeNode)
    {
        if (quorum->activeNodes.GetLength() <= 1)
            return false; // can't remove last active node
        else
            return true;
    }

    return false; // dummy
}

bool ConfigState::CompleteActivateShardServer(ConfigMessage& message)
{
    ConfigQuorum* quorum;
    
    quorum = GetQuorum(message.quorumID);
    if (quorum == NULL)
        return false; // no such quorum

    if (quorum->IsInactiveMember(message.nodeID))
        return true;

    return false;
}

bool ConfigState::CompleteDeactivateShardServer(ConfigMessage& message)
{
    ConfigQuorum* quorum;
    
    quorum = GetQuorum(message.quorumID);
    if (quorum == NULL)
        return false; // no such quorum

    if (quorum->IsActiveMember(message.nodeID))
    {
        if (quorum->activeNodes.GetLength() == 1)
            return false;

        return true;
    }

    return false;
}

bool ConfigState::CompleteCreateDatabase(ConfigMessage& message)
{
    ConfigDatabase* database;
    
    if (message.name.GetLength() > DATABASE_NAME_SIZE)
        return false;
        
    if (!message.name.IsAsciiPrintable())
        return false;
    
    database = GetDatabase(message.name);
    if (database != NULL)
        return false; // database with name exists

    return true;
}

bool ConfigState::CompleteRenameDatabase(ConfigMessage& message)
{
    ConfigDatabase* database;
    
    if (message.name.GetLength() > DATABASE_NAME_SIZE)
        return false;

    database = GetDatabase(message.databaseID);
    if (database == NULL)
        return false; // no such database

    database = GetDatabase(message.name);
    if (database != NULL)
        return false; // database with name exists

    if (!message.name.IsAsciiPrintable())
        return false;

    return true;
}

bool ConfigState::CompleteDeleteDatabase(ConfigMessage& message)
{
    ConfigDatabase* database;
    
    database = GetDatabase(message.databaseID);
    if (database == NULL)
        return false; // no such database

    return true;
}

bool ConfigState::CompleteCreateTable(ConfigMessage& message)
{
    ConfigQuorum*   quorum;
    ConfigDatabase* database;
    ConfigTable*    table;
    
    if (message.name.GetLength() > DATABASE_NAME_SIZE)
        return false;

    quorum = GetQuorum(message.quorumID); 
    if (quorum == NULL)
        return false; // no such quorum

    database = GetDatabase(message.databaseID);
    if (database == NULL)
        return false; // no such database

    table = GetTable(message.databaseID, message.name);
    if (table != NULL)
        return false; // table with name exists in database

    if (!message.name.IsAsciiPrintable())
        return false;

    return true;
}

bool ConfigState::CompleteRenameTable(ConfigMessage& message)
{
    ConfigTable*    table;

    if (message.name.GetLength() > DATABASE_NAME_SIZE)
        return false;

    table = GetTable(message.tableID);
    if (table == NULL)
        return false; // no such table
    table = GetTable(table->databaseID, message.name);
    if (table != NULL)
        return false; // table with name exists in database

    if (!message.name.IsAsciiPrintable())
        return false;

    return true;
}

bool ConfigState::CompleteDeleteTable(ConfigMessage& message)
{
    ConfigTable*    table;

    table = GetTable(message.tableID);
    if (table == NULL)
        return false; // no such table

    return true;
}

bool ConfigState::CompleteFreezeTable(ConfigMessage& message)
{
    ConfigTable*    table;

    table = GetTable(message.tableID);
    if (table == NULL)
        return false; // no such table

    return true;
}

bool ConfigState::CompleteUnfreezeTable(ConfigMessage& message)
{
    ConfigTable*    table;

    table = GetTable(message.tableID);
    if (table == NULL)
        return false; // no such table

    return true;
}

bool ConfigState::CompleteTruncateTableBegin(ConfigMessage& message)
{
    ConfigTable*    table;

    table = GetTable(message.tableID);
    if (table == NULL)
        return false; // no such table

    return true;
}

bool ConfigState::CompleteTruncateTableComplete(ConfigMessage&)
{
    return true;
}

bool ConfigState::CompleteSplitShardBegin(ConfigMessage& message)
{
    ConfigShard* shard;
    
    shard = GetShard(message.shardID);
    
    if (shard == NULL)
        return false;
    
    if (!RangeContains(shard->firstKey, shard->lastKey, message.splitKey))
        return false;
    
    if (ReadBuffer::Cmp(shard->firstKey, message.splitKey) == 0)
        return false;

    if (ReadBuffer::Cmp(shard->lastKey, message.splitKey) == 0)
        return false;
    
    if (isSplitting)
        return false;
    
    isSplitting = true;

    return true;
}

bool ConfigState::CompleteSplitShardComplete(ConfigMessage& message)
{
    ConfigShard* shard;
    
    shard = GetShard(message.shardID);
    
    if (shard->state == CONFIG_SHARD_STATE_SPLIT_CREATING)
        return true;
    
    return false;
}

bool ConfigState::CompleteShardMigrationComplete(ConfigMessage& message)
{
    if (!isMigrating)
        return false;
        
    if (migrateQuorumID != message.quorumID)
        return false;
    
    if (migrateSrcShardID != message.srcShardID)
        return false;
    
    return true;
}

void ConfigState::OnRegisterShardServer(ConfigMessage& message)
{
    ConfigShardServer* shardServer;
    
    shardServer = GetShardServer(message.nodeID);
    if (shardServer == NULL)
    {
        // new node
        shardServer = new ConfigShardServer;
        shardServer->nodeID = nextNodeID++;
        shardServer->endpoint = message.endpoint;
        
        shardServers.Append(shardServer); 
        
        message.nodeID = shardServer->nodeID;
    }
    else
    {
        shardServer->endpoint = message.endpoint;
    }
}

void ConfigState::OnCreateQuorum(ConfigMessage& message)
{
    uint64_t*       it;
    ConfigQuorum*   quorum;
    
    quorum = new ConfigQuorum;
    quorum->quorumID = nextQuorumID++;
    quorum->activeNodes.Clear();
    FOREACH (it, message.nodes)
        quorum->activeNodes.Add(*it);
    
    quorums.Append(quorum);
    
    message.quorumID = quorum->quorumID;
}

void ConfigState::OnDeleteQuorum(ConfigMessage& message)
{
    ConfigQuorum* quorum;
    
    // make sure quorum exists
    quorum = GetQuorum(message.quorumID);
    
    quorums.Remove(quorum);
    
    delete quorum;
}

void ConfigState::OnAddNode(ConfigMessage& message)
{
    ConfigQuorum* quorum;
    
    quorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    ASSERT(quorum != NULL);
        
    // make sure node is not already in quorum
    if (quorum->IsMember(message.nodeID))
        ASSERT_FAIL();

    quorum->inactiveNodes.Add(message.nodeID);
}

void ConfigState::OnRemoveNode(ConfigMessage& message)
{
    ConfigQuorum* quorum;
    
    quorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    ASSERT(quorum != NULL);
    
    if (quorum->IsActiveMember(message.nodeID))
    {
        quorum->activeNodes.Remove(message.nodeID);
        return;
    }
    
    if (quorum->IsInactiveMember(message.nodeID))
    {
        quorum->inactiveNodes.Remove(message.nodeID);
        return;
    }
    
    ASSERT_FAIL();
}

void ConfigState::OnActivateShardServer(ConfigMessage& message)
{
    ConfigQuorum*       quorum;
    ConfigShardServer*  shardServer;
    
    quorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    ASSERT(quorum != NULL);

    if (quorum->IsInactiveMember(message.nodeID))
    {
        quorum->inactiveNodes.Remove(message.nodeID);
        quorum->activeNodes.Add(message.nodeID);

        if (quorum->isActivatingNode)
        {
            shardServer = GetShardServer(quorum->activatingNodeID);
            ASSERT(shardServer != NULL);
            
            Log_Message("Activated shard server %U in quorum %U ",
             quorum->activatingNodeID, quorum->quorumID);

            quorum->ClearActivation();
        }
        return;
    }

    ASSERT_FAIL();
}

void ConfigState::OnDeactivateShardServer(ConfigMessage& message)
{
    ConfigQuorum* quorum;
    
    quorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    ASSERT(quorum != NULL);
    
    if (!quorum->IsActiveMember(message.nodeID))
        ASSERT_FAIL();

    quorum->activeNodes.Remove(message.nodeID);
    quorum->inactiveNodes.Add(message.nodeID);

    Log_Message("Deactivating shard server %U in quorum %U...", message.nodeID, message.quorumID);

}

void ConfigState::OnCreateDatabase(ConfigMessage& message)
{
    ConfigDatabase* database;
    
    database = GetDatabase(message.name);
    // make sure database with name does not exist
    ASSERT(database == NULL);
        
    database = new ConfigDatabase;

    database->databaseID = nextDatabaseID++;
    database->name.Write(message.name);
    databases.Append(database);
    
    message.databaseID = database->databaseID;
}

void ConfigState::OnRenameDatabase(ConfigMessage& message)
{
    ConfigDatabase* database;

    database = GetDatabase(message.name);
    // make sure database with name does not exist
    ASSERT(database == NULL);

    database = GetDatabase(message.databaseID);
    // make sure database with ID exists
    ASSERT(database != NULL);
    
    database->name.Write(message.name);
}

void ConfigState::OnDeleteDatabase(ConfigMessage& message)
{
    ConfigDatabase* database;
    ConfigTable*    table;
    uint64_t*       itTableID;
    
    database = GetDatabase(message.databaseID);
    // make sure database with ID exists
    ASSERT(database != NULL);

    FOREACH (itTableID, database->tables)
    {
        table = GetTable(*itTableID);
        DeleteTable(table);
    }

    databases.Delete(database);
}

void ConfigState::OnCreateTable(ConfigMessage& message)
{
    ConfigQuorum*   quorum;
    ConfigDatabase* database;
    ConfigTable*    table;
    ConfigShard*    shard;

    quorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    ASSERT(quorum != NULL);

    database = GetDatabase(message.databaseID);
    // make sure database exists
    ASSERT(database != NULL);

    table = GetTable(nextTableID);
    // make sure table with ID does not exist
    ASSERT(table == NULL);
    table = GetTable(message.databaseID, message.name);
    // make sure table with name in database does not exist
    ASSERT(table == NULL);

    shard = new ConfigShard;
    shard->quorumID = message.quorumID;
    shard->tableID = nextTableID;
    shard->shardID = nextShardID++;
    shards.Append(shard);

    table = new ConfigTable;
    table->databaseID = message.databaseID;
    table->tableID = nextTableID++;
    table->isFrozen = false; // by default all tables are UNfrozen
    table->name.Write(message.name);
    table->shards.Append(shard->shardID);
    tables.Append(table);
    
    database->tables.Append(table->tableID);
    quorum->shards.Add(shard->shardID);

    message.tableID = table->tableID;
    message.shardID = shard->shardID;
}

void ConfigState::OnRenameTable(ConfigMessage& message)
{
    ConfigTable*    table;

    table = GetTable(message.tableID);
    // make sure table with ID exists
    ASSERT(table != NULL);
    
    table = GetTable(table->databaseID, message.name);
    // make sure table with name does not exist
    ASSERT(table == NULL);

    table = GetTable(message.tableID);
    // make sure table with ID exists
    ASSERT(table != NULL);
    
    table->name.Write(message.name);
}

void ConfigState::OnDeleteTable(ConfigMessage& message)
{
    ConfigDatabase* database;
    ConfigTable*    table;
    
    table = GetTable(message.tableID);
    // make sure table exists
    ASSERT(table != NULL);

    database = GetDatabase(table->databaseID);
    // make sure database exists
    ASSERT(database != NULL);
    
    DeleteTable(table);
    database->tables.Remove(message.tableID);
}

void ConfigState::OnFreezeTable(ConfigMessage& message)
{
    ConfigTable*    table;

    table = GetTable(message.tableID);
    // make sure table exists
    ASSERT(table != NULL);
    
    table->isFrozen = true;
}

void ConfigState::OnUnfreezeTable(ConfigMessage& message)
{
    ConfigTable*    table;

    table = GetTable(message.tableID);
    // make sure table exists
    ASSERT(table != NULL);
    
    table->isFrozen = false;
}

void ConfigState::OnTruncateTableBegin(ConfigMessage& message)
{
    uint64_t*       itShardID;
    ConfigQuorum*   quorum;
    ConfigDatabase* database;
    ConfigTable*    table;
    ConfigShard*    shard;
    uint64_t        quorumID;
    
    table = GetTable(message.tableID);
    // make sure table exists
    ASSERT(table != NULL);

    database = GetDatabase(table->databaseID);
    // make sure database exists
    ASSERT(database != NULL);

    quorumID = 0;
    // delete all old shards
    FOREACH_FIRST (itShardID, table->shards)
    {
        shard = GetShard(*itShardID);
        ASSERT(shard != NULL);
        quorumID = shard->quorumID;
        DeleteShard(shard);
    }
    
    quorum = GetQuorum(quorumID);
    ASSERT(quorum != NULL);

    // create a new shard
    shard = new ConfigShard;
    shard->state = CONFIG_SHARD_STATE_TRUNC_CREATING;
    shard->shardID = nextShardID++;
    shard->quorumID = quorumID;
    shard->tableID = table->tableID;

    table->shards.Append(shard->shardID);
    shards.Append(shard);
    quorum->shards.Add(shard->shardID);
}

void ConfigState::OnTruncateTableComplete(ConfigMessage& message)
{
    ConfigShard*    shard;
    ConfigTable*    table;

    table = GetTable(message.tableID);
    // make sure table exists
    ASSERT(table != NULL);
    ASSERT(table->shards.GetLength() == 1);
    
    shard = GetShard(*(table->shards.First()));
    ASSERT(shard->state == CONFIG_SHARD_STATE_TRUNC_CREATING);

    shard->state = CONFIG_SHARD_STATE_NORMAL;
}

void ConfigState::OnSplitShardBegin(ConfigMessage& message)
{
    ConfigShard*    newShard;
    ConfigShard*    parentShard;
    ConfigQuorum*   quorum;
    ConfigTable*    table;
    
    parentShard = GetShard(message.shardID);

    if (!parentShard)
    {
        // table has been deleted or truncated
        isSplitting = false;
        return;
    }
    
    newShard = new ConfigShard;
    newShard->state = CONFIG_SHARD_STATE_SPLIT_CREATING;
    newShard->shardID = nextShardID++;
    newShard->parentShardID = message.shardID;
    newShard->quorumID = parentShard->quorumID;
    newShard->tableID = parentShard->tableID;
    newShard->firstKey.Write(message.splitKey);
    newShard->lastKey.Write(parentShard->lastKey);
    shards.Append(newShard);
    
    parentShard->lastKey.Write(newShard->firstKey);

    quorum = GetQuorum(newShard->quorumID);
    quorum->shards.Add(newShard->shardID);
    
    table = GetTable(parentShard->tableID);
    table->shards.Add(newShard->shardID);

    message.newShardID = newShard->shardID;
}

void ConfigState::OnSplitShardComplete(ConfigMessage& message)
{
    ConfigShard* shard;
    
    shard = GetShard(message.shardID);
    ASSERT(shard != NULL);
    shard->state = CONFIG_SHARD_STATE_NORMAL;
}

void ConfigState::OnShardMigrationComplete(ConfigMessage& message)
{
    ConfigQuorum*   quorum;
    ConfigShard*    shard;
    ConfigTable*    table;
    
    shard = GetShard(message.srcShardID);
    ASSERT(shard != NULL);
    // quorum = old quorum
    quorum = GetQuorum(shard->quorumID);
    ASSERT(quorum != NULL);
    quorum->shards.Remove(shard->shardID);
    // table
    table = GetTable(shard->tableID);
    ASSERT(table != NULL);
    table->shards.Remove(message.srcShardID);
    table->shards.Add(message.dstShardID);
    // change shard ID
    shard->shardID = message.dstShardID;
    // quorum = new quorum
    quorum = GetQuorum(message.quorumID);
    quorum->shards.Add(shard->shardID);
    shard->quorumID = message.quorumID;
}

bool ConfigState::ReadQuorums(ReadBuffer& buffer, bool withVolatile)
{
    int             read;
    unsigned        num, i;
    ConfigQuorum*   quorum;
    
    read = buffer.Readf("%u", &num);
    CHECK_ADVANCE(1);
    
    for (i = 0; i < num; i++)
    {
        READ_SEPARATOR();
        quorum = new ConfigQuorum;
        if (!ReadQuorum(*quorum, buffer, withVolatile))
        {
            delete quorum;
            return false;
        }
        quorums.Append(quorum);
    }
    
    return true;
}

void ConfigState::WriteQuorums(Buffer& buffer, bool withVolatile)
{
    ConfigQuorum* itQuorum;

    // write number of quorums
    buffer.Appendf("%u", quorums.GetLength());

    FOREACH (itQuorum, quorums)
    {
        buffer.Appendf(":");
        WriteQuorum(*itQuorum, buffer, withVolatile);
    }
}

bool ConfigState::ReadDatabases(ReadBuffer& buffer)
{
    int             read;
    unsigned        num, i;
    ConfigDatabase* database;
    
    read = buffer.Readf("%u", &num);
    CHECK_ADVANCE(1);
    
    for (i = 0; i < num; i++)
    {
        READ_SEPARATOR();
        database = new ConfigDatabase;
        if (!ReadDatabase(*database, buffer))
        {
            delete database;
            return false;
        }
        databases.Append(database);
    }
    
    return true;
}

void ConfigState::WriteDatabases(Buffer& buffer)
{
    ConfigDatabase* it;
    
    // write number of databases
    buffer.Appendf("%u", databases.GetLength());
    
    for (it = databases.First(); it != NULL; it = databases.Next(it))
    {
        buffer.Appendf(":");
        WriteDatabase(*it, buffer);
    }
}

bool ConfigState::ReadTables(ReadBuffer& buffer)
{
    int             read;
    unsigned        num, i;
    ConfigTable*    table;
    
    read = buffer.Readf("%u", &num);
    CHECK_ADVANCE(1);
    
    for (i = 0; i < num; i++)
    {
        READ_SEPARATOR();
        table = new ConfigTable;
        if (!ReadTable(*table, buffer))
        {
            delete table;
            return false;
        }
        tables.Append(table);
    }
    
    return true;
}

void ConfigState::WriteTables(Buffer& buffer)
{
    ConfigTable* itTable;
    
    // write number of databases
    buffer.Appendf("%u", tables.GetLength());
    
    FOREACH (itTable, tables)
    {
        buffer.Appendf(":");
        WriteTable(*itTable, buffer);
    }
}

void ConfigState::DeleteTable(ConfigTable* table)
{
    ConfigShard*    shard;
    uint64_t*       itShardID;

    FOREACH_FIRST (itShardID, table->shards)
    {
        shard = GetShard(*itShardID);
        ASSERT(shard != NULL);
        DeleteShard(shard);
    }

    tables.Delete(table);
}

bool ConfigState::ReadShards(ReadBuffer& buffer, bool withVolatile)
{
    int             read;
    unsigned        num, i;
    ConfigShard*    shard;
    
    read = buffer.Readf("%u", &num);
    CHECK_ADVANCE(1);
    
    for (i = 0; i < num; i++)
    {
        READ_SEPARATOR();
        shard = new ConfigShard;
        if (!ReadShard(*shard, buffer, withVolatile))
        {
            delete shard;
            return false;
        }
        shards.Append(shard);
    }
    
    return true;
}

void ConfigState::WriteShards(Buffer& buffer, bool withVolatile)
{
    ConfigShard* itShard;
    
    // write number of databases
    buffer.Appendf("%u", shards.GetLength());
    
    FOREACH (itShard, shards)
    {
        buffer.Appendf(":");
        WriteShard(*itShard, buffer, withVolatile);
    }
}

bool ConfigState::ReadShardServers(ReadBuffer& buffer, bool withVolatile)
{
    int                 read;
    unsigned            num, i;
    ConfigShardServer*  shardServer;
    
    read = buffer.Readf("%u", &num);
    CHECK_ADVANCE(1);
    
    for (i = 0; i < num; i++)
    {
        READ_SEPARATOR();
        shardServer = new ConfigShardServer;
        if (!ReadShardServer(*shardServer, buffer, withVolatile))
        {
            delete shardServer;
            return false;
        }
        shardServers.Append(shardServer);
    }
    
    return true;
}

void ConfigState::WriteShardServers(Buffer& buffer, bool withVolatile)
{
    ConfigShardServer* itShardServer;
    
    // write number of databases
    buffer.Appendf("%u", shardServers.GetLength());
    
    FOREACH (itShardServer, shardServers)
    {
        buffer.Appendf(":");
        WriteShardServer(*itShardServer, buffer, withVolatile);
    }
}

void ConfigState::DeleteShard(ConfigShard* shard)
{
    ConfigQuorum*   quorum;
    ConfigTable*    table;

    quorum = GetQuorum(shard->quorumID);
    ASSERT(quorum != NULL);
    quorum->shards.Remove(shard->shardID);
    table = GetTable(shard->tableID);
    ASSERT(table != NULL);
    table->shards.Remove(shard->shardID);

    shards.Delete(shard);
}

bool ConfigState::ReadQuorum(ConfigQuorum& quorum, ReadBuffer& buffer, bool withVolatile)
{
    int     read;
    char    c;
    
    read = buffer.Readf("%U", &quorum.quorumID);
    CHECK_ADVANCE(1);
    READ_SEPARATOR();
    if (!ReadIDList(quorum.activeNodes, buffer))
        return false;
    READ_SEPARATOR();
    if (!ReadIDList(quorum.inactiveNodes, buffer))
        return false;
    READ_SEPARATOR();
    if (!ReadIDList(quorum.shards, buffer))
        return false;
    
    quorum.hasPrimary = false;
    quorum.isActivatingNode = false;
    if (withVolatile)
    {
        READ_SEPARATOR();
        read = buffer.Readf("%U", &quorum.configID);
        CHECK_ADVANCE(1);
        READ_SEPARATOR();
        
        c = NO;
        read = buffer.Readf("%c", &c);
        CHECK_ADVANCE(1);
        if (c == YES)
        {
            READ_SEPARATOR();
            read = buffer.Readf("%U", &quorum.activatingNodeID);
            CHECK_ADVANCE(1);
            quorum.isActivatingNode = true;
        }
        
        read = buffer.Readf(":%U:", &quorum.paxosID);
        CHECK_ADVANCE(3);
        c = NO;
        read = buffer.Readf("%c", &c);
        CHECK_ADVANCE(1);
        if (c == YES)
        {
            READ_SEPARATOR();
            read = buffer.Readf("%U", &quorum.primaryID);
            CHECK_ADVANCE(1);
            quorum.hasPrimary = true;
        }
    }
    
    if (quorum.quorumID >= nextQuorumID)
        nextQuorumID = quorum.quorumID + 1;    
    
    return true;
}

void ConfigState::WriteQuorum(ConfigQuorum& quorum, Buffer& buffer, bool withVolatile)
{
    buffer.Appendf("%U", quorum.quorumID);
    buffer.Appendf(":");
    WriteIDList(quorum.activeNodes, buffer);
    buffer.Appendf(":");
    WriteIDList(quorum.inactiveNodes, buffer);
    buffer.Appendf(":");
    WriteIDList(quorum.shards, buffer);

    if (withVolatile)
    {
        buffer.Appendf(":");
        buffer.Appendf("%U", quorum.configID);
        buffer.Appendf(":");

        if (quorum.isActivatingNode)
            buffer.Appendf("%c:%U", YES, quorum.activatingNodeID);
        else
            buffer.Appendf("%c", NO);

        buffer.Appendf(":%U:", quorum.paxosID);
        if (quorum.hasPrimary)
            buffer.Appendf("%c:%U", YES, quorum.primaryID);
        else
            buffer.Appendf("%c", NO);
    }
}

bool ConfigState::ReadDatabase(ConfigDatabase& database, ReadBuffer& buffer)
{
    int read;
    
    read = buffer.Readf("%U:%#B", &database.databaseID, &database.name);
    CHECK_ADVANCE(5);
    READ_SEPARATOR();
    if (!ReadIDList(database.tables, buffer))
        return false;

    if (database.databaseID >= nextDatabaseID)
        nextDatabaseID = database.databaseID + 1;

    return true;
}

void ConfigState::WriteDatabase(ConfigDatabase& database, Buffer& buffer)
{
    buffer.Appendf("%U:%#B", database.databaseID, &database.name);
    buffer.Appendf(":");
    WriteIDList(database.tables, buffer);
}

bool ConfigState::ReadTable(ConfigTable& table, ReadBuffer& buffer)
{
    int read;
    
    read = buffer.Readf("%U:%U:%#B:%b", &table.databaseID, &table.tableID, &table.name, &table.isFrozen);
    CHECK_ADVANCE(5);
    READ_SEPARATOR();
    if (!ReadIDList(table.shards, buffer))
        return false;
    
    if (table.tableID >= nextTableID)
        nextTableID = table.tableID + 1;    
    
    return true;
}

void ConfigState::WriteTable(ConfigTable& table, Buffer& buffer)
{
    buffer.Appendf("%U:%U:%#B:%b", table.databaseID, table.tableID, &table.name, table.isFrozen);
    buffer.Appendf(":");
    WriteIDList(table.shards, buffer);
}

bool ConfigState::ReadShard(ConfigShard& shard, ReadBuffer& buffer, bool withVolatile)
{
    int read;
    
    read = buffer.Readf("%U:%U:%U:%#B:%#B:%u:%U",
     &shard.quorumID, &shard.tableID, &shard.shardID,
     &shard.firstKey, &shard.lastKey,
     &shard.state, &shard.parentShardID);
    CHECK_ADVANCE(15);

    if (withVolatile)
    {
        read = buffer.Readf(":%U:%#B",
         &shard.shardSize, &shard.splitKey);
        CHECK_ADVANCE(5);
    }

    if (shard.shardID >= nextShardID)
        nextShardID = shard.shardID + 1;
    
    return true;
}

void ConfigState::WriteShard(ConfigShard& shard, Buffer& buffer, bool withVolatile)
{
    buffer.Appendf("%U:%U:%U:%#B:%#B:%u:%U",
     shard.quorumID, shard.tableID, shard.shardID,
     &shard.firstKey, &shard.lastKey,
     shard.state, shard.parentShardID);

    if (withVolatile)
    {
        buffer.Appendf(":%U:%#B",
         shard.shardSize, &shard.splitKey);
    }
}

bool ConfigState::ReadShardServer(ConfigShardServer& shardServer, ReadBuffer& buffer, bool withVolatile)
{
    int         read;
    ReadBuffer  rb;

    read = buffer.Readf("%U:%#R", &shardServer.nodeID, &rb);
    CHECK_ADVANCE(3);
    shardServer.endpoint.Set(rb);
    
    if (shardServer.nodeID >= nextNodeID)
        nextNodeID = shardServer.nodeID + 1;
    
    if (withVolatile)
    {
        read = buffer.Readf(":%u:%u:%b",
         &shardServer.httpPort, &shardServer.sdbpPort, &shardServer.hasHeartbeat);
        CHECK_ADVANCE(6);
        READ_SEPARATOR();
        return QuorumInfo::ReadList(buffer, shardServer.quorumInfos);
    }
    else
        return true;
}

void ConfigState::WriteShardServer(ConfigShardServer& shardServer, Buffer& buffer, bool withVolatile)
{
    ReadBuffer endpoint;
    
    endpoint = shardServer.endpoint.ToReadBuffer();
    buffer.Appendf("%U:%#R", shardServer.nodeID, &endpoint);
    
    if (withVolatile)
    {
        buffer.Appendf(":%u:%u:%b",
         shardServer.httpPort, shardServer.sdbpPort, shardServer.hasHeartbeat);
        buffer.Appendf(":");
        QuorumInfo::WriteList(buffer, shardServer.quorumInfos);
    }
}

bool ConfigState::ReadNextIDs(ReadBuffer& buffer)
{
    int read;
    
    read = buffer.Readf("%U:%U:%U:%U:%U",
     &nextQuorumID, &nextDatabaseID, &nextTableID, &nextShardID, &nextNodeID);

    CHECK_ADVANCE(9);

    return true;
}

void ConfigState::WriteNextIDs(Buffer& buffer)
{
    buffer.Appendf("%U:%U:%U:%U:%U",
     nextQuorumID, nextDatabaseID, nextTableID, nextShardID, nextNodeID);
}

template<typename List>
bool ConfigState::ReadIDList(List& IDs, ReadBuffer& buffer)
{
    uint64_t    ID;
    unsigned    i, num;
    int         read;
    
    num = 0;
    read = buffer.Readf("%u", &num);
    CHECK_ADVANCE(1);
    
    for (i = 0; i < num; i++)
    {
        READ_SEPARATOR();
        read = buffer.Readf("%U", &ID);
        CHECK_ADVANCE(1);
        IDs.Add(ID);
    }
    
    return true;
}

template<typename List>
void ConfigState::WriteIDList(List& IDs, Buffer& buffer)
{
    uint64_t*   it;
    
    // write number of items
    
    buffer.Appendf("%u", IDs.GetLength());
    
    FOREACH (it, IDs)
        buffer.Appendf(":%U", *it);
}

#undef READ_SEPARATOR
#undef CHECK_READ
