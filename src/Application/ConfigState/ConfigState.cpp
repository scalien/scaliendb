#include "ConfigState.h"
#include "System/Macros.h"
#include "Application/Common/DatabaseConsts.h"

#define CONFIG_MESSAGE_PREFIX   'C'

#define READ_SEPARATOR()    \
    read = buffer.Readf(":");       \
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
    
    splitting = other.splitting;

    FOREACH(quorum, other.quorums)
        quorums.Append(new ConfigQuorum(*quorum));
    
    FOREACH(database, other.databases)
        databases.Append(new ConfigDatabase(*database));
    
    FOREACH(table, other.tables)
        tables.Append(new ConfigTable(*table));
    
    FOREACH(shard, other.shards)
        shards.Append(new ConfigShard(*shard));
    
    FOREACH(shardServer, other.shardServers)
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
    
    splitting = false;
    
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

        /* Table management */
        case CONFIGMESSAGE_CREATE_TABLE:
            return CompleteCreateTable(message);
        case CONFIGMESSAGE_RENAME_TABLE:
            return CompleteRenameTable(message);
        case CONFIGMESSAGE_DELETE_TABLE:
            return CompleteDeleteTable(message);
        case CONFIGMESSAGE_TRUNCATE_TABLE:
            return CompleteTruncateTable(message);
        
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
        assert(c == CONFIG_MESSAGE_PREFIX);

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
    // TODO: remove this when there is no more Writefs
    //buffer.Clear();
    
    if (withVolatile)
    {
        // HACK: in volatile mode the prefix is handled by ConfigState
        // TODO: change convention to Append in every Message::Write
        buffer.Appendf("%c:", CONFIG_MESSAGE_PREFIX);
        if (hasMaster)
            buffer.Appendf("%c:%U:", YES, masterID);
        else
            buffer.Appendf("%c:", NO);
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
    
    Log_Trace("buffer = %B", &buffer);
    
    return true;
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
        case CONFIGMESSAGE_TRUNCATE_TABLE:
            return OnTruncateTable(message);
        
        case CONFIGMESSAGE_SPLIT_SHARD_BEGIN:
            return OnSplitShardBegin(message);
        case CONFIGMESSAGE_SPLIT_SHARD_COMPLETE:
            return OnSplitShardComplete(message);
     
        case CONFIGMESSAGE_SET_CLUSTER_ID:
            return;
              
        default:
            ASSERT_FAIL(); 
    }
}

ConfigQuorum* ConfigState::GetQuorum(uint64_t quorumID)
{
    ConfigQuorum*   it;
    
    for (it = quorums.First(); it != NULL; it = quorums.Next(it))
    {
        if (it->quorumID == quorumID)
            return it;
    }
    
    return NULL;
}

ConfigDatabase* ConfigState::GetDatabase(uint64_t databaseID)
{
    ConfigDatabase* it;
    
    for (it =  databases.First(); it != NULL; it = databases.Next(it))
    {
        if (it->databaseID == databaseID)
            return it;
    }
    
    return NULL;
}

ConfigDatabase* ConfigState::GetDatabase(ReadBuffer name)
{
    ConfigDatabase* it;
    
    for (it = databases.First(); it != NULL; it = databases.Next(it))
    {
        if (BUFCMP(&it->name, &name))
            return it;
    }
    
    return NULL;
}

ConfigTable* ConfigState::GetTable(uint64_t tableID)
{
    ConfigTable*    it;
    
    for (it = tables.First(); it != NULL; it = tables.Next(it))
    {
        if (it->tableID == tableID)
            return it;
    }
    
    return NULL;
}

ConfigTable* ConfigState::GetTable(uint64_t databaseID, ReadBuffer name)
{
    ConfigTable*    it;
    
    for (it = tables.First(); it != NULL; it = tables.Next(it))
    {
        if (it->databaseID == databaseID && BUFCMP(&it->name, &name))
            return it;
    }
    
    return NULL;
}

ConfigShard* ConfigState::GetShard(uint64_t tableID, ReadBuffer key)
{
    ConfigShard*    it;
    
    for (it = shards.First(); it != NULL; it = shards.Next(it))
    {
        if (it->isSplitCreating)
            continue;
        
        if (it->tableID == tableID)
        {
            if (GREATER_THAN(key, it->firstKey) && LESS_THAN(key, it->lastKey))
                return it;
        }
    }
    
    return NULL;
}

ConfigShard* ConfigState::GetShard(uint64_t shardID)
{
    ConfigShard*    it;
    
    for (it = shards.First(); it != NULL; it = shards.Next(it))
    {
        if (it->shardID == shardID)
            return it;
    }
    
    return NULL;
}

ConfigShardServer* ConfigState::GetShardServer(uint64_t nodeID)
{
    ConfigShardServer*  it;
    
    for (it = shardServers.First(); it != NULL; it = shardServers.Next(it))
    {
        if (it->nodeID == nodeID)
            return it;
    }
    
    return NULL;
}

bool ConfigState::CompleteRegisterShardServer(ConfigMessage& message)
{
    message.nodeID = nextNodeID++;
    return true;
}

bool ConfigState::CompleteCreateQuorum(ConfigMessage& message)
{
    uint64_t*   it;
    
    for (it = message.nodes.First(); it != NULL; it = message.nodes.Next(it))
    {
        if (GetShardServer(*it) == NULL)
            return false; // no such shard server
    }

    message.quorumID = nextQuorumID++;
    
    return true;
}

bool ConfigState::CompleteDeleteQuorum(ConfigMessage& message)
{
    uint64_t*       itShardID;
    ConfigQuorum*   quorum;
    ConfigShard*    shard;

    quorum = GetQuorum(message.quorumID);
    if (quorum == NULL)
        return false; // no such quorum

    FOREACH(itShardID, quorum->shards)
    {
        shard = GetShard(*itShardID);
        if (!shard->isDeleted)
            return false; // quorum contains shards
    }
    
    return true;
}

bool ConfigState::CompleteAddNode(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    uint64_t*       itNodeID;
    
    itQuorum = GetQuorum(message.quorumID);
    if (itQuorum == NULL)
        return false; // no such quorum
    
    List<uint64_t>& activeNodes = itQuorum->activeNodes;
    
    for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            return false; // node already in quorum
    }

    List<uint64_t>& inactiveNodes = itQuorum->inactiveNodes;
    for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            return false; // node already in quorum
    }
    
    return true;
}

bool ConfigState::CompleteRemoveNode(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    uint64_t*       itNodeID;
    
    itQuorum = GetQuorum(message.quorumID);
    if (itQuorum == NULL)
        return false; // no such quorum
    
    if (itQuorum->activeNodes.GetLength() <= 1)
        return false; // can't remove last active node
    
    List<uint64_t>& activeNodes = itQuorum->activeNodes;
    for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            return true; // node in quorum
    }

    List<uint64_t>& inactiveNodes = itQuorum->inactiveNodes;
    for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            return true; // node in quorum
    }
    
    return false; // node not in quorum
}

bool ConfigState::CompleteActivateShardServer(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    uint64_t*       itNodeID;
    
    itQuorum = GetQuorum(message.quorumID);
    if (itQuorum == NULL)
        return false; // no such quorum

    List<uint64_t>& inactiveNodes = itQuorum->inactiveNodes;
    FOREACH(itNodeID, inactiveNodes)
    {
        if (*itNodeID == message.nodeID)
            return true; // active
    }

    return false; // not active
}

bool ConfigState::CompleteDeactivateShardServer(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    uint64_t*       itNodeID;
    
    itQuorum = GetQuorum(message.quorumID);
    if (itQuorum == NULL)
        return false; // no such quorum

    List<uint64_t>& activeNodes = itQuorum->activeNodes;
    FOREACH(itNodeID, activeNodes)
    {
        if (*itNodeID == message.nodeID)
            return true; // inactive
    }

    return false; // not inactive
}

bool ConfigState::CompleteCreateDatabase(ConfigMessage& message)
{
    ConfigDatabase* it;
    
    if (message.name.GetLength() > DATABASE_NAME_SIZE)
        return false;
    
    it = GetDatabase(message.name);
    if (it != NULL)
        return false; // database with name exists

    message.databaseID = 0;
    return true;
}

bool ConfigState::CompleteRenameDatabase(ConfigMessage& message)
{
    ConfigDatabase* it;
    
    if (message.name.GetLength() > DATABASE_NAME_SIZE)
        return false;

    it = GetDatabase(message.databaseID);
    if (it == NULL)
        return false; // no such database

    it = GetDatabase(message.name);
    if (it != NULL)
        return false; // database with name exists

    return true;
}

bool ConfigState::CompleteDeleteDatabase(ConfigMessage& message)
{
    ConfigDatabase* it;
    
    it = GetDatabase(message.databaseID);
    if (it == NULL)
        return false; // no such database

    return true;
}

bool ConfigState::CompleteCreateTable(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    ConfigDatabase* itDatabase;
    ConfigTable*    itTable;
    
    if (message.name.GetLength() > DATABASE_NAME_SIZE)
        return false;

    itQuorum = GetQuorum(message.quorumID); 
    if (itQuorum == NULL)
        return false; // no such quorum

    itDatabase = GetDatabase(message.databaseID);
    if (itDatabase == NULL)
        return false; // no such database

    itTable = GetTable(message.databaseID, message.name);
    if (itTable != NULL)
        return false; // table with name exists in database

    message.tableID = 0;
    message.shardID = 0;
    return true;
}

bool ConfigState::CompleteRenameTable(ConfigMessage& message)
{
    ConfigDatabase* itDatabase;
    ConfigTable*    itTable;

    if (message.name.GetLength() > DATABASE_NAME_SIZE)
        return false;

    itDatabase = GetDatabase(message.databaseID);
    if (itDatabase == NULL)
        return false; // no such database

    itTable = GetTable(message.tableID);
    if (itTable == NULL)
        return false; // no such table
    itTable = GetTable(message.databaseID, message.name);
    if (itTable != NULL)
        return false; // table with name exists in database

    return true;
}

bool ConfigState::CompleteDeleteTable(ConfigMessage& message)
{
    ConfigDatabase* itDatabase;
    ConfigTable*    itTable;

    itDatabase = GetDatabase(message.databaseID);
    if (itDatabase == NULL)
        return false; // no such database

    itTable = GetTable(message.tableID);
    if (itTable == NULL)
        return false; // no such table

    return true;
}

bool ConfigState::CompleteTruncateTable(ConfigMessage& message)
{
    ConfigDatabase* itDatabase;
    ConfigTable*    itTable;

    itDatabase = GetDatabase(message.databaseID);
    if (itDatabase == NULL)
        return false; // no such database

    itTable = GetTable(message.tableID);
    if (itTable == NULL)
        return false; // no such table

    return true;
}

bool ConfigState::CompleteSplitShardBegin(ConfigMessage& message)
{
    if (GetShard(message.shardID) == NULL)
        return false;
    return true;
}

bool ConfigState::CompleteSplitShardComplete(ConfigMessage& /*message*/)
{
    return true;
}

void ConfigState::OnRegisterShardServer(ConfigMessage& message)
{
    ConfigShardServer*  it;
    
    // make sure nodeID is available
    assert(GetShardServer(message.nodeID) == NULL);
    
    it = new ConfigShardServer;
    it->nodeID = message.nodeID;
    it->endpoint = message.endpoint;
    
    shardServers.Append(it);
    
    // when a round is cleaned out from Paxos on startup, we need to increase the nextIDs
    // usually nextIDs are increased when the ClientRequest is made, but not in this case
    if (message.nodeID == nextNodeID)
        nextNodeID++;
}

void ConfigState::OnCreateQuorum(ConfigMessage& message)
{
    ConfigQuorum*   it;
    
    // make sure quorumID is available
    assert(GetQuorum(message.quorumID) == NULL);
        
    it = new ConfigQuorum;
    it->quorumID = message.quorumID;
    it->activeNodes = message.nodes;
    
    quorums.Append(it);

    // when a round is cleaned out from Paxos on startup, we need to increase the nextIDs
    // usually nextIDs are increased when the ClientRequest is made, but not in this case
    if (message.quorumID == nextQuorumID)
        nextQuorumID++;
}

void ConfigState::OnDeleteQuorum(ConfigMessage& message)
{
    ConfigQuorum*   quorum;
    
    // make sure quorum exists
    quorum = GetQuorum(message.quorumID);
    
    quorums.Remove(quorum);
    
    delete quorum;
}

void ConfigState::OnAddNode(ConfigMessage& message)
{
    ConfigQuorum*   quorum;
    uint64_t*       itNodeID;
    
    quorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    assert(quorum != NULL);
        
    // make sure node is not already in quorum
    List<uint64_t>& activeNodes = quorum->activeNodes;
    for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            ASSERT_FAIL();
    }

    List<uint64_t>& inactiveNodes = quorum->inactiveNodes;
    for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            ASSERT_FAIL();
    }
    
    inactiveNodes.Append(message.nodeID);
}

void ConfigState::OnRemoveNode(ConfigMessage& message)
{
    ConfigQuorum*   quorum;
    uint64_t*       itNodeID;
    
    quorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    assert(quorum != NULL);
    
    // make sure node is in quorum
    List<uint64_t>& activeNodes = quorum->activeNodes;
    for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
        {
            activeNodes.Remove(itNodeID);
            return;
        }
    }

    List<uint64_t>& inactiveNodes = quorum->inactiveNodes;
    for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
        {
            inactiveNodes.Remove(itNodeID);
            return;
        }
    }
    
    ASSERT_FAIL();
}

void ConfigState::OnActivateShardServer(ConfigMessage& message)
{
    ConfigQuorum*       itQuorum;
    uint64_t*           itNodeID;
    ConfigShardServer*  shardServer;
    
    itQuorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    assert(itQuorum != NULL);

    // make sure node is in quorum
    List<uint64_t>& inactiveNodes = itQuorum->inactiveNodes;
    List<uint64_t>& activeNodes = itQuorum->activeNodes;
    FOREACH(itNodeID, inactiveNodes)
    {
        if (*itNodeID == message.nodeID)
        {
            inactiveNodes.Remove(itNodeID);
            activeNodes.Append(message.nodeID);

            if (itQuorum->isActivatingNode)
            {
                shardServer = GetShardServer(itQuorum->activatingNodeID);
                assert(shardServer != NULL);
                
                Log_Message("Activation succeeded for quorum %U and shard server %U",
                 itQuorum->quorumID, itQuorum->activatingNodeID);

                itQuorum->ClearActivation();
            }
            return;
        }
    }
    
    ASSERT_FAIL();
}

void ConfigState::OnDeactivateShardServer(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    uint64_t*       itNodeID;
    
    itQuorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    assert(itQuorum != NULL);
    
    // make sure node is in quorum
    List<uint64_t>& inactiveNodes = itQuorum->inactiveNodes;
    List<uint64_t>& activeNodes = itQuorum->activeNodes;
    FOREACH(itNodeID, activeNodes)
    {
        if (*itNodeID == message.nodeID)
        {
            activeNodes.Remove(itNodeID);
            inactiveNodes.Append(message.nodeID);
            return;
        }
    }
    
    ASSERT_FAIL();
}

void ConfigState::OnCreateDatabase(ConfigMessage& message)
{
    ConfigDatabase* it;
    
    it = GetDatabase(nextDatabaseID);
    // make sure database with ID does not exist
    assert(it == NULL);
    it = GetDatabase(message.name);
    // make sure database with name does not exist
    assert(it == NULL);
        
    it = new ConfigDatabase;

    //it->databaseID = message.databaseID;
    it->databaseID = nextDatabaseID;
    it->name.Write(message.name);
    databases.Append(it);

    message.databaseID = it->databaseID;
    
    nextDatabaseID++;
}

void ConfigState::OnRenameDatabase(ConfigMessage& message)
{
    ConfigDatabase* it;

    it = GetDatabase(message.name);
    // make sure database with name does not exist
    assert(it == NULL);

    it = GetDatabase(message.databaseID);
    // make sure database with ID exists
    assert(it != NULL);
    
    it->name.Write(message.name);
}

void ConfigState::OnDeleteDatabase(ConfigMessage& message)
{
    ConfigDatabase* database;
    ConfigTable*    table;
    uint64_t*       it;
    
    database = GetDatabase(message.databaseID);
    // make sure database with ID exists
    assert(database != NULL);

    FOREACH(it, database->tables)
    {
        table = GetTable(*it);
        DeleteTable(table);
    }

    databases.Delete(database);
}

void ConfigState::OnCreateTable(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    ConfigDatabase* itDatabase;
    ConfigTable*    itTable;
    ConfigShard*    itShard;

    itQuorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    assert(itQuorum != NULL);

    itDatabase = GetDatabase(message.databaseID);
    // make sure database exists
    assert(itDatabase != NULL);

    itTable = GetTable(nextTableID);
    // make sure table with ID does not exist
    assert(itTable == NULL);
    itTable = GetTable(message.databaseID, message.name);
    // make sure table with name in database does not exist
    assert(itTable == NULL);

    itShard = GetShard(message.shardID);
    // make sure shard does not exist
    assert(itShard == NULL);
        
    itShard = new ConfigShard;
    itShard->quorumID = message.quorumID;
    itShard->databaseID = message.databaseID;
    itShard->tableID = nextTableID;
    itShard->shardID = nextShardID;
    shards.Append(itShard);

    nextShardID++;
    
    itTable = new ConfigTable;
    itTable->databaseID = message.databaseID;
    itTable->tableID = nextTableID;
    itTable->name.Write(message.name);
    itTable->shards.Append(itShard->shardID);
    tables.Append(itTable);
    
    itDatabase->tables.Append(itTable->tableID);
    itQuorum->shards.Add(itShard->shardID);

    message.tableID = itTable->tableID;

    nextTableID++;
}

void ConfigState::OnRenameTable(ConfigMessage& message)
{
    ConfigDatabase* itDatabase;
    ConfigTable*    itTable;
    
    itDatabase = GetDatabase(message.databaseID);
    // make sure database exists
    assert(itDatabase != NULL);

    itTable = GetTable(message.databaseID, message.name);
    // make sure table with name does not exist
    assert(itTable == NULL);

    itTable = GetTable(message.tableID);
    // make sure table with ID exists
    assert(itTable != NULL);
    
    itTable->name.Write(message.name);
}

void ConfigState::OnDeleteTable(ConfigMessage& message)
{
    ConfigDatabase* itDatabase;
    ConfigTable*    itTable;
    
    itDatabase = GetDatabase(message.databaseID);
    // make sure database exists
    assert(itDatabase != NULL);
    itTable = GetTable(message.tableID);
    // make sure table exists
    assert(itTable != NULL);
    
    DeleteTable(itTable);
    itDatabase->tables.Remove(message.tableID);
}

void ConfigState::OnTruncateTable(ConfigMessage& message)
{
    uint64_t*       it;
    ConfigQuorum*   quorum;
    ConfigDatabase* database;
    ConfigTable*    table;
    ConfigShard*    shard;
    uint64_t        quorumID;
    
    database = GetDatabase(message.databaseID);
    // make sure database exists
    assert(database != NULL);
    table = GetTable(message.tableID);
    // make sure table exists
    assert(table != NULL);

    // delete all old shards
    FOREACH(it, table->shards)
    {
        shard = GetShard(*it);
        if (shard->isDeleted)
            continue;

        quorumID = shard->quorumID;
        DeleteShard(shard);
    }
    
    quorum = GetQuorum(quorumID);
    assert(quorum != NULL);

    // create a new shard
    shard = new ConfigShard;
    shard->shardID = nextShardID;
    shard->quorumID = quorumID;
    shard->databaseID = message.databaseID;
    shard->tableID = message.tableID;

    table->shards.Append(shard->shardID);
    shards.Append(shard);
    quorum->shards.Add(shard->shardID);

    nextShardID++;
}

void ConfigState::OnSplitShardBegin(ConfigMessage& message)
{
    ConfigShard*    newShard;
    ConfigShard*    parentShard;
    ConfigQuorum*   quorum;
    ConfigTable*    table;
    
    parentShard = GetShard(message.shardID);
    assert(parentShard);
    
    newShard = new ConfigShard;
    newShard->isSplitCreating = true;
    newShard->shardID = nextShardID++;
    newShard->parentShardID = message.shardID;
    newShard->quorumID = parentShard->quorumID;
    newShard->databaseID = parentShard->databaseID;
    newShard->tableID = parentShard->tableID;
    newShard->firstKey.Write(message.splitKey);
    newShard->lastKey.Write(parentShard->lastKey);
    shards.Append(newShard);
    
    parentShard->lastKey.Write(newShard->firstKey);

    quorum = GetQuorum(newShard->quorumID);
    quorum->shards.Add(newShard->shardID);
    
    table = GetTable(parentShard->tableID);
    table->shards.Add(newShard->shardID);

    message.newShardID = newShard->shardID; // TODO: this is kind of a hack
}

void ConfigState::OnSplitShardComplete(ConfigMessage& message)
{
    ConfigShard*    shard;
    
    shard = GetShard(message.shardID);
    assert(shard->isSplitCreating);
    
    shard->isSplitCreating = false;
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
    ConfigQuorum*   it;

    // write number of quorums
    buffer.Appendf("%u", quorums.GetLength());

    for (it = quorums.First(); it != NULL; it = quorums.Next(it))
    {
        buffer.Appendf(":");
        WriteQuorum(*it, buffer, withVolatile);
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
    ConfigTable*    it;
    
    // write number of databases
    buffer.Appendf("%u", tables.GetLength());
    
    for (it = tables.First(); it != NULL; it = tables.Next(it))
    {
        buffer.Appendf(":");
        WriteTable(*it, buffer);
    }
}

void ConfigState::DeleteTable(ConfigTable* table)
{
    ConfigShard*    shard;
    uint64_t*       it;

    FOREACH(it, table->shards)
    {
        shard = GetShard(*it);
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
    ConfigShard*    it;
    
    // write number of databases
    buffer.Appendf("%u", shards.GetLength());
    
    for (it = shards.First(); it != NULL; it = shards.Next(it))
    {
        buffer.Appendf(":");
        WriteShard(*it, buffer, withVolatile);
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
    ConfigShardServer*  it;
    
    // write number of databases
    buffer.Appendf("%u", shardServers.GetLength());
    
    for (it = shardServers.First(); it != NULL; it = shardServers.Next(it))
    {
        buffer.Appendf(":");
        WriteShardServer(*it, buffer, withVolatile);
    }
}

void ConfigState::DeleteShard(ConfigShard* shard)
{
    ConfigQuorum*   quorum;

    quorum = GetQuorum(shard->quorumID);
    assert(quorum != NULL);
    quorum->shards.Remove(shard->shardID);

//    shards.Delete(shard);
    shard->isDeleted = true;
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
    
    read = buffer.Readf("%U:%U:%#B", &table.databaseID, &table.tableID, &table.name);
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
    buffer.Appendf("%U:%U:%#B", table.databaseID, table.tableID, &table.name);
    buffer.Appendf(":");
    WriteIDList(table.shards, buffer);
}

bool ConfigState::ReadShard(ConfigShard& shard, ReadBuffer& buffer, bool withVolatile)
{
    int read;
    
    read = buffer.Readf("%U:%U:%U:%U:%b:%#B:%#B:%b:%U",
     &shard.quorumID, &shard.databaseID, &shard.tableID, &shard.shardID,
     &shard.isDeleted, &shard.firstKey, &shard.lastKey,
     &shard.isSplitCreating, &shard.parentShardID);
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
    buffer.Appendf("%U:%U:%U:%U:%b:%#B:%#B:%b:%U",
     shard.quorumID, shard.databaseID, shard.tableID, shard.shardID,
     shard.isDeleted, &shard.firstKey, &shard.lastKey,
     shard.isSplitCreating, shard.parentShardID);

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
        read = buffer.Readf(":%u:%u", &shardServer.httpPort, &shardServer.sdbpPort);
        CHECK_ADVANCE(4);
        READ_SEPARATOR();
        return QuorumPaxosID::ReadList(buffer, shardServer.quorumPaxosIDs);
    }
    else
        return true;
}

void ConfigState::WriteShardServer(ConfigShardServer& shardServer, Buffer& buffer, bool withVolatile)
{
    ReadBuffer  endpoint;
    
    endpoint = shardServer.endpoint.ToReadBuffer();
    buffer.Appendf("%U:%#R", shardServer.nodeID, &endpoint);
    
    if (withVolatile)
    {
        buffer.Appendf(":%u:%u",shardServer.httpPort, shardServer.sdbpPort);
        buffer.Appendf(":");
        QuorumPaxosID::WriteList(buffer, shardServer.quorumPaxosIDs);
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
    
    FOREACH(it, IDs)
        buffer.Appendf(":%U", *it);
}

#undef READ_SEPARATOR
#undef CHECK_READ
