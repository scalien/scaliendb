#include "ConfigState.h"

#define HAS_LEADER_YES          'Y'
#define HAS_LEADER_NO           'N'
#define CONFIG_MESSAGE_PREFIX   'C'

#define READ_SEPARATOR()    \
    read = buffer.Readf(":");       \
    if (read < 1) return false; \
    buffer.Advance(read);

#define CHECK_ADVANCE(n)    \
    if (read < n)           \
        return false;       \
    buffer.Advance(read)

ConfigState::~ConfigState()
{
    shardServers.DeleteList();
    quorums.DeleteList();
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
        case CONFIGMESSAGE_INCREASE_QUORUM:
            return CompleteIncreaseQuorum(message);
        case CONFIGMESSAGE_DECREASE_QUORUM:
            return CompleteDecreaseQuorum(message);

        /* Database management */
        case CONFIGMESSAGE_CREATE_DATABASE:
            return CompleteCreateDatabase(message);
        case CONFIGMESSAGE_RENAME_DATABASE:
            return CompleteRenameDatabase(message);
        case CONFIGMESSAGE_DELETE_DATABASE:
            return CompleteDeleteDatabase(message);

        /* Table management */
        case CONFIGMESSAGE_CREATE_TABLE:
            return CompleteCreateTable(message);
        case CONFIGMESSAGE_RENAME_TABLE:
            return CompleteRenameTable(message);
        case CONFIGMESSAGE_DELETE_TABLE:
            return CompleteDeleteTable(message);
        
        default:
            ASSERT_FAIL(); 
    }
}

bool ConfigState::Read(ReadBuffer& buffer_, bool withVolatile)
{
    char        c;
    int         read;
    ReadBuffer  buffer;
    
    buffer = buffer_; // because of Advance()

    hasMaster = false;
    if (withVolatile)
    {
        // HACK: in volatile mode the prefix is handled by ConfigState
        // TODO: change convention to Append in every Message::Write
        read = buffer.Readf("%c", &c);
        CHECK_ADVANCE(1);
        assert(c == CONFIG_MESSAGE_PREFIX);

        READ_SEPARATOR();
        c = HAS_LEADER_NO;
        buffer.Readf("%c", &c);
        CHECK_ADVANCE(1);
        if (c == HAS_LEADER_YES)
        {
            READ_SEPARATOR();
            buffer.Readf("%U", &masterID);
        }
    }
    
    if (!ReadQuorums(buffer, withVolatile))
        return false;
    READ_SEPARATOR();
    if (!ReadDatabases(buffer))
        return false;
    READ_SEPARATOR();
    if (!ReadTables(buffer))
        return false;
    READ_SEPARATOR();
    if (!ReadShards(buffer))
        return false;
    READ_SEPARATOR();
    if (!ReadShardServers(buffer))
        return false;

    return (buffer.GetLength() == 0);
}

bool ConfigState::Write(Buffer& buffer, bool withVolatile)
{
    buffer.Clear();
    
    if (withVolatile)
    {
        // HACK: in volatile mode the prefix is handled by ConfigState
        // TODO: change convention to Append in every Message::Write
        buffer.Appendf("%c:", CONFIG_MESSAGE_PREFIX);
        if (hasMaster)
            buffer.Appendf("%c:%U", HAS_LEADER_YES, masterID);
        else
            buffer.Appendf("%c", HAS_LEADER_NO);
    }

    WriteQuorums(buffer, withVolatile);
    buffer.Appendf(":");
    WriteDatabases(buffer);
    buffer.Appendf(":");
    WriteTables(buffer);
    buffer.Appendf(":");
    WriteShards(buffer);
    buffer.Appendf(":");
    WriteShardServers(buffer);
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
        case CONFIGMESSAGE_INCREASE_QUORUM:
            return OnIncreaseQuorum(message);
        case CONFIGMESSAGE_DECREASE_QUORUM:
            return OnDecreaseQuorum(message);

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
        if (it->tableID == tableID)
        {
            if (ReadBuffer::Cmp(key, it->firstKey) >= 0 && ReadBuffer::Cmp(key, it->lastKey) <= 0)
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

bool ConfigState::CompleteIncreaseQuorum(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    uint64_t*       itNodeID;
    
    itQuorum = GetQuorum(message.quorumID);
    if (itQuorum == NULL)
        return false; // no such quorum
    
    ConfigQuorum::NodeList& activeNodes = itQuorum->activeNodes;
    
    for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            return false; // node already in quorum
    }

    ConfigQuorum::NodeList& inactiveNodes = itQuorum->inactiveNodes;
    for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            return false; // node already in quorum
    }

    
    return true;
}

bool ConfigState::CompleteDecreaseQuorum(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    uint64_t*       itNodeID;
    
    itQuorum = GetQuorum(message.quorumID);
    if (itQuorum == NULL)
        return false; // no such quorum
    
    ConfigQuorum::NodeList& activeNodes = itQuorum->activeNodes;
    for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            return true; // node in quorum
    }

    ConfigQuorum::NodeList& inactiveNodes = itQuorum->inactiveNodes;
    for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            return true; // node in quorum
    }
    
    return false; // node not in quorum
}

bool ConfigState::CompleteCreateDatabase(ConfigMessage& message)
{
    ConfigDatabase* it;
    
    it = GetDatabase(message.name);
    if (it != NULL)
        return false; // database with name exists

    message.databaseID = nextDatabaseID++;
    return true;
}

bool ConfigState::CompleteRenameDatabase(ConfigMessage& message)
{
    ConfigDatabase* it;
    
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
    
    itQuorum = GetQuorum(message.quorumID); 
    if (itQuorum == NULL)
        return false; // no such quorum

    itDatabase = GetDatabase(message.databaseID);
    if (itDatabase == NULL)
        return false; // no such database

    itTable = GetTable(message.databaseID, message.name);
    if (itTable != NULL)
        return false; // table with name exists in database

    message.tableID = nextTableID++;
    message.shardID = nextShardID++;
    return true;
}

bool ConfigState::CompleteRenameTable(ConfigMessage& message)
{
    ConfigDatabase* itDatabase;
    ConfigTable*    itTable;

    itDatabase = GetDatabase(message.databaseID);
    if (itDatabase == NULL)
        return false; // no such database

    itTable = GetTable(message.tableID);
    if (itTable == NULL)
        return false; // no such table
    itTable = GetTable(message.databaseID, message.name);
    if (itTable != NULL);
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

void ConfigState::OnRegisterShardServer(ConfigMessage& message)
{
    ConfigShardServer*  it;
    
    // make sure nodeID is available
    assert(GetShardServer(message.nodeID) == NULL);
    
    it = new ConfigShardServer;
    it->nodeID = message.nodeID;
    it->endpoint = message.endpoint;
    
    shardServers.Append(it);
}

void ConfigState::OnCreateQuorum(ConfigMessage& message)
{
    ConfigQuorum*   it;
    
    // make sure quorumID is available
    assert(GetQuorum(message.quorumID) == NULL);
        
    it = new ConfigQuorum;
    it->quorumID = message.quorumID;
    it->activeNodes = message.nodes;
    it->productionType = message.productionType;
    
    quorums.Append(it);
}

void ConfigState::OnIncreaseQuorum(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    uint64_t*       itNodeID;
    
    itQuorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    assert(itQuorum != NULL);
        
    // make sure node is not already in quorum
    ConfigQuorum::NodeList& activeNodes = itQuorum->activeNodes;
    for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            ASSERT_FAIL();
    }

    ConfigQuorum::NodeList& inactiveNodes = itQuorum->inactiveNodes;
    for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
            ASSERT_FAIL();
    }
    
    inactiveNodes.Append(message.nodeID);
}

void ConfigState::OnDecreaseQuorum(ConfigMessage& message)
{
    ConfigQuorum*   itQuorum;
    uint64_t*       itNodeID;
    
    itQuorum = GetQuorum(message.quorumID);
    // make sure quorum exists
    assert(itQuorum != NULL);
    
    // make sure node is in quorum
    ConfigQuorum::NodeList& activeNodes = itQuorum->activeNodes;
    for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
    {
        if (*itNodeID == message.nodeID)
        {
            activeNodes.Remove(itNodeID);
            return;
        }
    }

    ConfigQuorum::NodeList& inactiveNodes = itQuorum->inactiveNodes;
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

void ConfigState::OnCreateDatabase(ConfigMessage& message)
{
    ConfigDatabase* it;
    
    it = GetDatabase(message.databaseID);
    // make sure database with ID does not exist
    assert(it == NULL);
    it = GetDatabase(message.name);
    // make sure database with name does not exist
    assert(it == NULL);
        
    it = new ConfigDatabase;
    it->databaseID = message.databaseID;
    it->name.Write(message.name);
    databases.Append(it);
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
    ConfigDatabase* it;

    it = GetDatabase(message.databaseID);
    // make sure database with ID exists
    assert(it != NULL);

    databases.Delete(it);
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

    itTable = GetTable(message.tableID);
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
    itShard->tableID = message.tableID;
    itShard->shardID = message.shardID;
    shards.Append(itShard);
    
    itTable = new ConfigTable;
    itTable->databaseID = message.databaseID;
    itTable->tableID = message.tableID;
    itTable->name.Write(message.name);
    itTable->shards.Append(message.shardID);
    tables.Append(itTable);
    
    itDatabase->tables.Append(message.tableID);
}

void ConfigState::OnRenameTable(ConfigMessage& message)
{
    ConfigDatabase* itDatabase;
    ConfigTable*    itTable;
    
    itDatabase = GetDatabase(message.databaseID);
    // make sure database exists
    assert(itDatabase != NULL);
    itTable = GetTable(message.tableID);
    // make sure table with ID exists
    assert(itTable != NULL);
    itTable = GetTable(message.databaseID, message.name);
    // make sure table with name does not exist
    assert(itTable == NULL);
    
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
    
    tables.Delete(itTable);
    itDatabase->tables.Remove(message.tableID);
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

bool ConfigState::ReadShards(ReadBuffer& buffer)
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
        if (!ReadShard(*shard, buffer))
        {
            delete shard;
            return false;
        }
        shards.Append(shard);
    }
    
    return true;
}

void ConfigState::WriteShards(Buffer& buffer)
{
    ConfigShard*    it;
    
    // write number of databases
    buffer.Appendf("%u", shards.GetLength());
    
    for (it = shards.First(); it != NULL; it = shards.Next(it))
    {
        buffer.Appendf(":");
        WriteShard(*it, buffer);
    }
}

bool ConfigState::ReadShardServers(ReadBuffer& buffer)
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
        if (!ReadShardServer(*shardServer, buffer))
        {
            delete shardServer;
            return false;
        }
        shardServers.Append(shardServer);
    }
    
    return true;
}

void ConfigState::WriteShardServers(Buffer& buffer)
{
    ConfigShardServer*  it;
    
    // write number of databases
    buffer.Appendf("%u", shardServers.GetLength());
    
    for (it = shardServers.First(); it != NULL; it = shardServers.Next(it))
    {
        buffer.Appendf(":");
        WriteShardServer(*it, buffer);
    }
}

bool ConfigState::ReadQuorum(ConfigQuorum& quorum, ReadBuffer& buffer, bool withVolatile)
{
    int     read;
    char    c;
    
    read = buffer.Readf("%U:%c", &quorum.quorumID, &quorum.productionType);
    CHECK_ADVANCE(3);
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
    if (withVolatile)
    {
        READ_SEPARATOR();
        c = HAS_LEADER_NO;
        buffer.Readf("%c", &c);
        CHECK_ADVANCE(1);
        if (c == HAS_LEADER_YES)
        {
            READ_SEPARATOR();
            buffer.Readf("%U", &quorum.primaryID);
        }
    }
    
    if (quorum.quorumID >= nextQuorumID)
        nextQuorumID = quorum.quorumID + 1;    
    
    return true;
}

void ConfigState::WriteQuorum(ConfigQuorum& quorum, Buffer& buffer, bool withVolatile)
{
    buffer.Appendf("%U:%c", quorum.quorumID, quorum.productionType);
    buffer.Appendf(":");
    WriteIDList(quorum.activeNodes, buffer);
    buffer.Appendf(":");
    WriteIDList(quorum.inactiveNodes, buffer);
    buffer.Appendf(":");
    WriteIDList(quorum.shards, buffer);

    if (withVolatile)
    {
        buffer.Appendf(":");
        if (quorum.hasPrimary)
            buffer.Appendf("P:%U", quorum.primaryID);
        else
            buffer.Appendf("N");
    }
}

bool ConfigState::ReadDatabase(ConfigDatabase& database, ReadBuffer& buffer)
{
    int read;
    
    read = buffer.Readf("%U:%#B:%c", &database.databaseID, &database.name, &database.productionType);
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
    buffer.Appendf("%U:%#B:%c", database.databaseID, &database.name, database.productionType);
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

bool ConfigState::ReadShard(ConfigShard& shard, ReadBuffer& buffer)
{
    int read;
    
    read = buffer.Readf("%U:%U:%U:%U:%#B:%#B",
     &shard.quorumID, &shard.databaseID, &shard.tableID,
     &shard.shardID, &shard.firstKey, &shard.lastKey);
    CHECK_ADVANCE(11);

    if (shard.shardID >= nextShardID)
        nextShardID = shard.shardID + 1;

    return true;
}

void ConfigState::WriteShard(ConfigShard& shard, Buffer& buffer)
{
    buffer.Appendf("%U:%U:%U:%U:%#B:%#B",
     shard.quorumID, shard.databaseID, shard.tableID,
     shard.shardID, &shard.firstKey, &shard.lastKey);
}

bool ConfigState::ReadShardServer(ConfigShardServer& shardServer, ReadBuffer& buffer)
{
    int         read;
    ReadBuffer  rb;

    read = buffer.Readf("%U:%#R", &shardServer.nodeID, &rb);
    CHECK_ADVANCE(3);
    shardServer.endpoint.Set(rb);
    
    if (shardServer.nodeID >= nextNodeID)
        nextNodeID = shardServer.nodeID + 1;
    
    return true;
}

void ConfigState::WriteShardServer(ConfigShardServer& shardServer, Buffer& buffer)
{
    ReadBuffer endpoint;
    
    endpoint = shardServer.endpoint.ToReadBuffer();
    buffer.Appendf("%U:%#R", shardServer.nodeID, &endpoint);
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
        IDs.Append(ID);
    }
    
    return true;
}

template<typename List>
void ConfigState::WriteIDList(List& IDs, Buffer& buffer)
{
    uint64_t*   it;
    
    // write number of items
    
    buffer.Appendf("%u", IDs.GetLength());
    
    for (it = IDs.First(); it != NULL; it = IDs.Next(it))
        buffer.Appendf(":%U", *it);
}

#undef READ_SEPARATOR
#undef CHECK_READ
