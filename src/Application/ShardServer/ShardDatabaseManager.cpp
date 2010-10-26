#include "ShardDatabaseManager.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "ShardServer.h"

static void WriteValue(
Buffer &buffer, uint64_t paxosID, uint64_t commandID, ReadBuffer userValue)
{
    if (!buffer.Writef("%U:%U:%R", paxosID, commandID, &userValue))
        ASSERT_FAIL();
}

static void ReadValue(
ReadBuffer& buffer, uint64_t& paxosID, uint64_t& commandID, ReadBuffer& userValue)
{
    int read;

    read = buffer.Readf("%U:%U:%R", &paxosID, &commandID, &userValue);
    
    if (read < 4)
        ASSERT_FAIL();
}

static size_t Hash(uint64_t h)
{
    return h;
}

void ShardDatabaseManager::Init(ShardServer* shardServer_)
{
    shardServer = shardServer_;
    environment.InitCache(configFile.GetIntValue("database.cacheSize", STORAGE_DEFAULT_CACHE_SIZE));
    environment.SetSync(configFile.GetBoolValue("database.sync", true));
    environment.Open(configFile.GetValue("database.dir", "db"));
    systemDatabase = environment.GetDatabase("system");
    REPLICATION_CONFIG->Init(systemDatabase->GetTable("system"));
}

void ShardDatabaseManager::Shutdown()
{
    environment.Close();
}

StorageEnvironment* ShardDatabaseManager::GetEnvironment()
{
    return &environment;
}

StorageTable* ShardDatabaseManager::GetQuorumTable(uint64_t quorumID)
{
    Buffer name;
    
    name.Writef("%U", quorumID);
    name.NullTerminate();
    return systemDatabase->GetTable(name.GetBuffer());
}

StorageTable* ShardDatabaseManager::GetTable(uint64_t tableID)
{
    StorageTable* table;
    
    if (!tables.Get(tableID, table))
        return NULL;
    return table;
}

StorageShard* ShardDatabaseManager::GetShard(uint64_t shardID)
{
    ConfigShard*    shard;
    StorageTable*   table;
    
    shard = shardServer->GetConfigState()->GetShard(shardID);
    if (!shard)
        return NULL;
    
    table = GetTable(shard->tableID);
    if (!table)
        return NULL;
    
    return table->GetShard(shardID);
}

void ShardDatabaseManager::SetShards(List<uint64_t>& shards)
{
    uint64_t*           sit;
    ConfigShard*        shard;
    StorageDatabase*    database;
    StorageTable*       table;
    Buffer              name;
    ReadBuffer          firstKey;

    for (sit = shards.First(); sit != NULL; sit = shards.Next(sit))
    {
        shard = shardServer->GetConfigState()->GetShard(*sit);
        assert(shard != NULL);
        
        if (!databases.Get(shard->databaseID, database))
        {
            name.Writef("%U", shard->databaseID);
            name.NullTerminate();

            database = environment.GetDatabase(name.GetBuffer());
            databases.Set(shard->databaseID, database);
        }
        
        if (!tables.Get(shard->tableID, table))
        {
            name.Writef("%U", shard->tableID);
            name.NullTerminate();
            
            table = database->GetTable(name.GetBuffer());
            assert(table != NULL);
            tables.Set(shard->tableID, table);
        }
        
        // check if key already exists
        firstKey.Wrap(shard->firstKey);
        if (!table->ShardExists(firstKey))
            table->CreateShard(*sit, firstKey);
    }
}

#define CHECK_CMD()                                             \
	if (readPaxosID > paxosID ||                                \
	(readPaxosID == paxosID && readCommandID >= commandID))     \
		break;

#define FAIL()                                                  \
    {                                                           \
    if (request)                                                \
        request->response.Failed();                             \
    break;                                                      \
    }

void ShardDatabaseManager::OnClientReadRequest(ClientRequest* request)
{
    uint64_t        paxosID;
    uint64_t        commandID;
    StorageTable*   table;
    ReadBuffer      key;
    ReadBuffer      value;
    ReadBuffer      userValue;

    table = shardServer->GetDatabaseAdapter()->GetTable(request->tableID);
    if (!table)
    {
        request->response.Failed();
        return;
    }

    key.Wrap(request->key);
    if (!table->Get(key, value))
    {
        request->response.Failed();
        request->OnComplete();
        return;
    }
    
    ReadValue(value, paxosID, commandID, userValue);
    
    request->response.Value(userValue);
}

void ShardDatabaseManager::ExecuteWriteMessage(
 uint64_t paxosID, uint64_t commandID, ShardMessage& message, ClientRequest* request)
{
    uint64_t        readPaxosID;
    uint64_t        readCommandID;
    int64_t         number;
    unsigned        nread;
    ReadBuffer      userValue;
    ReadBuffer      readBuffer;
    Buffer          buffer;
    Buffer          numberBuffer;
    StorageTable*   table;

    table = shardServer->GetDatabaseAdapter()->GetTable(message.tableID);
    if (!table)
        ASSERT_FAIL();

    // TODO: differentiate return status (FAILED, NOSERVICE)
    switch (message.type)
    {
        case SHARDMESSAGE_SET:
            WriteValue(buffer, paxosID, commandID, message.value);
            if (!table->Set(message.key, buffer))
                FAIL();
            break;
        case SHARDMESSAGE_SET_IF_NOT_EXISTS:
            if (table->Get(message.key, readBuffer))
                FAIL();
            WriteValue(buffer, paxosID, commandID, message.value);
            if (!table->Set(message.key, buffer))
                FAIL();
            break;
        case SHARDMESSAGE_TEST_AND_SET:
            if (!table->Get(message.key, readBuffer))
                FAIL();
            ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
            CHECK_CMD();
            if (ReadBuffer::Cmp(userValue, message.test) != 0)
            {
                if (request)
                    request->response.Value(userValue);
                break;
            }
            WriteValue(buffer, paxosID, commandID, message.value);
            if (!table->Set(message.key, buffer))
                FAIL();
            if (request)
                request->response.Value(message.value);
            break;
        case SHARDMESSAGE_ADD:
            if (!table->Get(message.key, readBuffer))
                FAIL();
            ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
            CHECK_CMD();
            number = BufferToInt64(userValue.GetBuffer(), userValue.GetLength(), &nread);
            if (nread != userValue.GetLength())
                FAIL();
            number += message.number;
            numberBuffer.Writef("%I", number);
            WriteValue(buffer, paxosID, commandID, ReadBuffer(numberBuffer));
            if (!table->Set(message.key, buffer))
                FAIL();
            if (request)
                request->response.Number(number);
            break;
        case SHARDMESSAGE_DELETE:
            if (!table->Delete(message.key))
            {
                if (request)
                    request->response.Failed();
            }
            break;
        case SHARDMESSAGE_REMOVE:
            if (table->Get(message.key, readBuffer))
            {
                ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
                request->response.Value(userValue);
            }
            if (!table->Delete(message.key))
            {
                if (request)
                    request->response.Failed();
            }
            break;
        default:
            ASSERT_FAIL();
            break;
    }
}

#undef CHECK_CMD
#undef FAIL
