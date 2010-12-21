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
    Buffer  envpath;
    
    shardServer = shardServer_;

    envpath.Writef("%s", configFile.GetValue("database.dir", "db"));
    environment.Open(envpath);

    // TODO: replace 1 with symbolic name
    systemShard.Init(&environment, QUORUM_DATABASE_SYSTEM_CONTEXT, 1);
    REPLICATION_CONFIG->Init(&systemShard);
}

void ShardDatabaseManager::Shutdown()
{
    environment.Close();
}

StorageEnvironment* ShardDatabaseManager::GetEnvironment()
{
    return &environment;
}

StorageShardProxy* ShardDatabaseManager::GetQuorumShard(uint64_t quorumID)
{
    StorageShardProxy*  shard;
    
    if (quorumShards.Get(quorumID, shard))
        return shard;

    return NULL;
}

StorageShardProxy* ShardDatabaseManager::GetDataShard(uint64_t shardID)
{
    ConfigShard*        configShard;
    StorageShardProxy*  dataShard;
    
    configShard = shardServer->GetConfigState()->GetShard(shardID);
    if (!configShard)
        return NULL;

    if (dataShards.Get(shardID, dataShard))
        return dataShard;
    
    return NULL;
}

void ShardDatabaseManager::SetShards(SortedList<uint64_t>& shards)
{
    uint64_t*           sit;
    ConfigShard*        shard;
    Buffer              name;
    ReadBuffer          firstKey;
    StorageShardProxy*  dataShard;
    StorageShardProxy*  quorumShard;

    FOREACH (sit, shards)
    {
        shard = shardServer->GetConfigState()->GetShard(*sit);
        assert(shard != NULL);
        
        dataShard = GetDataShard(shard->tableID);
        if (!dataShard)
        {
            environment.CreateShard(QUORUM_DATABASE_DATA_CONTEXT, *sit, shard->tableID,
             shard->firstKey, shard->lastKey, true);
            dataShard = new StorageShardProxy;
            dataShard->Init(&environment, QUORUM_DATABASE_DATA_CONTEXT, shard->shardID);
            dataShards.Set(shard->tableID, dataShard);
        }
        
        quorumShard = GetQuorumShard(shard->quorumID);
        if (!quorumShard)
        {
            environment.CreateShard(QUORUM_DATABASE_QUORUM_CONTEXT, *sit, shard->tableID,
             shard->firstKey, shard->lastKey, true);
            quorumShard = new StorageShardProxy;
            quorumShard->Init(&environment, QUORUM_DATABASE_QUORUM_CONTEXT, shard->shardID);
            quorumShards.Set(shard->quorumID, quorumShard);
        }
    }
}

void ShardDatabaseManager::RemoveDeletedDatabases()
{
// TODO:
//    DatabaseMap::Node*      it;
//    DatabaseMap::Node*      next;    
//    ConfigState*            configState;
//
//    configState = shardServer->GetConfigState();
//
//    for (it = databases.First(); it != NULL; it = next)
//    {
//        next = databases.Next(it);
//        if (!configState->GetDatabase(it->Key()))
//        {
//            environment.DeleteDatabase(it->Value());
//            databases.Remove(it->Key());
//        }
//    }
}

void ShardDatabaseManager::RemoveDeletedTables()
{
// TODO:
//    TableMap::Node*     it;
//    TableMap::Node*     next;    
//    ConfigState*        configState;
//    StorageDatabase*    database;
//
//    configState = shardServer->GetConfigState();
//
//    for (it = tables.First(); it != NULL; it = next)
//    {
//        next = tables.Next(it);
//        if (!configState->GetTable(it->Key()))
//        {
//            database = it->Value()->GetDatabase();
//            database->DeleteTable(it->Value());
//            tables.Remove(it->Key());
//        }
//    }
}

#define CHECK_CMD()                                             \
	if (readPaxosID > paxosID ||                                \
	(readPaxosID == paxosID && readCommandID >= commandID))     \
		break;

#define RESPONSE_FAIL()                                         \
    {                                                           \
    if (request)                                                \
        request->response.Failed();                             \
    break;                                                      \
    }

void ShardDatabaseManager::OnClientReadRequest(ClientRequest* request)
{
    uint64_t        paxosID;
    uint64_t        commandID;
    ReadBuffer      key;
    ReadBuffer      value;
    ReadBuffer      userValue;
    StorageShardProxy* shard;

    shard = GetDataShard(request->tableID);
    if (!shard)
    {
        request->response.Failed();
        return;        
    }

    key.Wrap(request->key);
    if (!shard->Get(key, value))
    {
        request->response.Failed();
        return;
    }
    
    ReadValue(value, paxosID, commandID, userValue);    
    request->response.Value(userValue);
}

void ShardDatabaseManager::ExecuteMessage(
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
    Buffer          tmpBuffer;
    StorageShardProxy* shard;

    // TODO: shard splitting

    shard = GetDataShard(message.tableID);
    if (!shard)
        ASSERT_FAIL();

    if (request)
        request->response.OK();

    // TODO: differentiate return status (FAILED, NOSERVICE)
    switch (message.type)
    {
        case SHARDMESSAGE_SET:
            WriteValue(buffer, paxosID, commandID, message.value);
            if (!shard->Set(message.key, buffer))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_SET_IF_NOT_EXISTS:
            if (shard->Get(message.key, readBuffer))
                RESPONSE_FAIL();
            WriteValue(buffer, paxosID, commandID, message.value);
            if (!shard->Set(message.key, buffer))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_TEST_AND_SET:
            if (!shard->Get(message.key, readBuffer))
                RESPONSE_FAIL();
            ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
            CHECK_CMD();
            if (ReadBuffer::Cmp(userValue, message.test) != 0)
            {
                if (request)
                    request->response.Value(userValue);
                break;
            }
            WriteValue(buffer, paxosID, commandID, message.value);
            if (!shard->Set(message.key, buffer))
                RESPONSE_FAIL();
            if (request)
                request->response.Value(message.value);
            break;
        case SHARDMESSAGE_GET_AND_SET:
            if (shard->Get(message.key, readBuffer))
            {
                ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
                if (request)
                {
                    request->response.Value(userValue);
                    request->response.CopyValue();
                }
            }
            else
            {
                if (request)
                    request->response.Failed();
            }
            WriteValue(buffer, paxosID, commandID, message.value);
            if (!shard->Set(message.key, buffer))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_ADD:
         
            if (!shard->Get(message.key, readBuffer))
                RESPONSE_FAIL();
            ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
            CHECK_CMD();
            number = BufferToInt64(userValue.GetBuffer(), userValue.GetLength(), &nread);
            if (nread != userValue.GetLength())
                RESPONSE_FAIL();
            number += message.number;
            numberBuffer.Writef("%I", number);
            WriteValue(buffer, paxosID, commandID, ReadBuffer(numberBuffer));
            if (!shard->Set(message.key, buffer))
                RESPONSE_FAIL();
            if (request)
                request->response.Number(number);
            break;
        case SHARDMESSAGE_APPEND:
            if (!shard->Get(message.key, readBuffer))
                RESPONSE_FAIL();
            ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
            CHECK_CMD();
            tmpBuffer.Write(userValue);
            tmpBuffer.Append(message.value);
            WriteValue(buffer, paxosID, commandID, ReadBuffer(tmpBuffer));
            if (!shard->Set(message.key, buffer))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_DELETE:
            if (!shard->Delete(message.key))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_REMOVE:
            if (shard->Get(message.key, readBuffer))
            {
                ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
                request->response.Value(userValue);
            }
            if (!shard->Delete(message.key))
                RESPONSE_FAIL();
            break;
//        case SHARDMESSAGE_SPLIT_SHARD:
//            shard = GetShard(message.shardID);
//            if (!shard)
//                ASSERT_FAIL();
//            readBuffer.Wrap(message.splitKey);
//            shard->SplitShard(message.newShardID, readBuffer);
//            Log_Message("Split shard, shard ID: %U, split key: %B , new shardID: %U",
//             message.shardID, &message.splitKey, message.newShardID);
//            break;
        default:
            ASSERT_FAIL();
            break;
    }
}

#undef CHECK_CMD
#undef FAIL
