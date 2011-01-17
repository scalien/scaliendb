#include "ShardDatabaseManager.h"
#include "System/Events/EventLoop.h"
#include "ShardServer.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Storage/StoragePageCache.h"

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

ShardDatabaseManager::ShardDatabaseManager()
{
    yieldStorageThreadsTimer.SetDelay(SHARD_DATABASE_YIELD_THREADS_TIMEOUT);
    yieldStorageThreadsTimer.SetCallable(MFUNC(ShardDatabaseManager, OnYieldStorageThreadsTimer));
    executeReads.SetCallable(MFUNC(ShardDatabaseManager, OnExecuteReads));
}

void ShardDatabaseManager::Init(ShardServer* shardServer_)
{
    Buffer  envpath;
    
    shardServer = shardServer_;

    EventLoop::Add(&yieldStorageThreadsTimer);

    envpath.Writef("%s", configFile.GetValue("database.dir", "db"));
    environment.Open(envpath);

    // TODO: replace 1 with symbolic name
    systemShard.Init(&environment, QUORUM_DATABASE_SYSTEM_CONTEXT, 1);
    REPLICATION_CONFIG->Init(&systemShard);
}

void ShardDatabaseManager::Shutdown()
{
    ShardMap::Node*     node;
    
    FOREACH (node, quorumPaxosShards)
        delete node->Value();
    FOREACH (node, quorumLogShards)
        delete node->Value();
    
    readRequests.DeleteList();
    environment.Close();
    StoragePageCache::Shutdown();
}

StorageEnvironment* ShardDatabaseManager::GetEnvironment()
{
    return &environment;
}

StorageShardProxy* ShardDatabaseManager::GetQuorumPaxosShard(uint64_t quorumID)
{
    StorageShardProxy*  shard;
    
    if (quorumPaxosShards.Get(quorumID, shard))
        return shard;

    return NULL;
}

StorageShardProxy* ShardDatabaseManager::GetQuorumLogShard(uint64_t quorumID)
{
    StorageShardProxy*  shard;
    
    if (quorumLogShards.Get(quorumID, shard))
        return shard;

    return NULL;
}

void ShardDatabaseManager::SetShards(SortedList<uint64_t>& shards)
{
    uint64_t*           sit;
    ConfigShard*        shard;

    FOREACH (sit, shards)
    {
        shard = shardServer->GetConfigState()->GetShard(*sit);
        assert(shard != NULL);
        
        environment.CreateShard(QUORUM_DATABASE_DATA_CONTEXT, *sit, shard->tableID,
         shard->firstKey, shard->lastKey, true, false);
    }
}

void ShardDatabaseManager::SetQuorumShards(uint64_t quorumID)
{
    StorageShardProxy*  quorumShard;

    quorumShard = GetQuorumPaxosShard(quorumID);
    if (!quorumShard)
    {
        // TODO: HACK
        environment.CreateShard(QUORUM_DATABASE_QUORUM_PAXOS_CONTEXT, quorumID, 0,
         "", "", true, false);
        quorumShard = new StorageShardProxy;
        quorumShard->Init(&environment, QUORUM_DATABASE_QUORUM_PAXOS_CONTEXT, quorumID);
        quorumPaxosShards.Set(quorumID, quorumShard);
    }

    quorumShard = GetQuorumLogShard(quorumID);
    if (!quorumShard)
    {
        // TODO: HACK
        environment.CreateShard(QUORUM_DATABASE_QUORUM_LOG_CONTEXT, quorumID, 0,
         "", "", true, true);
        quorumShard = new StorageShardProxy;
        quorumShard->Init(&environment, QUORUM_DATABASE_QUORUM_LOG_CONTEXT, quorumID);
        quorumLogShards.Set(quorumID, quorumShard);
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
    readRequests.Append(request);

    if (!executeReads.IsActive())
        EventLoop::Add(&executeReads);
}

void ShardDatabaseManager::ExecuteMessage(
 uint64_t paxosID, uint64_t commandID, ShardMessage& message, ClientRequest* request)
{
    uint64_t        readPaxosID;
    uint64_t        readCommandID;
    uint64_t        shardID;
    int16_t         contextID;
    int64_t         number;
    unsigned        nread;
    ReadBuffer      userValue;
    ReadBuffer      readBuffer;
    Buffer          buffer;
    Buffer          numberBuffer;
    Buffer          tmpBuffer;

    // TODO: shard splitting

    contextID = QUORUM_DATABASE_DATA_CONTEXT;
    shardID = environment.GetShardID(contextID, message.tableID, message.key);

    if (request)
        request->response.OK();

    // TODO: differentiate return status (FAILED, NOSERVICE)
    switch (message.type)
    {
        case SHARDMESSAGE_SET:
            WriteValue(buffer, paxosID, commandID, message.value);
            if (!environment.Set(contextID, shardID, message.key, buffer))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_SET_IF_NOT_EXISTS:
            if (environment.Get(contextID, shardID, message.key, readBuffer))
                RESPONSE_FAIL();
            WriteValue(buffer, paxosID, commandID, message.value);
            if (!environment.Set(contextID, shardID, message.key, buffer))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_TEST_AND_SET:
            if (!environment.Get(contextID, shardID, message.key, readBuffer))
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
            if (!environment.Set(contextID, shardID, message.key, buffer))
                RESPONSE_FAIL();
            if (request)
                request->response.Value(message.value);
            break;
        case SHARDMESSAGE_GET_AND_SET:
            if (environment.Get(contextID, shardID, message.key, readBuffer))
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
            if (!environment.Set(contextID, shardID, message.key, buffer))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_ADD:
            if (!environment.Get(contextID, shardID, message.key, readBuffer))
                RESPONSE_FAIL();
            ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
            CHECK_CMD();
            number = BufferToInt64(userValue.GetBuffer(), userValue.GetLength(), &nread);
            if (nread != userValue.GetLength())
                RESPONSE_FAIL();
            number += message.number;
            numberBuffer.Writef("%I", number);
            WriteValue(buffer, paxosID, commandID, ReadBuffer(numberBuffer));
            if (!environment.Set(contextID, shardID, message.key, buffer))
                RESPONSE_FAIL();
            if (request)
                request->response.Number(number);
            break;
        case SHARDMESSAGE_APPEND:
            if (!environment.Get(contextID, shardID, message.key, readBuffer))
                RESPONSE_FAIL();
            ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
            CHECK_CMD();
            tmpBuffer.Write(userValue);
            tmpBuffer.Append(message.value);
            WriteValue(buffer, paxosID, commandID, ReadBuffer(tmpBuffer));
            if (!environment.Set(contextID, shardID, message.key, buffer))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_DELETE:
            if (!environment.Delete(contextID, shardID, message.key))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_REMOVE:
            if (environment.Get(contextID, shardID, message.key, readBuffer))
            {
                ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
                request->response.Value(userValue);
            }
            if (!environment.Delete(contextID, shardID, message.key))
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

void ShardDatabaseManager::OnYieldStorageThreadsTimer()
{
    if (readRequests.GetLength() > SHARD_DATABASE_YIELD_LIST_LENGTH)
        environment.SetYieldThreads(true);
    else
        environment.SetYieldThreads(false);
    
    EventLoop::Add(&yieldStorageThreadsTimer);
}

void ShardDatabaseManager::OnExecuteReads()
{
    uint64_t        start;
    uint64_t        paxosID;
    uint64_t        commandID;
    uint64_t        shardID;
    int16_t         contextID;
    ReadBuffer      key;
    ReadBuffer      value;
    ReadBuffer      userValue;
    ClientRequest*  itRequest;

    start = NowClock();

    FOREACH_FIRST(itRequest, readRequests)
    {
        if (NowClock() - start >= SHARD_DATABASE_YIELD_TIME)
        {
            // let other code run every YIELD_TIME msec
            if (executeReads.IsActive())
                STOP_FAIL(1, "Program bug: resumeRead.IsActive() should be false.");
            EventLoop::Add(&executeReads);
            break;
        }

        readRequests.Remove(itRequest);

        if (!itRequest->session->IsActive())
        {
            itRequest->OnComplete();
            continue;
        }

        key.Wrap(itRequest->key);
        contextID = QUORUM_DATABASE_DATA_CONTEXT;
        shardID = environment.GetShardID(contextID, itRequest->tableID, key);

        if (!environment.Get(contextID, shardID, key, value))
        {
            itRequest->response.Failed();
            itRequest->OnComplete();
            continue;
        }
        
        ReadValue(value, paxosID, commandID, userValue);    
        itRequest->response.Value(userValue);
        itRequest->OnComplete();
    }
}

#undef CHECK_CMD
#undef FAIL
