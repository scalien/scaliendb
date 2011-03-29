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

static bool LessThan(const ClientRequest& a, const ClientRequest& b)
{
    return Buffer::Cmp(a.key, b.key) < 0;
}

void ShardDatabaseAsyncGet::OnRequestComplete()
{
    uint64_t        paxosID;
    uint64_t        commandID;
    ReadBuffer      userValue;

    active = false;

    if (!ret || !request->session->IsActive())
    {
        if (!request->session->IsActive())
            request->response.NoResponse();
        else
            request->response.Failed();
            
        request->OnComplete();
        if (async && !manager->executeReads.IsActive())
            EventLoop::Add(&manager->executeReads);
        return;
    }
    
    ReadValue(value, paxosID, commandID, userValue);    
    request->response.Value(userValue);
    request->OnComplete();

    if (async && !manager->executeReads.IsActive())
        EventLoop::Add(&manager->executeReads);
}

void ShardDatabaseAsyncList::OnShardComplete()
{
    uint64_t                paxosID;
    uint64_t                commandID;
    ReadBuffer              userValue;
    StorageFileKeyValue*    it;
    unsigned                numKeys;
    unsigned                i;

    // handle disconnected
    if (lastResult->final && !request->session->IsActive())
    {
        OnRequestComplete();
        return;
    }

    numKeys = lastResult->dataPage.GetNumKeys();
    total += numKeys;
    // create results
    if (numKeys > 0 && (type == KEY || type == KEYVALUE))
    {
		CLIENTRESPONSE_ASSERT_NUMKEYS(numKeys);
        ReadBuffer  keys[CLIENTRESPONSE_NUMKEYS(numKeys)];
        ReadBuffer  values[CLIENTRESPONSE_NUMKEYS(numKeys)];
        i = 0;
        FOREACH (it, lastResult->dataPage)
        {
            keys[i] = it->GetKey();
            if (type == KEYVALUE)
            {
                userValue = it->GetValue();
                ReadValue(userValue, paxosID, commandID, values[i]);
            }
            i++;
        }

        if (type == KEYVALUE)
            request->response.ListKeyValues(numKeys, keys, values);
        else
            request->response.ListKeys(numKeys, keys);
        request->OnComplete(false);
    }

    // found count items
    if (lastResult->final && request->count != 0 && total == request->count)
    {
        OnRequestComplete();
        return;
    }

    if (lastResult->final)
        TryNextShard();
}

void ShardDatabaseAsyncList::OnRequestComplete()
{
    uint64_t    number;
    
    number = total;
    
    total = 0;
    num = 0;
    active = false;

    // handle disconnected
    if (lastResult->final && !request->session->IsActive())
    {
        request->response.NoResponse();            
        request->OnComplete();
        if (async && !manager->executeLists.IsActive())
            EventLoop::Add(&manager->executeLists);
        
        return;
    }
        
    if (lastResult->final)
    {
        if (type == COUNT)
            request->response.Number(number);
        else
            request->response.OK();
            
        request->OnComplete(true);
    }
    
    if (async && lastResult->final && !manager->executeLists.IsActive())
        EventLoop::Add(&manager->executeLists);
}

void ShardDatabaseAsyncList::TryNextShard()
{
    ConfigState*    configState;
    ConfigTable*    configTable;
    ConfigShard*    configShard;
    ReadBuffer      minKey;
    uint64_t        nextShardID;
    uint64_t*       itShard;
    
    if (shardLastKey.GetLength() == 0)
    {
        // this was the last shard
        OnRequestComplete();
        return;
    }
    
    configState = manager->GetConfigState();
    configTable = configState->GetTable(request->tableID);
    ASSERT(configTable != NULL);
    
    // find next shard
    FOREACH (itShard, configTable->shards)
    {
        configShard = configState->GetShard(*itShard);
        if (STORAGE_KEY_GREATER_THAN(configShard->firstKey, shardLastKey))
        {
            if (minKey.GetLength() == 0 || STORAGE_KEY_LESS_THAN(minKey, configShard->firstKey))
            {
                minKey = configShard->firstKey;
                nextShardID = *itShard;
            }
        }
    }
    
    Log_Debug("minKey: %R", &minKey);
    
    // found the next shard
    if (minKey.GetLength() != 0)
    {
        // TODO: HACK updating the request
        request->key.Write(minKey);
        request->offset = offset;
        
        // reschedule request
        manager->OnClientListRequest(request);
    }
    else
        OnRequestComplete();
}

ShardDatabaseManager::ShardDatabaseManager()
{
    yieldStorageThreadsTimer.SetDelay(SHARD_DATABASE_YIELD_THREADS_TIMEOUT);
    yieldStorageThreadsTimer.SetCallable(MFUNC(ShardDatabaseManager, OnYieldStorageThreadsTimer));
    executeReads.SetCallable(MFUNC(ShardDatabaseManager, OnExecuteReads));
    executeLists.SetCallable(MFUNC(ShardDatabaseManager, OnExecuteLists));
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
    
    // Initialize async GET
    asyncGet.active = false;
    asyncGet.manager = this;

    // Initialize async LIST
    asyncList.active = false;
    asyncList.manager = this;
    asyncList.num = 0;
    asyncList.total = 0;
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

ConfigState* ShardDatabaseManager::GetConfigState()
{
    return shardServer->GetConfigState();
}

void ShardDatabaseManager::DeleteQuorumPaxosShard(uint64_t quorumID)
{
    StorageShardProxy*  shard;
    
    if (quorumPaxosShards.Get(quorumID, shard))
    {
        quorumPaxosShards.Remove(quorumID);
        delete shard;
    }
    
    environment.DeleteShard(QUORUM_DATABASE_QUORUM_LOG_CONTEXT, quorumID);
}

void ShardDatabaseManager::DeleteQuorumLogShard(uint64_t quorumID)
{
    StorageShardProxy*  shard;
    
    if (quorumLogShards.Get(quorumID, shard))
    {
        quorumLogShards.Remove(quorumID);
        delete shard;
    }

    environment.DeleteShard(QUORUM_DATABASE_QUORUM_PAXOS_CONTEXT, quorumID);
}

void ShardDatabaseManager::DeleteDataShards(uint64_t quorumID)
{
    ConfigQuorum*   configQuorum;
    uint64_t*       itShardID;
    
    configQuorum = shardServer->GetConfigState()->GetQuorum(quorumID);
    if (!configQuorum)
        return;
    
    FOREACH(itShardID, configQuorum->shards)
    {
        environment.DeleteShard(QUORUM_DATABASE_DATA_CONTEXT, *itShardID);        
    }
}

void ShardDatabaseManager::SetShards(SortedList<uint64_t>& shards)
{
    uint64_t*           sit;
    ConfigShard*        shard;

    FOREACH (sit, shards)
    {
        shard = shardServer->GetConfigState()->GetShard(*sit);
        assert(shard != NULL);
        
        if (!shard->isSplitCreating)
        {
            Log_Trace("Calling CreateShard() for shardID = %U", *sit);
            environment.CreateShard(QUORUM_DATABASE_DATA_CONTEXT, *sit, shard->tableID,
             shard->firstKey, shard->lastKey, true, false);
        }
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

void ShardDatabaseManager::RemoveDeletedDataShards(SortedList<uint64_t>& myShardIDs)
{
    uint64_t*                       itShardID;
    StorageEnvironment::ShardIDList storageShardIDs;
    
    environment.GetShardIDs(QUORUM_DATABASE_DATA_CONTEXT, storageShardIDs);
    
    FOREACH(itShardID, storageShardIDs)
    {
        if (!myShardIDs.Contains(*itShardID))
            environment.DeleteShard(QUORUM_DATABASE_DATA_CONTEXT, *itShardID);
    }
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
    readRequests.Add(request);

    environment.SetYieldThreads(true);

    if (!executeReads.IsActive())
        EventLoop::Add(&executeReads);
}

void ShardDatabaseManager::OnClientListRequest(ClientRequest* request)
{
    listRequests.Append(request);

    environment.SetYieldThreads(true);

    if (!executeLists.IsActive())
        EventLoop::Add(&executeLists);
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
    ConfigShard*    configShard;

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
        case SHARDMESSAGE_SPLIT_SHARD:
            environment.SplitShard(contextID, message.shardID, message.newShardID, message.splitKey);
            Log_Debug("Split shard, shard ID: %U, split key: %B, new shardID: %U, paxosID: %U",
             message.shardID, &message.splitKey, message.newShardID, paxosID);
            break;
         case SHARDMESSAGE_MIGRATION_BEGIN:
            Log_Debug("shardMigration BEGIN shardID = %U", message.shardID);
            configShard = shardServer->GetConfigState()->GetShard(message.shardID);
            assert(configShard != NULL);
            environment.DeleteShard(contextID, message.shardID);
            environment.CreateShard(
             contextID, configShard->shardID, configShard->tableID,
             configShard->firstKey, configShard->lastKey, true, false);
            break;
         case SHARDMESSAGE_MIGRATION_SET:
            environment.Set(contextID, message.shardID, message.key, message.value);
            break;
         case SHARDMESSAGE_MIGRATION_DELETE:
            environment.Delete(contextID, message.shardID, message.key);
            break;
        default:
            ASSERT_FAIL();
            break;
    }
}

void ShardDatabaseManager::OnYieldStorageThreadsTimer()
{
    Log_Trace("readRequests: %u", readRequests.GetLength());
    if (readRequests.GetLength() > SHARD_DATABASE_YIELD_LIST_LENGTH)
        environment.SetYieldThreads(true);
    else
        environment.SetYieldThreads(false);
    
    EventLoop::Add(&yieldStorageThreadsTimer);
}

void ShardDatabaseManager::OnExecuteReads()
{
    uint64_t        start;
    uint64_t        shardID;
    int16_t         contextID;
    ReadBuffer      key;
    ClientRequest*  itRequest;

    if (asyncGet.active)
        return;
    
    start = NowClock();

    FOREACH_FIRST (itRequest, readRequests)
    {
        // let other code run in the main thread every YIELD_TIME msec
        if (NowClock() - start >= YIELD_TIME)
        {
            if (executeReads.IsActive())
                STOP_FAIL(1, "Program bug: resumeRead.IsActive() should be false.");
            EventLoop::Add(&executeReads);
            return;
        }

        readRequests.Remove(itRequest);

        // silently drop requests from disconnected clients
        if (!itRequest->session->IsActive())
        {
            itRequest->response.NoResponse();
            itRequest->OnComplete();
            continue;
        }

        key.Wrap(itRequest->key);
        contextID = QUORUM_DATABASE_DATA_CONTEXT;
        shardID = environment.GetShardID(contextID, itRequest->tableID, key);

        asyncGet.request = itRequest;
        asyncGet.key = key;
        asyncGet.onComplete = MFunc<ShardDatabaseAsyncGet, &ShardDatabaseAsyncGet::OnRequestComplete>(&asyncGet);
        asyncGet.active = true;
        asyncGet.async = false;
        if (!environment.TryNonblockingGet(contextID, shardID, &asyncGet))
        {
            // HACK store timestamp for later comparison in order to avoid duplicate memo chunk search
            itRequest->changeTimeout = start;
            blockingReadRequests.Append(itRequest);
        }
    }
    
    FOREACH_FIRST (itRequest, blockingReadRequests)
    {
        // let other code run in the main thread every YIELD_TIME msec
        if (NowClock() - start >= YIELD_TIME)
        {
            if (executeReads.IsActive())
                STOP_FAIL(1, "Program bug: resumeRead.IsActive() should be false.");
            EventLoop::Add(&executeReads);
            return;
        }

        blockingReadRequests.Remove(itRequest);

        // silently drop requests from disconnected clients
        if (!itRequest->session->IsActive())
        {
            itRequest->response.NoResponse();
            itRequest->OnComplete();
            continue;
        }

        key.Wrap(itRequest->key);
        contextID = QUORUM_DATABASE_DATA_CONTEXT;
        shardID = environment.GetShardID(contextID, itRequest->tableID, key);

        asyncGet.skipMemoChunk = false;
        if (itRequest->changeTimeout == start)
            asyncGet.skipMemoChunk = true;
        asyncGet.request = itRequest;
        asyncGet.key = key;
        asyncGet.onComplete = MFunc<ShardDatabaseAsyncGet, &ShardDatabaseAsyncGet::OnRequestComplete>(&asyncGet);
        asyncGet.active = true;
        asyncGet.async = false;
        environment.AsyncGet(contextID, shardID, &asyncGet);
        if (asyncGet.active)
        {
            // TODO: HACK
            asyncGet.async = true;
            return;
        }
    }
}

void ShardDatabaseManager::OnExecuteLists()
{
    uint64_t        start;
    uint64_t        shardID;
    int16_t         contextID;
    ReadBuffer      startKey;
    ClientRequest*  itRequest;
    ConfigShard*    configShard;

    if (asyncGet.active)
        return;
    
    start = NowClock();

    FOREACH_FIRST(itRequest, listRequests)
    {
        // let other code run in the main thread every YIELD_TIME msec
        if (NowClock() - start >= YIELD_TIME)
        {
            if (executeLists.IsActive())
                STOP_FAIL(1, "Program bug: resumeRead.IsActive() should be false.");
            EventLoop::Add(&executeLists);
            return;
        }

        listRequests.Remove(itRequest);

        // silently drop requests from disconnected clients
        if (!itRequest->session->IsActive())
        {
            itRequest->response.NoResponse();
            itRequest->OnComplete();
            continue;
        }


        startKey.Wrap(itRequest->key);
        contextID = QUORUM_DATABASE_DATA_CONTEXT;
        shardID = environment.GetShardID(contextID, itRequest->tableID, startKey);
        configShard = shardServer->GetConfigState()->GetShard(shardID);
        ASSERT(configShard != NULL);

        if (itRequest->type == CLIENTREQUEST_LIST_KEYS)
            asyncList.type = StorageAsyncList::KEY;
        else if (itRequest->type == CLIENTREQUEST_LIST_KEYVALUES)
            asyncList.type = StorageAsyncList::KEYVALUE;
        else if (itRequest->type == CLIENTREQUEST_COUNT)
            asyncList.type = StorageAsyncList::COUNT;
        else
            ASSERT_FAIL();

        // HACK: limit count to an acceptable value so that it won't hog the database
        if (itRequest->count == 0 && 
            (itRequest->type == CLIENTREQUEST_LIST_KEYS || 
             itRequest->type == CLIENTREQUEST_LIST_KEYVALUES))
        {
            itRequest->count = 1000;
        }

        asyncList.request = itRequest;
        asyncList.startKey = startKey;
        asyncList.count = itRequest->count;
        asyncList.offset = itRequest->offset;
        asyncList.shardLastKey.Write(configShard->lastKey);
        Log_Debug("shardLastKey: %B", &asyncList.shardLastKey);
        asyncList.onComplete = MFunc<ShardDatabaseAsyncList, &ShardDatabaseAsyncList::OnShardComplete>(&asyncList);
        asyncList.active = true;
        asyncList.async = false;
        environment.AsyncList(contextID, shardID, &asyncList);
        if (asyncList.active)
        {
            // TODO: HACK
            asyncList.async = true;
            return;
        }
    }
}
