#include "ShardDatabaseManager.h"
#include "System/Events/EventLoop.h"
#include "ShardServer.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Storage/StoragePageCache.h"

#define SHARD_MIGRATION_WRITER  (shardServer->GetShardMigrationWriter())

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

/*
===============================================================================================

 ShardDatabaseSequence

===============================================================================================
*/

static inline const ShardDatabaseSequence* Key(const ShardDatabaseSequence* seq)
{
    return seq;
}

static inline int KeyCmp(const ShardDatabaseSequence* a, const ShardDatabaseSequence* b)
{
    if (a->tableID < b->tableID)
        return -1;
    if (a->tableID > b->tableID)
        return 1;
    
    return Buffer::Cmp(a->key, b->key);
}

ShardDatabaseSequence::ShardDatabaseSequence()
{
    tableID = 0;
    nextValue = 0;
    remaining = 0;
}

/*
===============================================================================================

 ShardDatabaseAsyncGet

===============================================================================================
*/

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


/*
===============================================================================================

 ShardDatabaseAsyncList

===============================================================================================
*/

ShardDatabaseAsyncList::ShardDatabaseAsyncList(ShardDatabaseManager* manager_)
{
    manager = manager_;
    request = NULL;
    total = 0;

    next = prev = this;
}

void ShardDatabaseAsyncList::SetRequest(ClientRequest* request_)
{
    request = request_;
}

void ShardDatabaseAsyncList::SetTotal(uint64_t total_)
{
    total = total_;
}

void ShardDatabaseAsyncList::OnShardComplete()
{
    uint64_t                paxosID;
    uint64_t                commandID;
    ReadBuffer              userValue;
    StorageFileKeyValue*    it;
    ReadBuffer*             keys;
    ReadBuffer*             values;
    unsigned                numKeys;
    unsigned                i;

    // possibly an error happened or already disconnected
    if (!lastResult || !request || !request->IsActive())
    {
        Log_Debug("OnShardComplete error");
        OnRequestComplete();
        return;
    }

    Log_Debug("OnShardComplete, final: %b", lastResult->final);

    numKeys = lastResult->numKeys;
    total += numKeys;

    if (numKeys > 0 && (type == KEY || type == KEYVALUE))
    {
        ReadBuffer fk = lastResult->dataPage.First()->GetKey();
        Log_Debug("numKeys: %u, firstKey: %R", numKeys, &fk);
    }

    // create results if necessary
    if (numKeys > 0 && (type == KEY || type == KEYVALUE))
    {
        keys = new ReadBuffer[numKeys];
        values = new ReadBuffer[numKeys];
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
        
        delete[] keys;
        delete[] values;
    }

    // found all items given by 'count'
    if (lastResult->final && request->count != 0 && total == request->count)
    {
        Log_Debug("OnShardComplete count found");
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
    
    // reset async list variables
    total = 0;
    num = 0;

    // handle error from sync callback
    if (!lastResult)
    {
        request->response.NoResponse();
        if (request->session->IsActive())
            request->response.Failed();
            
        request->OnComplete();
        request = NULL;
        manager->inactiveAsyncLists.Append(this);
        return;
    }
    
    // already disconnected
    if (!request)
    {
        manager->inactiveAsyncLists.Append(this);
        goto ActivateExecuteList;
    }
    
    // handle disconnected
    if (!request->IsActive())
    {
        SetAborted(true);
        request->response.NoResponse();
        request->OnComplete();
        request = NULL;
        manager->inactiveAsyncLists.Append(this);
        goto ActivateExecuteList;        
    }

    // send final message
    if (lastResult->final)
    {
        if (type == COUNT)
        {
            request->response.Number(number);
            request->OnComplete(false);
        }

        request->response.OK();
        request->OnComplete(true);
        request = NULL;
        manager->inactiveAsyncLists.Append(this);
    }

ActivateExecuteList:
    // activate async list executor
    if (lastResult->final && !manager->executeLists.IsActive())
        EventLoop::Add(&manager->executeLists);
}

void ShardDatabaseAsyncList::TryNextShard()
{
    ReadBuffer  rbShardLastKey;

    if (forwardDirection)
        rbShardLastKey.Wrap(shardLastKey);
    else
        rbShardLastKey.Wrap(shardFirstKey);

    Log_Debug("TryNextShard: shardLastKey: %R", &rbShardLastKey);
    
    // check if this was the last shard
    if (rbShardLastKey.GetLength() == 0)
    {
        OnRequestComplete();
        return;
    }

    // check if shardLastKey matches prefix
    if (prefix.GetLength() > 0 && !rbShardLastKey.BeginsWith(prefix))
    {
        OnRequestComplete();
        return;
    }

    // send COUNT partial result
    if (request->type == CLIENTREQUEST_COUNT)
    {
        request->response.Number(total);
        request->OnComplete(false);
    }
    
    // TODO: HACK: Find next shard based on rbShardLastKey when iterating backwards
    if (!forwardDirection)
        request->findByLastKey = true;

    // update the request with the next shard's startKey
    request->key.Write(rbShardLastKey);
    if (request->count > 0)
        request->count -= total;

    // reset async list variables
    total = 0;
    num = 0;
    
    // reschedule request
    manager->OnClientListRequest(request);
    manager->inactiveAsyncLists.Append(this);
    request = NULL;
}

bool ShardDatabaseAsyncList::IsActive()
{
    if (request != NULL)
        return true;
    return false;
}

/*
===============================================================================================

 ShardDatabaseManager

===============================================================================================
*/

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
    if (REPLICATION_CONFIG->GetNodeID() > 0)
        Log_Message("My nodeID is %U", REPLICATION_CONFIG->GetNodeID());
    
    // Initialize async GET
    asyncGet.active = false;
    asyncGet.manager = this;

    // Initialize async LIST operations
    for (int i = 0; i < configFile.GetIntValue("database.numAsyncThreads", 10); i++)
    {
        ShardDatabaseAsyncList* asyncList = new ShardDatabaseAsyncList(this);
        inactiveAsyncLists.Append(asyncList);
    }
}

void ShardDatabaseManager::Shutdown()
{
    ShardMap::Node*     node;
    
    FOREACH (node, quorumPaxosShards)
        delete node->Value();
    FOREACH (node, quorumLogShards)
        delete node->Value();
    
    // TODO: delete async list ops
    //asyncList.Clear();
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
    
    FOREACH (itShardID, configQuorum->shards)
        environment.DeleteShard(QUORUM_DATABASE_DATA_CONTEXT, *itShardID);
}

void ShardDatabaseManager::SetShards(SortedList<uint64_t>& shards)
{
    uint64_t*           sit;
    ConfigShard*        shard;

    FOREACH (sit, shards)
    {
        shard = shardServer->GetConfigState()->GetShard(*sit);
        ASSERT(shard != NULL);
        
        if (shard->state == CONFIG_SHARD_STATE_NORMAL)
        {
            //Log_Trace("Calling CreateShard() for shardID = %U", *sit);
            environment.CreateShard(shard->quorumID, QUORUM_DATABASE_DATA_CONTEXT, *sit, shard->tableID,
             shard->firstKey, shard->lastKey, true, STORAGE_SHARD_TYPE_STANDARD);
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
        environment.CreateShard(quorumID, QUORUM_DATABASE_QUORUM_PAXOS_CONTEXT, quorumID, 0,
         "", "", true, STORAGE_SHARD_TYPE_DUMP);
        quorumShard = new StorageShardProxy;
        quorumShard->Init(&environment, QUORUM_DATABASE_QUORUM_PAXOS_CONTEXT, quorumID);
        quorumPaxosShards.Set(quorumID, quorumShard);
    }

    quorumShard = GetQuorumLogShard(quorumID);
    if (!quorumShard)
    {
        // TODO: HACK
        environment.CreateShard(quorumID, QUORUM_DATABASE_QUORUM_LOG_CONTEXT, quorumID, 0,
         "", "", true, STORAGE_SHARD_TYPE_LOG);
        quorumShard = new StorageShardProxy;
        quorumShard->Init(&environment, QUORUM_DATABASE_QUORUM_LOG_CONTEXT, quorumID);
        quorumLogShards.Set(quorumID, quorumShard);
    }
}

void ShardDatabaseManager::RemoveDeletedDataShards(SortedList<uint64_t>& myShardIDs)
{
    uint64_t    shardID;
    Buffer      storageShardIDs;
    ReadBuffer  parse;
    
    environment.GetShardIDs(QUORUM_DATABASE_DATA_CONTEXT, storageShardIDs);
    parse.Wrap(storageShardIDs);
    
    while (parse.ReadLittle64(shardID))
    {
        parse.Advance(sizeof(uint64_t));
        if (!myShardIDs.Contains(shardID))
        {
            if (SHARD_MIGRATION_WRITER->IsActive() && SHARD_MIGRATION_WRITER->GetShardID() == shardID)
                SHARD_MIGRATION_WRITER->Abort();

            environment.DeleteShard(QUORUM_DATABASE_DATA_CONTEXT, shardID);
        }
    }
}

void ShardDatabaseManager::OnClientReadRequest(ClientRequest* request)
{
//    readRequests.Add(request);
    readRequests.Append(request);

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

bool ShardDatabaseManager::OnClientSequenceNext(ClientRequest* request)
{
    int                     cmpres;
    ShardDatabaseSequence   query;
    ShardDatabaseSequence*  it;

    query.tableID = request->tableID;
    query.key.Write(request->key);
    it = sequences.Locate(&query, cmpres);
    if (cmpres == 0 && it != NULL)
    {
        ASSERT(it->remaining > 0);
        
        request->response.Number(it->nextValue);
        request->OnComplete();

        it->nextValue++;
        it->remaining--;

        if (it->remaining == 0)
            sequences.Delete(it);

        return true;
    }

    return false;
}

uint64_t ShardDatabaseManager::ExecuteMessage(uint64_t quorumID, uint64_t paxosID, uint64_t commandID,
 ShardMessage& message)
{
#define CHECK_CMD()                                             \
{                                                               \
	if (readPaxosID > paxosID ||                                \
	 (readPaxosID == paxosID && readCommandID >= commandID))    \
		break;                                                  \
}

#define RESPONSE_FAIL()                                         \
{                                                               \
    if (message.clientRequest)                                  \
        message.clientRequest->response.Failed();               \
    break;                                                      \
}

#define CHECK_SHARDID()                                         \
{                                                               \
    if (shardID == 0)                                           \
    {                                                           \
        if (message.clientRequest)                              \
            message.clientRequest->response.BadSchema();        \
        break;                                                  \
    }                                                           \
}

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
    ShardDatabaseSequence* sequence;
    Buffer          shardIDs;
    ReadBuffer      parse;
    
    contextID = QUORUM_DATABASE_DATA_CONTEXT;
    shardID = 0;

    if (message.clientRequest)
    {
        message.clientRequest->response.OK();
        message.clientRequest->response.paxosID = paxosID;
    }

    // TODO: differentiate return status (FAILED, NOSERVICE)
    switch (message.type)
    {
        case SHARDMESSAGE_SET:
            shardID = environment.GetShardID(contextID, message.tableID, message.key);
            CHECK_SHARDID();
            WriteValue(buffer, paxosID, commandID, message.value);
            if (!environment.Set(contextID, shardID, message.key, buffer))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_ADD:
        case SHARDMESSAGE_SEQUENCE_ADD:
            shardID = environment.GetShardID(contextID, message.tableID, message.key);
            CHECK_SHARDID();
            if (environment.Get(contextID, shardID, message.key, readBuffer))
            {
                // GET succeeded, key exists, try to parse number
                ReadValue(readBuffer, readPaxosID, readCommandID, userValue);
                CHECK_CMD();
                number = BufferToInt64(userValue.GetBuffer(), userValue.GetLength(), &nread);
                if (nread != userValue.GetLength())
                    RESPONSE_FAIL();
            }
            else
            {
                // GET failed, key does not exist; assume value is 0
                if (message.type == SHARDMESSAGE_ADD)
                    number = 0;
                else // SHARDMESSAGE_SEQUENCE_ADD
                    number = 1;
            }
            number += message.number;
            numberBuffer.Writef("%I", number);
            WriteValue(buffer, paxosID, commandID, ReadBuffer(numberBuffer));
            if (!environment.Set(contextID, shardID, message.key, buffer))
                RESPONSE_FAIL();
            if (message.clientRequest)
            {
                if (message.type == SHARDMESSAGE_ADD)
                {
                    message.clientRequest->response.SignedNumber(number);
                }
                else // SHARDMESSAGE_SEQUENCE_ADD
                {
                    sequence = new ShardDatabaseSequence;
                    sequence->tableID = message.tableID;
                    sequence->key.Write(message.key);
                    sequence->nextValue = number - message.number;
                    sequence->remaining = message.number;
                    sequences.Insert<const ShardDatabaseSequence*>(sequence);
                }
            }
            break;
        case SHARDMESSAGE_DELETE:
            shardID = environment.GetShardID(contextID, message.tableID, message.key);
            CHECK_SHARDID();
            if (!environment.Delete(contextID, shardID, message.key))
                RESPONSE_FAIL();
            break;
        case SHARDMESSAGE_SPLIT_SHARD:
            environment.SplitShard(contextID, message.shardID, message.newShardID, message.splitKey);
            Log_Debug("Splitting shard %U into shards %U and %U at key: %B (paxosID: %U)",
             message.shardID, message.shardID, message.newShardID, &message.splitKey, paxosID);
            break;
        case SHARDMESSAGE_TRUNCATE_TABLE:
            environment.GetShardIDs(contextID, message.tableID, shardIDs);
            parse.Wrap(shardIDs);
            while (parse.ReadLittle64(shardID))
            {
                parse.Advance(sizeof(uint64_t));
                environment.DeleteShard(contextID, shardID);
            }
            environment.CreateShard(
             quorumID, contextID, message.newShardID, message.tableID, "", "", true, STORAGE_SHARD_TYPE_STANDARD);
            break;
        case SHARDMESSAGE_MIGRATION_BEGIN:
            Log_Debug("shardMigration BEGIN shardID = %U", message.srcShardID);
            configShard = shardServer->GetConfigState()->GetShard(message.srcShardID);
            if (configShard)
            {
                // this is not completely right
                // we can only split the shard here if
                // 1) our configState has the previous shard
                // 2) the storage engine has the previous shard
                // problems occur if after restart the node re-executes the last
                // paxos message, but the the shard has already been split
                environment.CreateShard(quorumID, 
                 contextID, message.dstShardID, configShard->tableID,
                 configShard->firstKey, configShard->lastKey, true, STORAGE_SHARD_TYPE_STANDARD);
            }
            break;
         case SHARDMESSAGE_MIGRATION_SET:
            //Log_Debug("shardMigration SET shardID = %U", message.shardID);
            environment.Set(contextID, message.shardID, message.key, message.value);
            break;
         case SHARDMESSAGE_MIGRATION_DELETE:
            environment.Delete(contextID, message.shardID, message.key);
            break;
         case SHARDMESSAGE_MIGRATION_COMPLETE:
            // TODO: handle this message type
            Log_Debug("TODO SHARDMESSAGE_MIGRATION_COMPLETE");
            break;
         default:
            ASSERT_FAIL();
            break;
    }

    return shardID;

#undef CHECK_CMD
#undef RESPONSE_FAIL
#undef CHECK_SHARDID
}

void ShardDatabaseManager::OnLeaseTimeout()
{
    sequences.DeleteTree();
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

    Log_Trace("asyncGet: %b", asyncGet.active);

    if (asyncGet.active)
        return;
    
    start = NowClock();

    FOREACH_FIRST (itRequest, readRequests)
    {
        TRY_YIELD_RETURN(executeReads, start);

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
            asyncGet.active = false;
            itRequest->changeTimeout = start;
            blockingReadRequests.Append(itRequest);
        }
    }

    Log_Trace("blocking");
        
    FOREACH_FIRST (itRequest, blockingReadRequests)
    {
        TRY_YIELD_RETURN(executeReads, start);

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
    uint64_t                    start;
    uint64_t                    shardID;
    int16_t                     contextID;
    ReadBuffer                  prefix;
    ReadBuffer                  startKey;
    ReadBuffer                  endKey;
    ClientRequest*              request;
    ConfigShard*                configShard;
    ShardDatabaseAsyncList*     asyncList;

    if (inactiveAsyncLists.GetLength() == 0)
        return;
    
    start = NowClock();

    FOREACH_FIRST (request, listRequests)
    {
        TRY_YIELD_RETURN(executeLists, start);
        listRequests.Remove(request);

        // silently drop requests from disconnected clients
        if (!request->session->IsActive())
        {
            request->response.NoResponse();
            request->OnComplete();
            continue;
        }

        // set if prefix is set it is assumed that startKey and endKey is prefixed
        prefix = request->prefix;
        if (request->key.GetLength() == 0 && request->prefix.GetLength() > 0)
        {
            request->key.Write(request->prefix);
            if (!request->forwardDirection)
            {
                if ((unsigned)request->prefix.GetCharAt(request->prefix.GetLength() - 1) < 255)
                    request->key.GetBuffer()[request->key.GetLength() - 1]++;
                else
                    request->key.Append((char)0);
            }
        }

        startKey = request->key;
        endKey = request->endKey;

        if (request->forwardDirection && ReadBuffer::Cmp(startKey, prefix) < 0)
        {
            startKey = request->prefix;
        }
        
        // when iterating backwards, empty startKey means start from the last shard
        if (!request->forwardDirection && startKey.GetLength() == 0)
            request->findByLastKey = true;

        // find the exact shard based on what startKey is given in the request
        contextID = QUORUM_DATABASE_DATA_CONTEXT;
        if (request->findByLastKey)
            shardID = environment.GetShardIDByLastKey(contextID, request->tableID, startKey);
        else
            shardID = environment.GetShardID(contextID, request->tableID, startKey);

        // check if the shard is local
        configShard = shardServer->GetConfigState()->GetShard(shardID);
        if (configShard == NULL)
        {
            Log_Debug("sending Next");
            request->response.Next(startKey, endKey, prefix, request->count);
            request->OnComplete();
            continue;
        }

        asyncList = inactiveAsyncLists.Pop();
        asyncList->Clear();

        // set type based on request type
        if (request->type == CLIENTREQUEST_LIST_KEYS)
            asyncList->type = StorageAsyncList::KEY;
        else if (request->type == CLIENTREQUEST_LIST_KEYVALUES)
            asyncList->type = StorageAsyncList::KEYVALUE;
        else if (request->type == CLIENTREQUEST_COUNT)
            asyncList->type = StorageAsyncList::COUNT;
        else
            ASSERT_FAIL();

        // set parameters
        asyncList->SetTotal(0);
        asyncList->num = 0;
        asyncList->SetRequest(request);
        asyncList->startKey = startKey;
        asyncList->endKey = endKey;
        asyncList->prefix = request->prefix;
        asyncList->count = request->count;
        asyncList->forwardDirection = request->forwardDirection;
        asyncList->startWithLastKey = request->findByLastKey;
        asyncList->shardFirstKey.Write(configShard->firstKey);
        asyncList->shardLastKey.Write(configShard->lastKey);
        asyncList->onComplete = MFunc<ShardDatabaseAsyncList, &ShardDatabaseAsyncList::OnShardComplete>(asyncList);
        
        // try to execute the list command asynchronously
        Log_Debug("Listing shard: shardFirstKey: %B, shardLastKey: %B", &asyncList->shardFirstKey, &asyncList->shardLastKey);
        environment.AsyncList(contextID, shardID, asyncList);
 
        if (!asyncList->IsActive())
            inactiveAsyncLists.Append(asyncList);

        if (inactiveAsyncLists.GetLength() == 0)
            return;
    }
}
