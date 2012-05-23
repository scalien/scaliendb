#include "ShardHTTPClientSession.h"
#include "ShardServer.h"
#include "System/Config.h"
#include "System/Common.h"
#include "System/FileSystem.h"
#include "System/CrashReporter.h"
#include "System/IO/IOProcessor.h"
#include "Application/HTTP/HTTPConnection.h"
#include "Application/Common/ClientRequestCache.h"
#include "Application/ShardServer/ShardServerApp.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Storage/StoragePageCache.h"
#include "Framework/Storage/StorageFileDeleter.h"
#include "Version.h"

#define PARAM_BOOL_VALUE(param)                         \
    ((ReadBuffer::Cmp((param), "yes") == 0 ||           \
    ReadBuffer::Cmp((param), "true") == 0 ||            \
    ReadBuffer::Cmp((param), "on") == 0 ||            \
    ReadBuffer::Cmp((param), "1") == 0) ? true : false)

#define SHARD_MIGRATION_WRITER (shardServer->GetShardMigrationWriter())

void ShardHTTPClientSession::SetShardServer(ShardServer* shardServer_)
{
    shardServer = shardServer_;
}

void ShardHTTPClientSession::SetConnection(HTTPConnection* conn_)
{
    session.SetConnection(conn_);
    conn_->SetOnClose(MFUNC(ShardHTTPClientSession, OnConnectionClose));
}

bool ShardHTTPClientSession::HandleRequest(HTTPRequest& request)
{
    ReadBuffer  cmd;
    ReadBuffer  param;
    
    session.ParseRequest(request, cmd, params);

    // assume printable output by default
    binaryData = false;
    if (HTTP_GET_OPT_PARAM(params, "binary", param))
    {
        binaryData = PARAM_BOOL_VALUE(param);
    }

    return ProcessCommand(cmd);
}

void ShardHTTPClientSession::OnComplete(ClientRequest* request, bool last)
{
    Buffer          tmp;
    ReadBuffer      rb;
    Buffer          key, value;
    ClientResponse* response;
    Buffer          location;
    ReadBuffer      param;

    response = &request->response;
    switch (response->type)
    {
    case CLIENTRESPONSE_OK:
        if (request->type == CLIENTREQUEST_COUNT)
        {
            tmp.Writef("%u", response->numKeys);
            rb.Wrap(tmp);
            session.Print(rb);
        }
        else
        {
            if (HTTP_GET_OPT_PARAM(params, "timing", param) && PARAM_BOOL_VALUE(param) == true)
            {
                // this is not the default case because of compatibility reasons
                // some test programs assume that the result of list commands returns with "OK"
                uint64_t elapsed = EventLoop::Now() - request->lastChangeTime;
                if (elapsed < 1000)
                    tmp.Writef("OK, %U msec", elapsed);
                else if (elapsed < 10*1000)
                    tmp.Writef("OK, %U.%U sec", elapsed / 1000, (elapsed % 1000) / 100);
                else
                    tmp.Writef("OK, %U sec", elapsed / 1000);
            }
            else
                tmp.Write("OK");

            session.Print(tmp);
        }
        break;
    case CLIENTRESPONSE_NUMBER:
        if (request->type == CLIENTREQUEST_COUNT)
        {
            response->numKeys += response->number;
        }
        else
        {
            tmp.Writef("%U", response->number);
            rb.Wrap(tmp);
            session.Print(rb);
        }
        break;
    case CLIENTRESPONSE_SNUMBER:
        tmp.Writef("%I", response->snumber);
        rb.Wrap(tmp);
        session.Print(rb);
        break;
    case CLIENTRESPONSE_VALUE:
        session.Print(response->value);
        break;
    case CLIENTRESPONSE_LIST_KEYS:
        for (unsigned i = 0; i < response->numKeys; i++)
        {
            if (binaryData)
            {
                key.Writef("%#R", &response->keys[i]);
                session.Print(key);
            }
            else
                session.Print(response->keys[i]);
        }
        break;
    case CLIENTRESPONSE_LIST_KEYVALUES:
        for (unsigned i = 0; i < response->numKeys; i++)
        {
            if (binaryData)
            {
                key.Writef("%#R", &response->keys[i]);
                value.Writef("%#R", &response->values[i]);
                session.PrintPair(key, value);
            }
            else
                session.PrintPair(response->keys[i], response->values[i]);
        }
        break;
    case CLIENTRESPONSE_NEXT:
        tmp.Writef("NEXT \"%R\" \"%R\" %U",
         &response->value, &response->endKey, response->number);
        rb.Wrap(tmp);
        session.Print(rb);
        break;
    case CLIENTRESPONSE_NOSERVICE:
        if (GetRedirectedShardServer(request->tableID, request->key, location))
            session.Redirect(location);
        else
            session.Print("NOSERVICE");
        break;
    case CLIENTRESPONSE_BADSCHEMA:
        if (GetRedirectedShardServer(request->tableID, request->key, location))
            session.Redirect(location);
        else
            session.Print("BADSCHEMA");
        break;
    case CLIENTRESPONSE_FAILED:
        if (request->paxosID > 0 && GetRedirectedShardServer(request->tableID, request->key, location))
            session.Redirect(location);
        else
            session.Print("FAILED");
        break;
    }
    
    if (last)
    {
        session.Flush();        
        delete request;
    }
}

bool ShardHTTPClientSession::IsActive()
{
    return true;
}

void ShardHTTPClientSession::PrintStatus()
{
    Buffer                      keybuf;
    Buffer                      valbuf;
    uint64_t                    primaryID;
    uint64_t                    totalSpace, freeSpace;
    ShardQuorumProcessor*       it;
    char                        hexbuf[64 + 1];
    int                         i;
    uint64_t                    elapsed;

    session.PrintPair("ScalienDB", "ShardServer");
    session.PrintPair("Version", VERSION_STRING);

    valbuf.Writef("%U", GetProcessID());
    valbuf.NullTerminate();
    session.PrintPair("ProcessID", valbuf.GetBuffer());

    UInt64ToBufferWithBase(hexbuf, sizeof(hexbuf), REPLICATION_CONFIG->GetClusterID(), 64);
    session.PrintPair("ClusterID", hexbuf);

    valbuf.Writef("%U", MY_NODEID);
    valbuf.NullTerminate();
    session.PrintPair("NodeID", valbuf.GetBuffer());   

    valbuf.Clear();
    for (i = 0; i < configFile.GetListNum("controllers"); i++)
    {
        if (i != 0)
            valbuf.Append(",");
        valbuf.Append(configFile.GetListValue("controllers", i, ""));
    }
    valbuf.NullTerminate();
    session.PrintPair("Controllers", valbuf.GetBuffer());

    totalSpace = FS_DiskSpace(configFile.GetValue("database.dir", "db"));
    freeSpace = FS_FreeDiskSpace(configFile.GetValue("database.dir", "db"));

    // write quorums, shards, and leases
    ShardServer::QuorumProcessorList* quorumProcessors = shardServer->GetQuorumProcessors();
    
    valbuf.Writef("%u", quorumProcessors->GetLength());
    valbuf.NullTerminate();
    session.PrintPair("Number of quorums", valbuf.GetBuffer());
    
    FOREACH (it, *quorumProcessors)
    {
        primaryID = shardServer->GetLeaseOwner(it->GetQuorumID());
        
        keybuf.Writef("Quorum %U", it->GetQuorumID());
        keybuf.NullTerminate();

        uint64_t paxosID = shardServer->GetQuorumProcessor(it->GetQuorumID())->GetPaxosID();
        uint64_t highestPaxosID = shardServer->GetQuorumProcessor(it->GetQuorumID())->GetConfigQuorum()->paxosID;
        if (paxosID > highestPaxosID)
            highestPaxosID = paxosID;
        elapsed = (EventLoop::Now() - it->GetLastLearnChosenTime()) / 1000.0;
        if (it->GetLastLearnChosenTime() > 0)
        {
            valbuf.Writef("primary: %U, paxosID: %U/%U, Seconds since last replication: %U", primaryID, paxosID, highestPaxosID, elapsed);
        }
        else
        {
            valbuf.Writef("primary: %U, paxosID: %U/%U, No replication round seen since start...", primaryID, paxosID, highestPaxosID);
        }

        if (it->IsCatchupActive())
        {
            valbuf.Appendf(", catchup active (sent: %s/%s, aggregate throughput: %s/s)",
             HUMAN_BYTES(it->GetCatchupBytesSent()),
             HUMAN_BYTES(it->GetCatchupBytesTotal()),
             HUMAN_BYTES(it->GetCatchupThroughput())
             );
        }
        valbuf.NullTerminate();
        
        session.PrintPair(keybuf.GetBuffer(), valbuf.GetBuffer());
    }    
    
    keybuf.Writef("Migrating shard (sending)");
    keybuf.NullTerminate();
    if (SHARD_MIGRATION_WRITER->IsActive())
        valbuf.Writef("yes (sent: %s/%s, aggregate throughput: %s/s)",
         HUMAN_BYTES(SHARD_MIGRATION_WRITER->GetBytesSent()),
         HUMAN_BYTES(SHARD_MIGRATION_WRITER->GetBytesTotal()),
         HUMAN_BYTES(SHARD_MIGRATION_WRITER->GetThroughput()));
    else
        valbuf.Writef("no");
    valbuf.NullTerminate();
    session.PrintPair(keybuf.GetBuffer(), valbuf.GetBuffer());
    
    valbuf.Writef("%u", shardServer->GetNumSDBPClients());
    valbuf.NullTerminate();
    session.PrintPair("Number of clients", valbuf.GetBuffer());
    
    valbuf.Writef("%u", shardServer->GetLockManager()->GetNumLocks());
    valbuf.NullTerminate();
    session.PrintPair("Number of locks", valbuf.GetBuffer());

    session.Flush();
}

void ShardHTTPClientSession::PrintStorage()
{
    Buffer  buffer;
    
    shardServer->GetDatabaseManager()->GetEnvironment()->PrintState(
     QUORUM_DATABASE_DATA_CONTEXT, buffer);

    session.Print(buffer);

    session.Flush();
}

void ShardHTTPClientSession::PrintStatistics()
{
    Buffer                  buffer;
    IOProcessorStat         iostat;
    FS_Stat                 fsStat;
    ShardDatabaseManager*   databaseManager;
    ShardQuorumProcessor*   quorumProcessor;
    ReadBuffer              param;
    
    IOProcessor::GetStats(&iostat);
    
    buffer.Append("IOProcessor stats\n");
    buffer.Appendf("numPolls: %U\n", iostat.numPolls);
    buffer.Appendf("numTCPReads: %U\n", iostat.numTCPReads);
    buffer.Appendf("numTCPWrites: %U\n", iostat.numTCPWrites);
    buffer.Appendf("numTCPBytesSent: %s\n", HUMAN_BYTES(iostat.numTCPBytesSent));
    buffer.Appendf("numTCPBytesReceived: %s\n", HUMAN_BYTES(iostat.numTCPBytesReceived));
    buffer.Appendf("numCompletions: %U\n", iostat.numCompletions);
    buffer.Appendf("totalPollTime: %U\n", iostat.totalPollTime);
    buffer.Appendf("totalNumEvents: %U\n", iostat.totalNumEvents);

    FS_GetStats(&fsStat);
    buffer.Append("  Category: FileSystem\n");
    buffer.Appendf("numReads: %U\n", fsStat.numReads);
    buffer.Appendf("numWrites: %U\n", fsStat.numWrites);
    buffer.Appendf("numBytesRead: %s\n", HUMAN_BYTES(fsStat.numBytesRead));
    buffer.Appendf("numBytesWritten: %s\n", HUMAN_BYTES(fsStat.numBytesWritten));
    buffer.Appendf("numFileOpens: %U\n", fsStat.numFileOpens);
    buffer.Appendf("numFileCloses: %U\n", fsStat.numFileCloses);
    buffer.Appendf("numFileDeletes: %U\n", fsStat.numFileDeletes);

    databaseManager = shardServer->GetDatabaseManager();
    buffer.Append("  Category: ShardServer\n");
    buffer.Appendf("uptime: %U sec\n", (Now() - shardServer->GetStartTimestamp()) / 1000);
    buffer.Appendf("pendingReadRequests: %u\n", databaseManager->GetNumReadRequests());
    buffer.Appendf("pendingBlockingReadRequests: %u\n", databaseManager->GetNumBlockingReadRequests());
    buffer.Appendf("pendingListRequests: %u\n", databaseManager->GetNumListRequests());
    buffer.Appendf("inactiveListThreads: %u\n", databaseManager->GetNumInactiveListThreads());
    buffer.Appendf("numAbortedListRequests: %U\n", databaseManager->GetNumAbortedListRequests());
    buffer.Appendf("nextRequestID: %U\n", databaseManager->GetNextRequestID());
    buffer.Appendf("listPageCacheSize: %U\n", StorageDataPageCache::GetCacheSize());
    buffer.Appendf("maxListPageCacheSize: %U\n", StorageDataPageCache::GetMaxCacheSize());
    buffer.Appendf("maxUsedListPageCacheSize: %U\n", StorageDataPageCache::GetMaxUsedSize());
    buffer.Appendf("largestDataPageSeen: %u\n", StorageDataPageCache::GetLargestSeen());
    buffer.Appendf("pageCacheFreeListLength: %u\n", StorageDataPageCache::GetFreeListLength());
    buffer.Appendf("numCacheHit: %U\n", StorageDataPageCache::GetNumCacheHit());
    buffer.Appendf("numCacheMissPoolHit: %U\n", StorageDataPageCache::GetNumCacheMissPoolHit());
    buffer.Appendf("numCacheMissPoolMiss: %U\n", StorageDataPageCache::GetNumCacheMissPoolMiss());
    buffer.Append("  Category: Mutexes\n");
    buffer.Appendf("StorageFileDeleter mutexLockCounter: %U\n", StorageFileDeleter::GetMutex().lockCounter);
    buffer.Appendf("StorageFileDeleter mutexLastLockDate: %U\n", StorageFileDeleter::GetMutex().lastLockTime);
    buffer.Appendf("Endpoint mutexLockCounter: %U\n", Endpoint::GetMutex().lockCounter);
    buffer.Appendf("Endpoint mutexLastLockDate: %U\n", Endpoint::GetMutex().lastLockTime);
    buffer.Appendf("Log mutexLockCounter: %U\n", Log_GetMutex().lockCounter);
    buffer.Appendf("Log mutexLastLockDate: %U\n", Log_GetMutex().lastLockTime);


    buffer.Append("  Category: Replication\n");
    FOREACH (quorumProcessor, *shardServer->GetQuorumProcessors())
    {
        buffer.Appendf("quorum[%U].messageListSize: %u\n", quorumProcessor->GetQuorumID(), 
            quorumProcessor->GetMessageListSize());
        buffer.Appendf("quorum[%U].shardAppendStateSize: %u\n", quorumProcessor->GetQuorumID(), 
            quorumProcessor->GetShardAppendStateSize());
        buffer.Appendf("quorum[%U].contextSize: %u\n", quorumProcessor->GetQuorumID(), 
            quorumProcessor->GetQuorumContextSize());
    }

    session.Print(buffer);
    session.Flush();
}

void ShardHTTPClientSession::PrintMemoryState()
{
    Buffer                  buffer;

    shardServer->GetMemoryUsageBuffer(buffer);

    session.Print(buffer);
    session.Flush();
}

void ShardHTTPClientSession::PrintConfigFile()
{
    ConfigVar*  configVar;
    Buffer      value;

    FOREACH (configVar, configFile)
    {
        // fix terminating zeroes
        value.Write(configVar->value);
        value.Shorten(1);
        // replace zeroes back to commas
        for (unsigned i = 0; i < value.GetLength(); i++)
        {
            if (value.GetCharAt(i) == '\0')
                value.SetCharAt(i, ',');
        }
        session.PrintPair(configVar->name, value);
    }

    session.Flush();
}

bool ShardHTTPClientSession::ProcessCommand(ReadBuffer& cmd)
{
    ClientRequest*  request;
    
    if (HTTP_MATCH_COMMAND(cmd, ""))
    {
        PrintStatus();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "storage"))
    {
        PrintStorage();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "settings"))
    {
        ProcessSettings();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "clearcache"))
    {
        StoragePageCache::Clear();
        session.Print("Cache cleared");
        session.Flush();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "stats"))
    {
        PrintStatistics();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "memory"))
    {
        PrintMemoryState();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "config"))
    {
        PrintConfigFile();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "debug"))
    {
        ProcessDebugCommand();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "startbackup"))
    {
        ProcessStartBackup();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "endbackup"))
    {
        ProcessEndBackup();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "dumpmemochunks"))
    {
        ProcessDumpMemoChunks();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "rotatelog"))
    {
        Log_Rotate();
        session.Print("Log rotated");
        session.Flush();
        return true;
    }

    request = ProcessShardServerCommand(cmd);
    if (!request)
        return false;

    request->session = this;
    request->lastChangeTime = EventLoop::Now();
    shardServer->OnClientRequest(request);
    
    return true;
}

void ShardHTTPClientSession::ProcessDebugCommand()
{
#ifdef DEBUG
    ReadBuffer  param;
    bool        boolValue;

    if (HTTP_GET_OPT_PARAM(params, "crash", param))
    {
        Log_Debug("Crashing server from web console...");
        char* null = NULL;
        *null = 0;
        // Access violation
    }

    if (HTTP_GET_OPT_PARAM(params, "timedcrash", param))
    {
        uint64_t crashInterval = 0;
        HTTP_GET_OPT_U64_PARAM(params, "interval", crashInterval);
        CrashReporter::TimedCrash((unsigned int) crashInterval);
        session.Print(INLINE_PRINTF("Crash in %u msec", 100, (unsigned int) crashInterval));
    }

    if (HTTP_GET_OPT_PARAM(params, "randomcrash", param))
    {
        uint64_t crashInterval = 0;
        HTTP_GET_OPT_U64_PARAM(params, "interval", crashInterval);
        CrashReporter::RandomCrash((unsigned int) crashInterval);
        session.Print(INLINE_PRINTF("Crash in %u msec", 100, (unsigned int) crashInterval));
    }

    if (HTTP_GET_OPT_PARAM(params, "sleep", param))
    {
        unsigned sleep = 0;
        param.Readf("%u", &sleep);
        Log_Debug("Sleeping for %u seconds", sleep);
        session.Print("Start sleeping");
        MSleep(sleep * 1000);
        session.Print("Sleep finished");
    }

    if (HTTP_GET_OPT_PARAM(params, "log", param))
    {
        boolValue = PARAM_BOOL_VALUE(param);
        Log_SetDebug(boolValue);
        session.PrintPair("Debug", boolValue ? "on" : "off");
    }

    if (HTTP_GET_OPT_PARAM(params, "assert", param))
    {
        ASSERT_FAIL();
        // program terminates here
    }

    session.Flush();
#endif
}

void ShardHTTPClientSession::ProcessStartBackup()
{
    uint64_t                tocID;
    Buffer                  output;
	Buffer					configStateBuffer;
    StorageEnvironment*     env;
	ConfigState*			configState;

    env = shardServer->GetDatabaseManager()->GetEnvironment();

	configState = shardServer->GetDatabaseManager()->GetConfigState();
    if (configState)
	    configState->Write(configStateBuffer, false);

    // turn off file deletion and write a snapshot of the TOC
    env->SetDeleteEnabled(false);
    tocID = env->WriteSnapshotTOC(configStateBuffer);
    
    output.Writef("%U", tocID);
    output.NullTerminate();
    
    session.Print(output.GetBuffer());
    session.Flush();
}

void ShardHTTPClientSession::ProcessEndBackup()
{
    uint64_t                tocID;
    Buffer                  output;
    StorageEnvironment*     env;

    tocID = 0;
    HTTP_GET_OPT_U64_PARAM(params, "tocID", tocID);
    
    env = shardServer->GetDatabaseManager()->GetEnvironment();
    env->DeleteSnapshotTOC(tocID);
    env->SetDeleteEnabled(true);
    
    session.Print("Done.");
    session.Flush();
}

void ShardHTTPClientSession::ProcessDumpMemoChunks()
{
    shardServer->GetDatabaseManager()->GetEnvironment()->dumpMemoChunks = true;

    session.Print("Dumping memo chunks.");
    session.Flush();
}

bool ShardHTTPClientSession::ProcessSettings()
{
    ReadBuffer              param;
    bool                    boolValue;
    uint64_t                traceBufferSize;
    uint64_t                logFlushInterval;
    uint64_t                logTraceInterval;
    uint64_t                replicationLimit;
	uint64_t				abortWaitingListsNum;
    uint64_t                listDataPageCacheSize;
    ShardQuorumProcessor*   quorumProcessor;

    if (HTTP_GET_OPT_PARAM(params, "trace", param))
    {
        boolValue = PARAM_BOOL_VALUE(param);
        Log_SetTrace(boolValue);
        Log_Flush();
        session.PrintPair("Trace", boolValue ? "on" : "off");
        // Optional log trace interval in seconds
        logTraceInterval = 0;
        HTTP_GET_OPT_U64_PARAM(params, "interval", logTraceInterval);
        if (logTraceInterval > 0 && !onTraceOffTimeout.IsActive())
        {
            onTraceOffTimeout.SetCallable(MFUNC(ShardHTTPClientSession, OnTraceOffTimeout));
            onTraceOffTimeout.SetDelay(logTraceInterval * 1000);
            EventLoop::Add(&onTraceOffTimeout);
            session.Flush(false);
            return true;
        }
    }

    if (HTTP_GET_OPT_PARAM(params, "traceBufferSize", param))
    {
        // initialize variable, because conversion may fail
        traceBufferSize = 0;
        HTTP_GET_OPT_U64_PARAM(params, "traceBufferSize", traceBufferSize);
        // we expect traceBufferSize is in bytes
        Log_SetTraceBufferSize((unsigned) traceBufferSize);
        session.PrintPair("TraceBufferSize", INLINE_PRINTF("%u", 100, (unsigned) traceBufferSize));
    }

    if (HTTP_GET_OPT_PARAM(params, "debug", param))
    {
        boolValue = PARAM_BOOL_VALUE(param);
        Log_SetDebug(boolValue);
        Log_Flush();
        session.PrintPair("Debug", boolValue ? "on" : "off");
    }

    if (HTTP_GET_OPT_PARAM(params, "merge", param))
    {
        boolValue = PARAM_BOOL_VALUE(param);
        shardServer->GetDatabaseManager()->GetEnvironment()->SetMergeEnabled(boolValue);
        session.PrintPair("Merge", boolValue ? "on" : "off");
    }

    if (HTTP_GET_OPT_PARAM(params, "assert", param))
    {
        ASSERT(false);
    }

    if (HTTP_GET_OPT_PARAM(params, "logFlushInterval", param))
    {
        logFlushInterval = 0;
        HTTP_GET_OPT_U64_PARAM(params, "logFlushInterval", logFlushInterval);
        Log_SetFlushInterval((unsigned) logFlushInterval * 1000);
        session.PrintPair("LogFlushInterval", INLINE_PRINTF("%u", 100, (unsigned) logFlushInterval));
    }

    if (HTTP_GET_OPT_PARAM(params, "replicationLimit", param))
    {
        replicationLimit = 0;
        HTTP_GET_OPT_U64_PARAM(params, "replicationLimit", replicationLimit);
        FOREACH (quorumProcessor, *shardServer->GetQuorumProcessors())
        {
            quorumProcessor->SetReplicationLimit((unsigned) replicationLimit);
        }
        session.PrintPair("ReplicationLimit", INLINE_PRINTF("%u", 100, (unsigned) replicationLimit));
    }

	if (HTTP_GET_OPT_PARAM(params, "abortWaitingListsNum", param))
    {
        // initialize variable, because conversion may fail
        abortWaitingListsNum = 0;
        HTTP_GET_OPT_U64_PARAM(params, "abortWaitingListsNum", abortWaitingListsNum);
		shardServer->GetDatabaseManager()->GetEnvironment()->config.SetAbortWaitingListsNum(abortWaitingListsNum);
        session.PrintPair("AbortWaitingListsNum", INLINE_PRINTF("%u", 100, (unsigned) abortWaitingListsNum));
    }

	if (HTTP_GET_OPT_PARAM(params, "listDataPageCacheSize", param))
    {
        // initialize variable, because conversion may fail
        listDataPageCacheSize = 0;
        HTTP_GET_OPT_U64_PARAM(params, "listDataPageCacheSize", listDataPageCacheSize);
        if (listDataPageCacheSize > 0)
        {
            StorageDataPageCache::SetMaxCacheSize(listDataPageCacheSize);
            session.PrintPair("ListDataPageCacheSize", INLINE_PRINTF("%u", 100, (unsigned) listDataPageCacheSize));
        }
    }

    session.Flush();

    return true;
}

ClientRequest* ShardHTTPClientSession::ProcessShardServerCommand(ReadBuffer& cmd)
{
    if (HTTP_MATCH_COMMAND(cmd, "get"))
        return ProcessGet();
    if (HTTP_MATCH_COMMAND(cmd, "set"))
        return ProcessSet();
//    if (HTTP_MATCH_COMMAND(cmd, "setifnex"))
//        return ProcessSetIfNotExists();
//    if (HTTP_MATCH_COMMAND(cmd, "testandset"))
//        return ProcessTestAndSet();
//    if (HTTP_MATCH_COMMAND(cmd, "testanddelete"))
//        return ProcessTestAndDelete();
//    if (HTTP_MATCH_COMMAND(cmd, "getandset"))
//        return ProcessGetAndSet();
    if (HTTP_MATCH_COMMAND(cmd, "add"))
        return ProcessAdd();
    if (HTTP_MATCH_COMMAND(cmd, "delete"))
        return ProcessDelete();
//    if (HTTP_MATCH_COMMAND(cmd, "remove"))
//        return ProcessRemove();
    if (HTTP_MATCH_COMMAND(cmd, "listKeys"))
        return ProcessListKeys();
    if (HTTP_MATCH_COMMAND(cmd, "listKeyValues"))
        return ProcessListKeyValues();
    if (HTTP_MATCH_COMMAND(cmd, "count"))
        return ProcessCount();
    
    return NULL;
}

ClientRequest* ShardHTTPClientSession::ProcessGet()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);

    request = new ClientRequest;
    request->Get(0, 0, tableID, key);

    HTTP_GET_OPT_U64_PARAM(params, "paxosID", request->paxosID);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessSet()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->Set(0, 0, tableID, key, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessSetIfNotExists()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->SetIfNotExists(0, 0, tableID, key, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessTestAndSet()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      test;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "test", test);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->TestAndSet(0, 0, tableID, key, test, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessTestAndDelete()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      test;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "test", test);

    request = new ClientRequest;
    request->TestAndDelete(0, 0, tableID, key, test);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessGetAndSet()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->GetAndSet(0, 0, tableID, key, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessAdd()
{
    unsigned        nread;
    uint64_t        tableID;
    int64_t         number;
    ClientRequest*  request;
    ReadBuffer      key;
    ReadBuffer      numberBuffer;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "number", numberBuffer);

    number = BufferToInt64(numberBuffer.GetBuffer(), numberBuffer.GetLength(), &nread);
    if (nread != numberBuffer.GetLength())
        return NULL;

    request = new ClientRequest;
    request->Add(0, 0, tableID, key, number);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessDelete()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);

    request = new ClientRequest;
    request->Delete(0, 0, tableID, key);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessRemove()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);

    request = new ClientRequest;
    request->Remove(0, 0, tableID, key);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessListKeys()
{
    bool            forwardDirection;
    uint64_t        tableID;
    uint64_t        count;
    ReadBuffer      startKey;
    ReadBuffer      endKey;
    ReadBuffer      prefix;
    ReadBuffer      dirBuffer;
    ClientRequest*  request;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_OPT_PARAM(params, "startKey", startKey);
    HTTP_GET_OPT_PARAM(params, "endKey", endKey);
    HTTP_GET_OPT_PARAM(params, "prefix", prefix);
    HTTP_GET_OPT_PARAM(params, "direction", dirBuffer);
    count = 0;
    HTTP_GET_OPT_U64_PARAM(params, "count", count);

    if (ReadBuffer::Cmp(dirBuffer, "backward") == 0)
        forwardDirection = false;
    else
        forwardDirection = true;

    request = new ClientRequest;
    request->ListKeys(0, 0, tableID, startKey, endKey, prefix, count, forwardDirection);

    HTTP_GET_OPT_U64_PARAM(params, "paxosID", request->paxosID);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessListKeyValues()
{
    bool            forwardDirection;
    uint64_t        tableID;
    uint64_t        count;
    ReadBuffer      startKey;
    ReadBuffer      endKey;
    ReadBuffer      prefix;
    ReadBuffer      dirBuffer;
    ClientRequest*  request;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_OPT_PARAM(params, "startKey", startKey);
    HTTP_GET_OPT_PARAM(params, "endKey", endKey);
    HTTP_GET_OPT_PARAM(params, "prefix", prefix);
    HTTP_GET_OPT_PARAM(params, "direction", dirBuffer);
    count = 0;
    HTTP_GET_OPT_U64_PARAM(params, "count", count);

    if (ReadBuffer::Cmp(dirBuffer, "backward") == 0)
        forwardDirection = false;
    else
        forwardDirection = true;

    request = new ClientRequest;
    request->ListKeyValues(0, 0, tableID, startKey, endKey, prefix, count, forwardDirection);

    HTTP_GET_OPT_U64_PARAM(params, "paxosID", request->paxosID);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessCount()
{
    bool            forwardDirection;
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      startKey;
    ReadBuffer      endKey;
    ReadBuffer      prefix;
    ReadBuffer      dirBuffer;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_OPT_PARAM(params, "startKey", startKey);
    HTTP_GET_OPT_PARAM(params, "endKey", endKey);
    HTTP_GET_OPT_PARAM(params, "prefix", prefix);
    HTTP_GET_OPT_PARAM(params, "direction", dirBuffer);

    if (ReadBuffer::Cmp(dirBuffer, "backward") == 0)
        forwardDirection = false;
    else
        forwardDirection = true;

    request = new ClientRequest;
    request->Count(0, 0, tableID, startKey, endKey, prefix, forwardDirection);

    HTTP_GET_OPT_U64_PARAM(params, "paxosID", request->paxosID);

    return request;    
}

void ShardHTTPClientSession::OnConnectionClose()
{
    shardServer->OnClientClose(this);
    session.SetConnection(NULL);
    delete this;
}

bool ShardHTTPClientSession::GetRedirectedShardServer(uint64_t tableID, const ReadBuffer& key, 
 Buffer& location)
{
    ConfigState*        configState;
    ConfigShard*        shard;
    ConfigQuorum*       quorum;
    ConfigShardServer*  server;
    Endpoint            endpoint;
    
    configState = shardServer->GetConfigState();
    shard = configState->GetShard(tableID, key);
    if (!shard)
        return false;
    
    quorum = configState->GetQuorum(shard->quorumID);
    if (!quorum)
        return false;
    
    if (!quorum->hasPrimary)
        return false;
    
    server = configState->GetShardServer(quorum->primaryID);
    if (!server)
        return false;
    
    if (server->nodeID == MY_NODEID)
        return false;
    
    endpoint = server->endpoint;
    endpoint.SetPort(server->httpPort);
    // TODO: change http:// to symbolic const
    location.Writef("http://%s%R", endpoint.ToString(), &session.uri);
    return true;
}

void ShardHTTPClientSession::OnTraceOffTimeout()
{
    Log_SetTrace(false);
    session.PrintPair("Trace", "off");
    session.Flush();
}
