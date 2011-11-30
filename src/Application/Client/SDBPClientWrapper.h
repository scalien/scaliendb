#ifndef SDBPCLIENTWRAPPER_H
#define SDBPCLIENTWRAPPER_H

/*
===============================================================================================

 SWIG compatible wrapper interface for generating client wrappers

===============================================================================================
*/

#include <string>
#include "System/Platform.h"

typedef void * ClientObj;
typedef void * ResultObj;

/*
===============================================================================================

 SDBP_NodeParams: helper class for converting node array to Init argument

===============================================================================================
*/

struct SDBP_NodeParams
{
    SDBP_NodeParams(int nodec_);
    ~SDBP_NodeParams();

    void        Close();
    void        AddNode(const std::string& node);

    int         nodec;
    char**      nodes;
    int         num;
};

struct SDBP_Buffer
{
    SDBP_Buffer();
    
    void      SetBuffer(char* data_, int len_);
    
    intptr_t  data;
    int       len;
};

/*
===============================================================================================

 Result functions

===============================================================================================
*/

void            SDBP_ResultClose(ResultObj result);
std::string     SDBP_ResultKey(ResultObj result);
std::string     SDBP_ResultValue(ResultObj result);
SDBP_Buffer     SDBP_ResultKeyBuffer(ResultObj result);
SDBP_Buffer     SDBP_ResultValueBuffer(ResultObj result);
int64_t         SDBP_ResultSignedNumber(ResultObj result);
uint64_t        SDBP_ResultNumber(ResultObj result);
bool            SDBP_ResultIsConditionalSuccess(ResultObj result);
uint64_t        SDBP_ResultDatabaseID(ResultObj result);
uint64_t        SDBP_ResultTableID(ResultObj result);
void            SDBP_ResultBegin(ResultObj result);
void            SDBP_ResultNext(ResultObj result);
bool            SDBP_ResultIsEnd(ResultObj result);
bool            SDBP_ResultIsFinished(ResultObj result);
int             SDBP_ResultTransportStatus(ResultObj result);
int             SDBP_ResultCommandStatus(ResultObj result);
unsigned        SDBP_ResultNumNodes(ResultObj result);
uint64_t        SDBP_ResultNodeID(ResultObj result, unsigned n);
unsigned        SDBP_ResultElapsedTime(ResultObj result);

/*
===============================================================================================

 Client functions

===============================================================================================
*/

ClientObj       SDBP_Create();
int             SDBP_Init(ClientObj client, const SDBP_NodeParams& params);
void            SDBP_Destroy(ClientObj client);
ResultObj       SDBP_GetResult(ClientObj client);

/*
===============================================================================================

 Client settings

===============================================================================================
*/

void            SDBP_SetGlobalTimeout(ClientObj client, uint64_t timeout);
void            SDBP_SetMasterTimeout(ClientObj client, uint64_t timeout);
uint64_t        SDBP_GetGlobalTimeout(ClientObj client);
uint64_t        SDBP_GetMasterTimeout(ClientObj client);

std::string     SDBP_GetJSONConfigState(ClientObj client);
void            SDBP_WaitConfigState(ClientObj client);

void            SDBP_SetConsistencyMode(ClientObj client, int consistencyMode);
void            SDBP_SetBatchMode(ClientObj client, int batchMode);
void            SDBP_SetBatchLimit(ClientObj client, unsigned batchLimit);

/*
===============================================================================================

 Schema commands

===============================================================================================
*/

int             SDBP_CreateDatabase(ClientObj client, const std::string& name);
int             SDBP_RenameDatabase(ClientObj client, uint64_t databaseID, const std::string& name);
int             SDBP_DeleteDatabase(ClientObj client, uint64_t databaseID);

int             SDBP_CreateTable(ClientObj client, uint64_t databaseID, uint64_t quorumID, const std::string& name);
int             SDBP_RenameTable(ClientObj client, uint64_t tableID, const std::string& name);
int             SDBP_DeleteTable(ClientObj client, uint64_t tableID);
int             SDBP_TruncateTable(ClientObj client, uint64_t tableID);

unsigned        SDBP_GetNumQuorums(ClientObj client);
uint64_t        SDBP_GetQuorumIDAt(ClientObj client, unsigned n);
std::string     SDBP_GetQuorumNameAt(ClientObj client, unsigned n);
uint64_t        SDBP_GetQuorumIDByName(ClientObj client, const std::string& name);

unsigned        SDBP_GetNumDatabases(ClientObj client);
uint64_t        SDBP_GetDatabaseIDAt(ClientObj client, unsigned n);
std::string     SDBP_GetDatabaseNameAt(ClientObj client, unsigned n);
uint64_t        SDBP_GetDatabaseIDByName(ClientObj client, const std::string& name);

unsigned        SDBP_GetNumTables(ClientObj client, uint64_t databaseID);
uint64_t        SDBP_GetTableIDAt(ClientObj client, uint64_t databaseID, unsigned n);
std::string     SDBP_GetTableNameAt(ClientObj client, uint64_t databaseID, unsigned n);
uint64_t        SDBP_GetTableIDByName(ClientObj client, uint64_t databaseID, const std::string& name);

/*
===============================================================================================

 Data commands

===============================================================================================
*/

int             SDBP_Get(ClientObj client, uint64_t tableID, const std::string& key);
int             SDBP_GetCStr(ClientObj client, uint64_t tableID, char* key, int len);
int             SDBP_Set(ClientObj client, uint64_t tableID, const std::string& key, const std::string& value);
int             SDBP_SetCStr(ClientObj client_, uint64_t tableID, char* key, int lenKey, char* value, int lenValue);
int             SDBP_Add(ClientObj client, uint64_t tableID, const std::string& key, int64_t number);
int             SDBP_AddCStr(ClientObj client_, uint64_t tableID, char* key, int len, int64_t number);
int             SDBP_Delete(ClientObj client, uint64_t tableID, const std::string& key);
int             SDBP_DeleteCStr(ClientObj client_, uint64_t tableID, char* key, int len);
int             SDBP_SequenceSet(ClientObj client, uint64_t tableID, const std::string& key, uint64_t number);
int             SDBP_SequenceSetCStr(ClientObj client_, uint64_t tableID, char* key, int len, uint64_t number);
int             SDBP_SequenceNext(ClientObj client, uint64_t tableID, const std::string& key);
int             SDBP_SequenceNextCStr(ClientObj client, uint64_t tableID, char* key, int len);
int             SDBP_ListKeys(
                 ClientObj client, uint64_t tableID,
                 const std::string& startKey, const std::string& endKey, const std::string& prefix,
                 unsigned count, bool forwardDirection, bool skip);
int             SDBP_ListKeysCStr(
                 ClientObj client, uint64_t tableID,
                 char* startKey, int startKeyLen, char* endKey, int endKeyLen, char* prefix, int prefixLen,
                 unsigned count, bool forwardDirection, bool skip);
int             SDBP_ListKeyValues(
                 ClientObj client, uint64_t tableID,
                 const std::string& startKey, const std::string& endKey, const std::string& prefix,
                 unsigned count, bool forwardDirection, bool skip);
int             SDBP_ListKeyValuesCStr(
                 ClientObj client, uint64_t tableID,
                 char* startKey, int startKeyLen, char* endKey, int endKeyLen, char* prefix, int prefixLen,
                 unsigned count, bool forwardDirection, bool skip);
int             SDBP_Count(
                 ClientObj client, uint64_t tableID,
                 const std::string& startKey, const std::string& endKey, const std::string& prefix,
                 bool forwardDirection);
int             SDBP_CountCStr(
                 ClientObj client, uint64_t tableID,
                 char* startKey, int startKeyLen, char* endKey, int endKeyLen, char* prefix, int prefixLen,
                 bool forwardDirection);

/*
===============================================================================================

 Grouping commands

===============================================================================================
*/

int             SDBP_Begin(ClientObj client);
int             SDBP_Submit(ClientObj client);
int             SDBP_Cancel(ClientObj client);

/*
===============================================================================================

 Debugging

===============================================================================================
*/

void            SDBP_SetTrace(bool trace);
void            SDBP_SetLogFile(const std::string& filename);
void            SDBP_SetTraceBufferSize(unsigned traceBufferSize);
void            SDBP_LogTrace(const std::string& msg);
void            SDBP_SetShardPoolSize(unsigned shardPoolSize);
std::string     SDBP_GetVersion();
std::string     SDBP_GetDebugString();

#endif
