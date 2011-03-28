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
    
    void        SetBuffer(char* data_, int len_);
    
    void*       data;
    int         len;
};

/*
===============================================================================================

 SDBP Result functions

===============================================================================================
*/

void            SDBP_ResultClose(ResultObj result);
std::string     SDBP_ResultKey(ResultObj result);
std::string     SDBP_ResultValue(ResultObj result);
SDBP_Buffer     SDBP_ResultKeyBuffer(ResultObj result);
SDBP_Buffer     SDBP_ResultValueBuffer(ResultObj result);
uint64_t        SDBP_ResultNumber(ResultObj result);
uint64_t        SDBP_ResultDatabaseID(ResultObj result);
uint64_t        SDBP_ResultTableID(ResultObj result);
void            SDBP_ResultBegin(ResultObj result);
void            SDBP_ResultNext(ResultObj result);
bool            SDBP_ResultIsEnd(ResultObj result);
int             SDBP_ResultTransportStatus(ResultObj result);
int             SDBP_ResultCommandStatus(ResultObj result);

/*
===============================================================================================

 SDBP Client functions

===============================================================================================
*/

ClientObj       SDBP_Create();
int             SDBP_Init(ClientObj client, const SDBP_NodeParams& params);
void            SDBP_Destroy(ClientObj client);
ResultObj       SDBP_GetResult(ClientObj client);

void            SDBP_SetGlobalTimeout(ClientObj client, uint64_t timeout);
void            SDBP_SetMasterTimeout(ClientObj client, uint64_t timeout);
uint64_t        SDBP_GetGlobalTimeout(ClientObj client);
uint64_t        SDBP_GetMasterTimeout(ClientObj client);

uint64_t        SDBP_GetCurrentDatabaseID(ClientObj client);
uint64_t        SDBP_GetCurrentTableID(ClientObj client);

void            SDBP_SetBatchLimit(ClientObj client, uint64_t limit);
void            SDBP_SetBulkLoading(ClientObj client, bool bulk);

int             SDBP_CreateQuorum(ClientObj client, const SDBP_NodeParams& params);
int             SDBP_DeleteQuorum(ClientObj client, uint64_t quorumID);
int             SDBP_AddNode(ClientObj client, uint64_t quorumID, uint64_t nodeID);
int             SDBP_RemoveNode(ClientObj client, uint64_t quorumID, uint64_t nodeID);
int             SDBP_ActivateNode(ClientObj client, uint64_t nodeID);

int             SDBP_CreateDatabase(ClientObj client, const std::string& name);
int             SDBP_RenameDatabase(ClientObj client, uint64_t databaseID, const std::string& name);
int             SDBP_DeleteDatabase(ClientObj client, uint64_t databaseID);

int             SDBP_CreateTable(ClientObj client, uint64_t databaseID, uint64_t quorumID, const std::string& name);
int             SDBP_RenameTable(ClientObj client, uint64_t tableID, const std::string& name);
int             SDBP_DeleteTable(ClientObj client, uint64_t tableID);
int             SDBP_TruncateTable(ClientObj client, uint64_t tableID);

uint64_t        SDBP_GetDatabaseID(ClientObj client, const std::string& name);
uint64_t        SDBP_GetTableID(ClientObj client, uint64_t databaseID, const std::string& name);

int             SDBP_UseDatabase(ClientObj client, const std::string& name);
int             SDBP_UseTable(ClientObj client, const std::string& name);

int             SDBP_Get(ClientObj client, const std::string& key);
int             SDBP_GetCStr(ClientObj client, char* key, int len);
int             SDBP_Set(ClientObj client, const std::string& key, const std::string& value);
int             SDBP_SetCStr(ClientObj client_, char* key, int lenKey, char* value, int lenValue);
int             SDBP_SetIfNotExists(ClientObj client, const std::string& key, const std::string& value);
int             SDBP_SetIfNotExistsCStr(ClientObj client, char* key, int lenKey, char* value, int lenValue);
int             SDBP_TestAndSet(ClientObj client, const std::string& key, const std::string& test, const std::string& value);
int             SDBP_TestAndSetCStr(ClientObj client, char* key, int lenKey, char* test, int lenTest, char* value, int lenValue);
int             SDBP_GetAndSet(ClientObj client, const std::string& key, const std::string& value);
int             SDBP_GetAndSetCStr(ClientObj client, char* key, int lenKey, char* value, int lenValue);
int             SDBP_Add(ClientObj client, const std::string& key, int64_t number);
int             SDBP_AddCStr(ClientObj client_, char* key, int len, int64_t number);
int             SDBP_Append(ClientObj client, const std::string& key, const std::string& value);
int             SDBP_AppendCStr(ClientObj client_, char* key, int lenKey, char* value, int lenValue);
int             SDBP_Delete(ClientObj client, const std::string& key);
int             SDBP_DeleteCStr(ClientObj client_, char* key, int len);
int             SDBP_Remove(ClientObj client, const std::string& key);
int             SDBP_RemoveCStr(ClientObj client_, char* key, int len);
int             SDBP_ListKeys(ClientObj client, const std::string& key, unsigned count, unsigned offset);
int             SDBP_ListKeysCStr(ClientObj client, char* key, int len, unsigned count, unsigned offset);
int             SDBP_ListKeyValues(ClientObj client, const std::string& key, unsigned count, unsigned offset);
int             SDBP_ListKeyValuesCStr(ClientObj client, char* key, int len, unsigned count, unsigned offset);
int             SDBP_Count(ClientObj client, const std::string& key, unsigned count, unsigned offset);
int             SDBP_CountCStr(ClientObj client, char* key, int len, unsigned count, unsigned offset);

/*
===============================================================================================

 Grouping commands

===============================================================================================
*/

int             SDBP_Begin(ClientObj client);
int             SDBP_Submit(ClientObj client);
int             SDBP_Cancel(ClientObj client);
bool            SDBP_IsBatched(ClientObj client);

/*
===============================================================================================

 Debugging

===============================================================================================
*/

void            SDBP_SetTrace(bool trace);

#endif
