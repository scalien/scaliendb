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


/*
===============================================================================================

 SDBP Result functions

===============================================================================================
*/

void            SDBP_ResultClose(ResultObj result);
std::string     SDBP_ResultKey(ResultObj result);
std::string     SDBP_ResultValue(ResultObj result);
uint64_t        SDBP_ResultNumber(ResultObj result);
uint64_t        SDBP_ResultDatabaseID(ResultObj result);
uint64_t        SDBP_ResultTableID(ResultObj result);
void            SDBP_ResultBegin(ResultObj result);
void            SDBP_ResultNext(ResultObj result);
bool            SDBP_ResultIsEnd(ResultObj result);
int				SDBP_ResultTransportStatus(ResultObj result);
int				SDBP_ResultCommandStatus(ResultObj result);

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

int             SDBP_CreateQuorum(ClientObj client, const SDBP_NodeParams& params);
int             SDBP_CreateDatabase(ClientObj client, const std::string& name);
int             SDBP_CreateTable(ClientObj client, uint64_t databaseID, uint64_t quorumID, const std::string& name);

uint64_t        SDBP_GetDatabaseID(ClientObj client, const std::string& name);
uint64_t        SDBP_GetTableID(ClientObj client, uint64_t databaseID, const std::string& name);

int             SDBP_UseDatabase(ClientObj client, const std::string& name);
int             SDBP_UseTable(ClientObj client, const std::string& name);

int             SDBP_Get(ClientObj client, const std::string& key);
int             SDBP_Set(ClientObj client, const std::string& key, const std::string& value);
int             SDBP_Delete(ClientObj client, const std::string& key);

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
