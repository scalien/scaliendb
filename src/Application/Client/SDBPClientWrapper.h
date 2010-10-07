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

	void Close();
	void AddNode(const std::string& node);

	int				nodec;
	char**			nodes;
	int				num;
};


/*
===============================================================================================

 SDBPClientWrapper functions

===============================================================================================
*/

ClientObj       SDBP_Create();
int             SDBP_Init(ClientObj client, const SDBP_NodeParams& params);
void            SDBP_Destroy(ClientObj client);
ResultObj       SDBP_GetResult(ClientObj client);

void			SDBP_SetGlobalTimeout(ClientObj client, uint64_t timeout);
void			SDBP_SetMasterTimeout(ClientObj client, uint64_t timeout);
uint64_t		SDBP_GetGlobalTimeout(ClientObj client);
uint64_t		SDBP_GetMasterTimeout(ClientObj client);

uint64_t        SDBP_GetDatabaseID(ClientObj client, const std::string& name);
uint64_t        SDBP_GetTableID(ClientObj client, uint64_t databaseID, const std::string& name);

int				SDBP_Get(ClientObj client,
                 uint64_t databaseID, uint64_t tableID,
                 const std::string& key);
int				SDBP_Set(ClientObj client,
                 uint64_t databaseID, uint64_t tableID,
                 const std::string& key, const std::string& value);
int				SDBP_Delete(ClientObj client,
                 uint64_t databaseID, uint64_t tableID,
                 const std::string& key);


#endif
