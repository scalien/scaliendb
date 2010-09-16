#ifndef SDBPCLIENT_H
#define SDBPCLIENT_H

#include "System/Platform.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Events/Countdown.h"

class SDBPClientResult;
class ClientRequest;
class ClientResponse;

/*
===============================================================================

 SDBPClient

===============================================================================
*/

class SDBPClient
{
public:
	SDBPClient();
	~SDBPClient();

	int					Init(int nodec, const char* nodev[]);
	void				Shutdown();

	void				SetGlobalTimeout(uint64_t timeout);
	void				SetMasterTimeout(uint64_t timeout);
	uint64_t			GetGlobalTimeout();
	uint64_t			GetMasterTimeout();

	// connection state related commands
	int					GetMaster();
	void				DistributeDirty(bool dd);
	
	SDBPClientResult*	GetResult();

	int					TransportStatus();
	int					ConnectivityStatus();
	int					TimeoutStatus();
	int					CommandStatus();
	
	bool				UseDatabase(ReadBuffer& database);
	bool				UseTable(ReadBuffer& table);
	
	// commands that return a Result
	int					Get(const ReadBuffer& key, bool dirty = false);
	int					DirtyGet(const ReadBuffer& key);

	// write commands
	int					Set(const ReadBuffer& key,
							const ReadBuffer& value);

	int					Delete(const ReadBuffer& key, bool remove = false);

private:
	// Controller connections
	// ShardServer connections
	// ClusterState
	//	- DatabaseMap:	database
	//	- ShardMap:	shard => nodeID
	//	- NodeMap:	nodeID => ClientConnection
	
	uint64_t			NextCommandID();
	uint64_t			NextMasterCommandID();
	ClientRequest*		CreateGetMasterRequest();
	void				ResendRequests(uint64_t nodeID);
	void				SetMaster(int64_t master, uint64_t nodeID);
	void				UpdateConnectivityStatus();
	void				OnGlobalTimeout();
	void				OnMasterTimeout();
	bool				IsSafe();
	
	int64_t				master;
	uint64_t			commandID;
	uint64_t			masterCommandID;
	int					connectivityStatus;
	int					timeoutStatus;
	Countdown			globalTimeout;
	Countdown			masterTimeout;
	
	friend class SDBPClientConnection;
};

#endif