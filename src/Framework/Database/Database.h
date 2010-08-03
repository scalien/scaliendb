#ifndef DATABASE_H
#define DATABASE_H

#include <db_cxx.h>
#include "System/Common.h"
#include "System/ThreadPool.h"
#include "System/Events/Callable.h"
#include "System/Events/Timer.h"
#include "System/Events/Countdown.h"
#include "System/Containers/HashMap.h"
#include "DatabaseConfig.h"

class Table;		// forward
class Transaction;	// forward

/*
===============================================================================

 Database

===============================================================================
*/

class Database
{
	friend class Table;
	friend class Transaction;

public:
	Database();
	~Database();
	
	bool			Init(const DatabaseConfig& config);
	void			Shutdown();
	
	Table*			GetTable(const char* name);
	
	void			OnCheckpointTimeout();
	void			Checkpoint();

private:
    typedef HashMap<const char*, Table*> TableMap;

	DatabaseConfig	config;
	DbEnv*			env;
	ThreadPool*		cpThread;
	bool			running;
	Countdown		checkpointTimeout;
    TableMap        tableMap;
};

void WarmCache(char* dbPath, unsigned cacheSize);

// global
extern Database database;

#endif
