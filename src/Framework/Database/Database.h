#ifndef DATABASE_H
#define DATABASE_H

#include <db_cxx.h>
#include "System/Common.h"
#include "System/ThreadPool.h"
#include "System/Events/Callable.h"
#include "System/Events/Timer.h"
#include "System/Events/Countdown.h"
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
	DatabaseConfig	config;
	DbEnv*			env;
	Table*			keyspace;
	ThreadPool*		cpThread;
	bool			running;
	Countdown		checkpointTimeout;
};

void WarmCache(char* dbPath, unsigned cacheSize);

// global
extern Database database;

#endif
