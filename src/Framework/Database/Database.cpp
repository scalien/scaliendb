#include <string.h>

#include "System/Events/Callable.h"
#include "System/Events/EventLoop.h"
#include "System/Time.h"
#include "System/Log.h"
#include "Database.h"
#include "Table.h"
#ifdef _WIN32
#include <windows.h>
#endif

#define DATABASE_DEFAULT_CACHESIZE	(256*1024)

// the global database
Database database;

#define LOG_BUFFER_ALLOC_ERROR "Unable to allocate memory for the log buffer"

inline size_t Hash(const char* s)
{
    size_t h;
    unsigned char *p;
    
    h = 0;
    for (p = (unsigned char *)s; *p != '\0'; p++) {
        h = 37 * h + *p;
    }
    
    return h;
}

static void DatabaseError(const DbEnv* /*dbenv*/,
						  const char* /*errpfx*/,
						  const char* msg)
{
	if (strcmp(msg, LOG_BUFFER_ALLOC_ERROR) == 0)
		STOP_FAIL("Not enough memory to allocate log buffer!\nChange database.logBufferSize in the config file!", 1);
	
	Log_Trace("%s", msg);
}

static void DatabaseTrace(const DbEnv* /*dbenv*/, const char* msg)
{
	Log_Trace("%s", msg);
}

Database::Database()
{
	checkpointTimeout.SetCallable(MFUNC(Database, OnCheckpointTimeout));
}

Database::~Database()
{
	Shutdown();
}

bool Database::Init(const DatabaseConfig& config_)
{
	u_int32_t flags = DB_CREATE | DB_INIT_MPOOL |
	DB_INIT_TXN | DB_RECOVER | DB_THREAD | DB_PRIVATE;
	int mode = 0;
	int ret;

	env = new DbEnv(DB_CXX_NO_EXCEPTIONS);
	
	env->set_errcall(DatabaseError);
	env->set_msgcall(DatabaseTrace);
	
	config = config_;
	
	if (config.cacheSize != 0)
	{
		u_int32_t gbytes = config.cacheSize / (1024 * 1024 * 1024);
		u_int32_t bytes = config.cacheSize % (1024 * 1024 * 1024);
		
		//env.set_cache_max(gbytes, bytes);
		env->set_cachesize(gbytes, bytes, 4);
	}

	if (config.logBufferSize != 0)
		env->set_lg_bsize(config.logBufferSize);
	
	if (config.logMaxFile != 0)
		env->set_lg_max(config.logMaxFile);
		
	if (config.verbose)
	{
		env->set_msgcall(DatabaseTrace);
#ifdef DB_VERB_FILEOPS
		env->set_verbose(DB_VERB_FILEOPS, 1);
#endif
#ifdef DB_VERB_FILEOPS_ALL
		env->set_verbose(DB_VERB_FILEOPS_ALL, 1);
#endif
#ifdef DB_VERB_RECOVERY
		env->set_verbose(DB_VERB_RECOVERY, 1);
#endif
#ifdef DB_VERB_REGISTER
		env->set_verbose(DB_VERB_REGISTER, 1);
#endif
#ifdef DB_VERB_REPLICATION
		env->set_verbose(DB_VERB_REPLICATION, 1);
#endif
#ifdef DB_VERB_WAITSFOR
		env->set_verbose(DB_VERB_WAITSFOR, 1);
#endif
	}
	
	Log_Trace();
	ret = env->open(config.dir, flags, mode);
	Log_Trace("ret = %d", ret);
	if (ret != 0)
		return false;

#ifdef DB_LOG_AUTOREMOVE
	env->set_flags(DB_LOG_AUTOREMOVE, 1);
#else
	env->log_set_config(DB_LOG_AUTO_REMOVE, 1);
#endif

#ifdef DB_DIRECT_DB	
	ret = env->set_flags(DB_DIRECT_DB, config.directDB);
#endif

#ifdef DB_TXN_NOSYNC
	ret = env->set_flags(DB_TXN_NOSYNC, config.txnNoSync);
#endif

#ifdef DB_TXN_WRITE_NOSYNC
	ret = env->set_flags(DB_TXN_WRITE_NOSYNC, config.txnWriteNoSync);
#endif

	Checkpoint();
	
	running = true;
	if (config.checkpointTimeout)
	{
		checkpointTimeout.SetDelay(config.checkpointTimeout * 1000);
		EventLoop::Add(&checkpointTimeout);
	}
	cpThread = ThreadPool::Create(1);
	cpThread->Start();
	
	return true;
}

void Database::Shutdown()
{
	HashNode<const char*, Table*>*	it;
	
	if (!running)
		return;

	for (it = tableMap.First(); it != NULL; it = tableMap.Next(it))
		delete it->Value();
	
	tableMap.Clear();

	running = false;
	cpThread->Stop();
	delete cpThread;
	env->close(0);

	// Bug #222: On Windows deleting the BDB environment object causes crash
	// On other platforms it works and causes a memleak if not deleted.
#ifndef PLATFORM_WINDOWS
	delete env;
#endif
}

Table* Database::GetTable(const char* name)
{
    Table** ptable;
    Table*  table;
    
    ptable = tableMap.Get(name);
    if (ptable == NULL)
    {
        // TODO: better error handling, preferably with a Table::Open function
        table = new Table(this, name, config.pageSize);
        tableMap.Set(name, table);
        return table;
    }
    
	return *ptable;
}

void Database::OnCheckpointTimeout()
{
	cpThread->Execute(MFUNC(Database, Checkpoint));
	if (config.checkpointTimeout)
		EventLoop::Reset(&checkpointTimeout);
}

void Database::Checkpoint()
{
	int ret;

	Log_Trace("started");
	ret = env->txn_checkpoint(100*1000 /* in kilobytes */, 0, 0);
	if (ret < 0)
		ASSERT_FAIL();
	Log_Trace("finished");
}

static void WarmFileCache(char* filepath, unsigned cacheSize)
{
	int i, num;
	char buf[1*MB];
	FILE* f;

	f = fopen(filepath, "rb");
	if (f == NULL)
		return;

	num = (int) ceil((double)cacheSize / SIZE(buf));
	for (i = 0; i < num; i++)
		fread(buf, 1, SIZE(buf), f);

	fclose(f);
}

static void WarmDatabaseCache(char* dbPath, unsigned cacheSize)
{
	char filepath[4096];

    // TODO: make this smarter
	snprintf(filepath, SIZE(filepath), "%s/keyspace", dbPath);

	WarmFileCache(filepath, cacheSize);
}

static void WarmLogCache(char* dbPath)
{
	char buf[4096];
	
#ifdef _WIN32
	BOOL next;
	WIN32_FIND_DATA FindFileData;
	HANDLE hFind;

	snprintf(buf, SIZE(buf), "%s/log.*", dbPath);
	strrep(buf, '/', '\\');
	hFind = FindFirstFile(buf, &FindFileData);
	if (hFind == INVALID_HANDLE_VALUE)
		return;
	next = true;
	while (next)
	{
		snprintf(buf, SIZE(buf), "%s/%s", dbPath, FindFileData.cFileName);
		strrep(buf, '/', '\\');
		WarmFileCache(buf, 11000000);
		next = FindNextFile(hFind, &FindFileData);
	}
	FindClose(hFind);
#else
	snprintf(buf, SIZE(buf), "cat %s/log.* > /dev/null 2> /dev/null", dbPath);
	system(buf);
#endif
}

void WarmCache(char* dbPath, unsigned cacheSize)
{
	Log_Message("Warming O/S file cache...");
	WarmLogCache(dbPath);
	WarmDatabaseCache(dbPath, cacheSize);
	Log_Message("Cache warmed");
}
