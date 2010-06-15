#ifndef DATABASE_CONFIG_H
#define DATABASE_CONFIG_H

#define DATABASE_CONFIG_DIR					"."
#define DATABASE_CONFIG_PAGE_SIZE			65536
#define DATABASE_CONFIG_CACHE_SIZE			500*MB
#define DATABASE_CONFIG_LOG_BUFFER_SIZE		250*MB
#define DATABASE_CONFIG_LOG_MAX_FILE		0
#define DATABASE_CONFIG_CHECKPOINT_TIMEOUT	60
#define DATABASE_CONFIG_VERBOSE				false

#define DATABASE_CONFIG_DIRECT_DB			true
#define	DATABASE_CONFIG_TXN_NOSYNC			false
#define DATABASE_CONFIG_TXN_WRITE_NOSYNC	true

class DatabaseConfig
{
public:
	DatabaseConfig()
	{
		dir = DATABASE_CONFIG_DIR;
		pageSize = DATABASE_CONFIG_PAGE_SIZE;
		cacheSize = DATABASE_CONFIG_CACHE_SIZE;
		logBufferSize = DATABASE_CONFIG_LOG_BUFFER_SIZE;
		logMaxFile = DATABASE_CONFIG_LOG_MAX_FILE;
		checkpointTimeout = DATABASE_CONFIG_CHECKPOINT_TIMEOUT;
		verbose = DATABASE_CONFIG_VERBOSE;
		
		directDB = DATABASE_CONFIG_DIRECT_DB;
		txnNoSync = DATABASE_CONFIG_TXN_NOSYNC;
		txnWriteNoSync = DATABASE_CONFIG_TXN_WRITE_NOSYNC;
	}
	
	const char*	dir;
	int			pageSize;
	int			cacheSize;
	int			logBufferSize;
	int			logMaxFile;
	int			checkpointTimeout;
	bool		verbose;

	bool		directDB;
	bool		txnNoSync;
	bool		txnWriteNoSync;
};

#endif
