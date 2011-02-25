#include "StorageConfig.h"
#include "System/Config.h"

void StorageConfig::Init()
{
    chunkSize = configFile.GetIntValue("database.chunkSize", STORAGE_DEFAULT_CHUNKSIZE);
    logSegmentSize = configFile.GetIntValue("database.logSegmentSize", STORAGE_DEFAULT_LOGSEGMENTSIZE);
    fileChunkCacheSize = (uint64_t) configFile.GetInt64Value("database.cacheSize", STORAGE_DEFAULT_FILECHUNK_CACHESIZE);
}
