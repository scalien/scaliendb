#include "StorageConfig.h"
#include "System/Config.h"

void StorageConfig::Init()
{
    chunkSize = configFile.GetIntValue("database.chunkSize", STORAGE_DEFAULT_CHUNKSIZE);
    logSegmentSize = configFile.GetIntValue("database.logSegmentSize", STORAGE_DEFAULT_LOGSEGMENTSIZE);
    fileChunkCacheSize = configFile.GetIntValue("database.cacheSize", STORAGE_DEFAULT_FILECHUNK_CACHESIZE);
}
