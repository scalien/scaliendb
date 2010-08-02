#include "LogCache.h"

void LogCache::Init(Table* table_)
{
	table = table_;
}

void LogCache::Set(uint64_t paxosID, const ReadBuffer& value, bool commit)
{
	Buffer key;
	
	// TODO: commit chaining
	key.Writef("@@paxosID:%U", paxosID);
	table->Set(NULL, key.GetBuffer(), key.GetLength(), value.GetBuffer(), value.GetLength());
}

void LogCache::Get(uint64_t paxosID, Buffer& value)
{
	Buffer key;
	key.Writef("@@paxosID:%U", paxosID);
	table->Get(NULL, key, value);
}
