#ifndef LOGCACHE_H
#define LOGCACHE_H

#include "System/Buffers/Buffer.h"
#include "Framework/Database/Table.h"

class LogCache
{
public:
	void			Init(Table* table);
	
	void			Set(uint64_t paxosID, ReadBuffer& value, bool commit);
	void			Get(uint64_t paxosID, Buffer& value);
	
private:
	Table*			table;
};

#endif
