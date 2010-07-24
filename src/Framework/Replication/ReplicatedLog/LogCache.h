#ifndef LOGCACHE_H
#define LOGCACHE_H

#include "System/Buffers/Buffer.h"

class LogCache
{
public:
	bool			Set(uint64_t paxosID, const Buffer& value, bool commit);
	Buffer*			Get(uint64_t paxosID);
};

#endif
