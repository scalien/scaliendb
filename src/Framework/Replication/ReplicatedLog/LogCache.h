#ifndef LOGCACHE_H
#define LOGCACHE_H

#include "System/Buffers/Buffer.h"

class LogCache
{
public:
	void			Set(uint64_t paxosID, const ReadBuffer& value, bool commit);
	void			Get(uint64_t paxosID, Buffer& value);
};

#endif
