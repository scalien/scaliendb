#ifndef REPLICATEDLOG_H
#define REPLICATEDLOG_H

#include "Quorums/Quorum.h"

class Database;				// forward
class Message;	// forward
class QuorumTransport;		// forward

/*
===============================================================================

 ReplicationContext

===============================================================================
*/

class ReplicationContext
{
public:
	virtual ~ReplicationContext() {};
	
	virtual bool					IsMaster() const		= 0;
	virtual int						GetMaster() const		= 0;
	virtual unsigned				GetLogID() const		= 0;
	virtual uint64_t				GetPaxosID() const		= 0;

	virtual Quorum*					GetQuorum() const		= 0;
	virtual Database*				GetDatabase() const		= 0;
	virtual QuorumTransport*		GetTransport() const	= 0;
};

#endif
