#ifndef QUORUMDATABASE_H
#define QUORUMDATABASE_H

#include "System/Platform.h"
#include "System/Buffers/Buffer.h"
#include "Framework/Database/Transaction.h"

class Table;	// forward

/*
===============================================================================

 QuorumDatabase

===============================================================================
*/

class QuorumDatabase
{
public:
	void				Init(Table* table);
	
	uint64_t			GetPaxosID();
	void				SetPaxosID(uint64_t paxosID);
	
	bool				GetAccepted(); // if not accepted => no acceptedProposalID+value
	void				SetAccepted(bool accepted);
	
	uint64_t			GetPromisedProposalID();
	void				SetPromisedProposalID(uint64_t promisedProposalID);
	
	uint64_t			GetAcceptedProposalID();
	void				SetAcceptedProposalID(uint64_t acceptedProposalID);
	
	uint64_t			GetAcceptedRunID();
	void				SetAcceptedRunID(uint64_t acceptedRunID);

	void				GetAcceptedValue(Buffer& acceptedValue);
	void				SetAcceptedValue(Buffer& acceptedValue);

	void				Begin();
	void				Commit();
	bool				IsActive();

//private: TODO HACK
	Table*				table;
	Transaction			transaction;
	Buffer				prefix;
	
	uint64_t			GetUint64(const char* name);
	void				SetUint64(const char* name, uint64_t value);
};

#endif
