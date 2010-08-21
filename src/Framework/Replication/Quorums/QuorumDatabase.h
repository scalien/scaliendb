#ifndef QUORUMDATABASE_H
#define QUORUMDATABASE_H

#include "System/Platform.h"
#include "System/Buffers/Buffer.h"
#include "Framework/Database/Transaction.h"
#include "Framework/Database/Section.h"

class Table;	// forward

/*
===============================================================================

 QuorumDatabase

===============================================================================
*/

class QuorumDatabase
{
public:
	Section*			GetSection();
	void				SetSection(Section* section);
	
	Transaction*		GetTransaction();
	void				SetTransaction(Transaction* transaction);
	
	uint64_t			GetPaxosID();
	void				SetPaxosID(uint64_t paxosID);
	
	bool				GetAccepted();
	void				SetAccepted(bool accepted);
	
	uint64_t			GetPromisedProposalID();
	void				SetPromisedProposalID(uint64_t promisedProposalID);
	
	uint64_t			GetAcceptedProposalID();
	void				SetAcceptedProposalID(uint64_t acceptedProposalID);
	
	uint64_t			GetAcceptedRunID();
	void				SetAcceptedRunID(uint64_t acceptedRunID);

	void				GetAcceptedValue(Buffer& acceptedValue);
	void				SetAcceptedValue(Buffer& acceptedValue);

	void				GetLearnedValue(uint64_t paxosID, Buffer& value);
	void				SetLearnedValue(uint64_t paxosID, ReadBuffer& value);

	bool				IsActive();

private:
	Section*			section;
	Transaction*		transaction;
	
	uint64_t			GetUint64(const char* name);
	void				SetUint64(const char* name, uint64_t value);
};

#endif
