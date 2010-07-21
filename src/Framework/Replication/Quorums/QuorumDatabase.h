#ifndef QUORUMDATABASE_H
#define QUORUMDATABASE_H

/*
===============================================================================

 QuorumDatabase

===============================================================================
*/

class QuorumDatabase
{
public:
	uint64_t			GetPaxosID();
	void				SetPaxosID(uint64_t paxosID);
	
	bool				GetAccepted(); // if not accepted => no acceptedProposalID+value
	void				SetAccepted(bool accepted);
	
	uint64_t			GetPromisedProposalID();
	void				SetPromisedProposalID(uint64_t promisedProposalID);
	
	uint64_t			GetAcceptedProposalID();
	void				SetAcceptedProposalID(uint64_t acceptedProposalID);
	
	Buffer*				GetAcceptedValue();
	void				SetAcceptedValue(Buffer* acceptedValue);
};

#endif
