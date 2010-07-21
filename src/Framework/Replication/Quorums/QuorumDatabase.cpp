#include "QuorumDatabase.h"
#include "Framework/Database/Table.h"

#define KEY(key) "/" key

uint64_t QuorumDatabase::GetPaxosID()
{
	return GetUint64("paxosID");
}

void QuorumDatabase::SetPaxosID(uint64_t paxosID)
{
	SetUint64("paxosID", paxosID);
}

bool QuorumDatabase::GetAccepted()
{
	Buffer		key;
	Buffer		value;
	int			ret;

	key.Write(prefix);
	key.Append(KEY("accepted"));
	
	ret = table->Get(NULL, key, value);
	if (!ret)
		return false;	// not found, return default
	
	if (value.GetLength() != 1)
		return false;	// incorrect value, return default
		
	if (value.GetCharAt(0) == '1')
		return true;
	
	return false;
}

void QuorumDatabase::SetAccepted(bool accepted)
{
	Buffer		key;
	Buffer		value;
	int			ret;
	
	key.Write(prefix);
	key.Append(KEY("accepted"));
	
	value.Writef("%d", accepted);
	ret = table->Set(&transaction, key, value);
}

uint64_t QuorumDatabase::GetPromisedProposalID()
{
	return GetUint64("promisedProposalID");
}

void QuorumDatabase::SetPromisedProposalID(uint64_t promisedProposalID)
{
	SetUint64("promisedProposalID", promisedProposalID);
}

uint64_t QuorumDatabase::GetAcceptedProposalID()
{
	return GetUint64("acceptedProposalID");
}

void QuorumDatabase::SetAcceptedProposalID(uint64_t acceptedProposalID)
{
	SetUint64("acceptedProposalID", acceptedProposalID);
}

void QuorumDatabase::GetAcceptedValue(Buffer& acceptedValue)
{
	Buffer		key;
	int			ret;
	
	key.Write(prefix);
	key.Append(KEY("acceptedValue"));
	
	ret = table->Get(NULL, key, acceptedValue);
}

void QuorumDatabase::SetAcceptedValue(const Buffer& acceptedValue)
{
	Buffer		key;
	int			ret;

	key.Write(prefix);
	key.Append(KEY("acceptedValue"));

	ret = table->Set(&transaction, key, acceptedValue);
}

void QuorumDatabase::Begin()
{
	transaction.Begin();
}

void QuorumDatabase::Commit()
{
	transaction.Commit();
}

bool QuorumDatabase::IsActive()
{
	// TODO for async write
	return false;
}

uint64_t QuorumDatabase::GetUint64(const char* name)
{
	Buffer		key;
	Buffer		value;
	uint64_t	u64;
	unsigned	nread;
	int			ret;
	
	key.Write(prefix);
	key.Append("/");
	key.Append(name);
	
	ret = table->Get(NULL, key, value);
	if (!ret)
		return false;

	nread = 0;
	u64 = strntouint64(value.GetBuffer(), value.GetLength(), &nread);
	if (nread != value.GetLength())
	{
		Log_Trace();
		u64 = 0;
	}
	
	return u64;
}

void QuorumDatabase::SetUint64(const char* name, uint64_t u64)
{
	Buffer	key;
	Buffer	value;
	int		ret;
	
	key.Write(prefix);
	key.Append("/");
	key.Append(name);
	
	value.Writef("%U", u64);
	ret = table->Set(&transaction, key, value);
}
