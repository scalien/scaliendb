/*
 *  QuorumDatabase.cpp
 *  ScalienDB
 *
 *  Created by Marton Trencseni on 7/21/10.
 *  Copyright 2010 __MyCompanyName__. All rights reserved.
 *
 */

#include "QuorumDatabase.h"

//	Log_Trace();
//
//	bool ret;
//	
//	if (table == NULL)
//		return false;
//	
//	writtenPaxosID = paxosID;
//	
//	buffers[0].Set(rprintf("%" PRIu64 "",	paxosID));
//	buffers[1].Set(rprintf("%d",			state.accepted));
//	buffers[2].Set(rprintf("%" PRIu64 "",	state.promisedProposalID));
//	buffers[3].Set(rprintf("%" PRIu64 "",	state.acceptedProposalID));
//	
//	mdbop.Init();
//	mdbop.SetCallback(&onDBComplete);
//	
//	ret = true;
//	ret &= mdbop.Set(table, "@@paxosID",			buffers[0]);
//	ret &= mdbop.Set(table, "@@accepted",			buffers[1]);
//	ret &= mdbop.Set(table, "@@promisedProposalID",	buffers[2]);
//	ret &= mdbop.Set(table, "@@acceptedProposalID",	buffers[3]);
//	ret &= mdbop.Set(table, "@@acceptedValue",		state.acceptedValue);
//
//	if (!ret)
//		return false;
//	
//	mdbop.SetTransaction(&transaction);
//	
//	dbWriter.Add(&mdbop);
//
//	return true;
//
// TODO: check that the transaction commited











//	bool ret;
//	unsigned nread = 0;
//	
//	if (table == NULL)
//		return false;
//
//	state.acceptedValue.Allocate(PAXOS_SIZE + 10*KB);
//
//	ret = table->Get(NULL, "@@paxosID", buffers[0]);
//	if (!ret)
//		return false;
//
//	nread = 0;
//	paxosID = strntouint64(buffers[0].buffer, buffers[0].length, &nread);
//	if (nread != (unsigned) buffers[0].length)
//	{
//		Log_Trace();
//		paxosID = 0;
//		return false;
//	}
//
//	ret = table->Get(NULL, "@@accepted", buffers[1]);
//	if (!ret)
//	{
//		// going from single to multi config
//		state.Init();
//		return (paxosID > 0);
//	}
//	
//	nread = 0;
//	state.accepted =
//		strntoint64(buffers[1].buffer, buffers[1].length, &nread) ? true : false;
//	if (nread != (unsigned) buffers[1].length)
//	{
//		state.accepted = false;
//		Log_Trace();
//		return false;
//	}
//
//	ret = table->Get(NULL, "@@promisedProposalID", buffers[2]);
//	if (!ret)
//		return false;
//
//	nread = 0;
//	state.promisedProposalID =
//		strntouint64(buffers[2].buffer, buffers[2].length, &nread);
//	if (nread != (unsigned) buffers[2].length)
//	{
//		state.promisedProposalID = 0;
//		Log_Trace();
//		return false;
//	}
//
//	ret = table->Get(NULL, "@@acceptedProposalID", buffers[3]);
//	if (!ret)
//		return false;
//
//	nread = 0;
//	state.acceptedProposalID =
//		strntouint64(buffers[3].buffer, buffers[3].length, &nread);
//	if (nread != (unsigned) buffers[3].length)
//	{
//		state.acceptedProposalID = 0;
//		Log_Trace();
//		return false;
//	}
//
//	ret = table->Get(NULL, "@@acceptedValue", state.acceptedValue);
