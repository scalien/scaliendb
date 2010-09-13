#include "ClusterMessage.h"

void ClusterMessage::SetNodeID(uint64_t nodeID_)
{
	type = CLUSTER_SET_NODEID;
	nodeID = nodeID_;
}

bool ClusterMessage::Read(ReadBuffer& buffer)
{
	int			read;
		
	if (buffer.GetLength() < 1)
		return false;
	
	switch (buffer.GetCharAt(0))
	{
		case CLUSTER_SET_NODEID:
			read = buffer.Readf("%c:%U",
			 &type, &nodeID);
			break;		
		default:
			return false;
	}
	
	return (read == (signed)buffer.GetLength() ? true : false);}

bool ClusterMessage::Write(Buffer& buffer)
{
	switch (type)
	{
		case CLUSTER_SET_NODEID:
			buffer.Writef("%c:%U",
			 type, nodeID);
			return true;
		default:
			return false;
	}
	
	return true;
}