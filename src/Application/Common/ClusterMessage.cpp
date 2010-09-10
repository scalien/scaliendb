#include "ClusterMessage.h"

void ClusterMessage::SetNodeID(uint64_t nodeID_)
{
	type = CLUSTER_SET_NODEID;
	nodeID = nodeID_;
}

bool ClusterMessage::Read(ReadBuffer& buffer)
{
	// TODO:
}

bool ClusterMessage::Write(Buffer& buffer)
{
	// TODO:
}