#include "ClusterMessage.h"
#include "System/Buffers/Buffer.h"

bool ClusterMessage::Read(ReadBuffer& buffer_)
{
	int			read;
	unsigned	i;
	Buffer		epBuffer;
	ReadBuffer	rb;
	ReadBuffer	buffer;
	char		proto;
	
	buffer = buffer_;
	
	if (buffer.GetLength() < 3)
		return false;
	
	switch (buffer.GetCharAt(2))
	{
		case CLUSTER_INFO:
			read = buffer.Readf("%c:%c:%U:%u", &proto, &type, &chunkID, &numNodes);
			assert(proto == CLUSTER_PROTOCOL_ID);
			buffer.Advance(read);
			for (i = 0; i < numNodes; i++)
			{
				read = buffer.Readf(":%U:%#R", &nodeIDs[i], &rb);
				epBuffer.Write(rb);
				epBuffer.NullTerminate();
				endpoints[i].Set(epBuffer.GetBuffer());
				buffer.Advance(read);
			}
			break;
	}
	
	return true;
	//return (read == (signed)buffer.GetLength() ? true : false);
}

bool ClusterMessage::Write(Buffer& buffer)
{
	Buffer			epBuffer;
	ReadBuffer		rb;
	unsigned		i;
	const char		proto = CLUSTER_PROTOCOL_ID;
	
	switch (type)
	{
		case CLUSTER_INFO:
			buffer.Writef("%c:%c:%U:%u", proto, type, chunkID, numNodes);
			for (i = 0; i < numNodes; i++)
			{
				epBuffer.Write(endpoints[i].ToString());
				rb.Wrap(epBuffer);
				buffer.Appendf(":%U:%#R", nodeIDs[i], &rb);
			}
			return true;
		default:
			return false;
	}
}
