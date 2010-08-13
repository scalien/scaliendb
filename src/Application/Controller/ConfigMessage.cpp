#include "ConfigMessage.h"
#include "System/Buffers/Buffer.h"

bool ConfigMessage::Read(ReadBuffer buffer)
{
	int			read;
	unsigned	i;
	ReadBuffer	rb;
		
	if (buffer.GetLength() < 1)
		return false;
	
	switch (buffer.GetCharAt(0))
	{
		case CONFIG_CREATE_CHUNK:
			read = buffer.Readf("%c:%U:%u", &type, &chunkID, &numNodes);
			buffer.Advance(read);
			for (i = 0; i < numNodes; i++)
			{
				read = buffer.Readf(":%U:%#R", &nodeIDs[i], &rb);
				endpoints[i].Set(rb);
				buffer.Advance(read);
			}
			break;
	}
	
	return true;
	//return (read == (signed)buffer.GetLength() ? true : false);
}

bool ConfigMessage::Write(Buffer& buffer)
{
	ReadBuffer		rb;
	unsigned		i;
	
	switch (type)
	{
		case CONFIG_CREATE_CHUNK:
			buffer.Writef("%c:%U:%u", type, chunkID, numNodes);
			for (i = 0; i < numNodes; i++)
			{
				rb = endpoints[i].ToReadBuffer();
				buffer.Appendf(":%U:%#R", nodeIDs[i], &rb);
			}
			return true;
		default:
			return false;
	}
}
