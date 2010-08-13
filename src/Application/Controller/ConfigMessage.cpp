#include "ConfigMessage.h"
#include "System/Buffers/Buffer.h"

bool ConfigMessage::Read(const ReadBuffer& buffer_)
{
	int			read;
	unsigned	i;
	Buffer		epBuffer;
	ReadBuffer	rb;
	ReadBuffer	buffer;
	
	buffer = buffer_;
	
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

bool ConfigMessage::Write(Buffer& buffer)
{
	Buffer			epBuffer;
	ReadBuffer		rb;
	unsigned		i;
	
	switch (type)
	{
		case CONFIG_CREATE_CHUNK:
			buffer.Writef("%c:%U:%u", type, chunkID, numNodes);
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
