#include "MessageConnection.h"

MessageConnection::MessageConnection()
{
	resumeRead.SetCallable(MFUNC(MessageConnection, OnResumeRead));
	resumeRead.SetDelay(1);
}

void MessageConnection::InitConnected(bool startRead)
{
	TCPConnection::InitConnected(startRead);
	
	Log_Trace();
}

void MessageConnection::Connect(Endpoint& endpoint_)
{
	Log_Trace();
	
	endpoint = endpoint_;
	TCPConnection::Connect(endpoint, MESSAGING_CONNECT_TIMEOUT);
}

void MessageConnection::Close()
{
	EventLoop::Remove(&resumeRead);
	TCPConnection::Close();	
}

void MessageConnection::Write(Buffer& msg)
{
	Buffer*		buffer;

	buffer = writer->AcquireBuffer();
	buffer->Writef("%#B", &msg);
	writer->WritePooled(buffer);
	writer->Flush();
}

void MessageConnection::Write(Message& msg)
{
	Buffer		writeBuffer;
	Buffer*		buffer;

	msg.Write(writeBuffer);
	buffer = writer->AcquireBuffer();
	buffer->Writef("%#B", &writeBuffer);
	writer->WritePooled(buffer);
	writer->Flush();
}

void MessageConnection::WritePriority(Buffer& msg)
{
	Buffer*		buffer;

	buffer = writer->AcquireBuffer();
	buffer->Writef("%#B", &msg);
	writer->WritePooledPriority(buffer);
	writer->Flush();
}

void MessageConnection::WritePriority(Message& msg)
{
	Buffer		writeBuffer;
	Buffer*		buffer;

	msg.Write(writeBuffer);
	buffer = writer->AcquireBuffer();
	buffer->Writef("%#B", &writeBuffer);
	writer->WritePooledPriority(buffer);
	writer->Flush();
}

void MessageConnection::Write(Buffer& prefix, Buffer& msg)
{
	unsigned	length;
	Buffer*		buffer;

	buffer = writer->AcquireBuffer();
	length = prefix.GetLength() + 1 + msg.GetLength();
	buffer->Writef("%u:%B:%B", length, &prefix, &msg);
	writer->WritePooled(buffer);
	writer->Flush();
}

void MessageConnection::Write(Buffer& prefix, Message& msg)
{
	unsigned	length;
	Buffer		writeBuffer;
	Buffer*		buffer;

	msg.Write(writeBuffer);
	buffer = writer->AcquireBuffer();
	length = prefix.GetLength() + 1 + writeBuffer.GetLength();
	buffer->Writef("%u:%B:%B", length, &prefix, &writeBuffer);
	writer->WritePooled(buffer);
	writer->Flush();
}

void MessageConnection::WritePriority(Buffer& prefix, Buffer& msg)
{
	unsigned length;
	Buffer* buffer;

	buffer = writer->AcquireBuffer();
	length = prefix.GetLength() + 1 + msg.GetLength();
	buffer->Writef("%u:%B:%B", length, &prefix, &msg);
	writer->WritePooledPriority(buffer);
	writer->Flush();
}

void MessageConnection::WritePriority(Buffer& prefix, Message& msg)
{
	unsigned	length;
	Buffer		writeBuffer;
	Buffer*		buffer;

	msg.Write(writeBuffer);
	buffer = writer->AcquireBuffer();
	length = prefix.GetLength() + 1 + writeBuffer.GetLength();
	buffer->Writef("%u:%B:%B", length, &prefix, &writeBuffer);
	writer->WritePooledPriority(buffer);
	writer->Flush();
}

void MessageConnection::OnConnect()
{
	Buffer		buffer;
	ReadBuffer	rb;

	TCPConnection::OnConnect();
	
	AsyncRead();		
}

void MessageConnection::OnClose()
{
	Log_Trace();
	
	if (connectTimeout.IsActive())
		return;
	
	Close();	
}

void MessageConnection::OnRead()
{
	bool			yield, closed;
	unsigned		pos, msglength, nread, msgbegin, msgend, required;
	uint64_t		start;
	Stopwatch		sw;
	ReadBuffer		msg;

	sw.Start();
	
	Log_Trace("Read buffer: %.*s", P(tcpread.buffer));
	
	tcpread.requested = IO_READ_ANY;
	pos = 0;
	start = Now();
	yield = false;

	while(true)
	{
		msglength = BufferToUInt64(tcpread.buffer->GetBuffer() + pos,
		 tcpread.buffer->GetLength() - pos, &nread);
		
		if (msglength > tcpread.buffer->GetSize() - NumDigits(msglength) - 1)
		{
			Log_Trace();
			required = msglength + NumDigits(msglength) + 1;
			if (required > 10*MB)
			{
				Log_Trace();
				OnClose();
				return;
			}
			tcpread.buffer->Allocate(required);
			break;
		}
		
		if (nread == 0 || (unsigned) tcpread.buffer->GetLength() - pos <= nread)
			break;
			
		if (tcpread.buffer->GetCharAt(pos + nread) != ':')
		{
			Log_Trace("Message protocol error");
			OnClose();
			return;
		}
	
		msgbegin = pos + nread + 1;
		msgend = pos + nread + 1 + msglength;
		
		if ((unsigned) tcpread.buffer->GetLength() < msgend)
		{
			tcpread.requested = msgend - pos;
			break;
		}

		msg.SetBuffer(tcpread.buffer->GetBuffer() + msgbegin);
		msg.SetLength(msglength);
		closed = OnMessage(msg);
		if (closed)
			return;

		pos = msgend;
		
		// if the user called Close() in OnMessageRead()
		if (state != CONNECTED)
			return;
		
		if (tcpread.buffer->GetLength() == msgend)
			break;

		if (Now() - start > MESSAGING_YIELD_TIME)
		{
			// let other code run every YIELD_TIME msec
			yield = true;
			if (resumeRead.IsActive())
				ASSERT_FAIL();
			EventLoop::Add(&resumeRead);
			break;
		}
	}
	
	if (pos > 0)
	{
		memmove(tcpread.buffer->GetBuffer(), tcpread.buffer->GetBuffer() + pos,
		 tcpread.buffer->GetLength() - pos);
		tcpread.buffer->Shorten(pos);
	}
	
	if (state == CONNECTED
	 && !tcpread.active && !yield)
		IOProcessor::Add(&tcpread);

	sw.Stop();
	Log_Trace("time spent in OnRead(): %ld", sw.Elapsed());
}

void MessageConnection::OnResumeRead()
{
	Log_Trace();

	OnRead();
}

void MessageConnection::OnConnectTimeout()
{
	TCPConnection::OnConnectTimeout();
	
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	Close();
	
	TCPConnection::Connect(endpoint, MESSAGING_CONNECT_TIMEOUT);
}
