#include "MessageConnection.h"

MessageConnection::MessageConnection()
{
	running = true;
	resumeRead.SetCallable(MFUNC(MessageConnection, OnResumeRead));
	resumeRead.SetDelay(1);
}

void MessageConnection::Init(bool startRead)
{
	running = true;
	TCPConnection::InitConnected(startRead);
}

void MessageConnection::OnRead()
{
	bool			yield;
	unsigned		pos, msglength, nread, msgbegin, msgend;
	uint64_t		start;
	Stopwatch		sw;

	sw.Start();
	
	Log_Trace("Read buffer: %.*s", P(tcpread.buffer));
	
	tcpread.requested = IO_READ_ANY;
	pos = 0;
	start = Now();
	yield = false;

	while(running)
	{
		msglength = strntouint64(tcpread.buffer->GetBuffer() + pos,
		 tcpread.buffer->GetLength() - pos, &nread);
		
		if (msglength > tcpread.buffer->GetSize() - numlen(msglength) - 1)
		{
			OnClose();
			return;
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
		OnMessageRead();
		
		pos = msgend;
		
		// if the user called Close() in OnMessageRead()
		if (state != CONNECTED)
			return;
		
		if (tcpread.buffer->GetLength() == msgend)
			break;

		if (Now() - start > YIELD_TIME && running)
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
	 && running && !tcpread.active && !yield)
		IOProcessor::Add(&tcpread);

	sw.Stop();
	Log_Trace("time spent in OnRead(): %ld", sw.Elapsed());
}

void MessageConnection::OnResumeRead()
{
	Log_Trace();

	OnRead();
}

ReadBuffer MessageConnection::GetMessage()
{
	return msg;
}

void MessageConnection::Stop()
{
	Log_Trace();
	running = false;
}

void MessageConnection::Continue()
{
	Log_Trace();
	running = true;
	OnRead();
}

void MessageConnection::Close()
{
	EventLoop::Remove(&resumeRead);
	TCPConnection::Close();
}
