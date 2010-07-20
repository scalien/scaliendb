#ifndef JSON_SESSION_H
#define JSON_SESSION_H

#include "System/Buffers/ReadBuffer.h"

class HttpConn;

class JSONSession
{
public:
	void			Init(HttpConn* conn, const ReadBuffer& jsonCallback);

	void			PrintStatus(const char* status, const char* type = NULL);

	void			PrintString(const char* s, unsigned len);
	void			PrintNumber(double number);
	void			PrintBool(bool b);
	void			PrintNull();
	
	void			PrintObjectStart(const char* name, unsigned len);
	void			PrintObjectEnd();

private:
	ReadBuffer		jsonCallback;
	HttpConn*		conn;
};

#endif
