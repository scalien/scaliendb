#ifndef URL_PARAM_H
#define URL_PARAM_H

#include "System/Buffers/Buffer.h"
#include "System/Buffers/ReadBuffer.h"

// helper function for parsing url strings, does *not* do url decoding!
bool UrlParam_Parse(const char* url, char sep, unsigned num, /* ByteString* arg1, ByteString* arg2, */...);

// this class does url decoding
class UrlParam
{
public:
	UrlParam();

	bool			Init(const char* url, char sep);

	int				GetNumParams() const;

	const char*		GetParam(int nparam) const;
	int				GetParamLen(int nparam) const;

	int				GetParamIndex(const char* name, int namelen = -1) const;
	
	bool			Get(unsigned num, /* ByteString* */...) const;
	bool			GetNamed(const char* name, int namelen, ReadBuffer& bs) const;
	
private:
	const char*		url;
	char			sep;
	Buffer			params;
	Buffer			buffer;
	int				numParams;
	
	bool			Parse();
	void			AddParam(const char* s, int length);

	static char		DecodeChar(const char* s);
	static char		HexToChar(char uhex, char lhex);
	static char		HexdigitToChar(char hexdigit);
};

#endif
