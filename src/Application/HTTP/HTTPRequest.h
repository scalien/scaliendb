#ifndef HTTP_REQUEST_H
#define HTTP_REQUEST_H

#include "IMF.h"

/*
===============================================================================================

 HTTPRequest

===============================================================================================
*/

class HTTPRequest
{
public:
	typedef IMFHeader::RequestLine RequestLine;
	enum State
	{
		REQUEST_LINE,
		HEADER,
		CONTENT
	};

	IMFHeader		header;
	RequestLine		line;
	State			state;
	int				pos;
	int				contentLength;
	
	void			Init();
	void			Free();
	int				Parse(char *buf, int len);
};

#endif
