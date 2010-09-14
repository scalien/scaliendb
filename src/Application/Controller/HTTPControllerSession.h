#ifndef HTTPCONTROLLERSESSION_H
#define HTTPCONTROLLERSESSION_H

#include "Application/Common/ClientSession.h"
#include "HTTPControllerContext.h"

class Controller;		// forward
class ConfigMessage;	// forward
class UrlParam;			// forward

/*
===============================================================================

 HTTPControllerSession

===============================================================================
*/

class HTTPControllerSession : public ClientSession
{
public:

	void				SetConnection(HTTPConnection* conn);
	void				SetController(Controller* controller);

	// ClientSession interface
	virtual void		OnComplete(Message* message, bool status);
	virtual bool		IsActive();

	bool				HandleRequest(HTTPRequest& request);

private:
	enum Type
	{
		PLAIN,
		HTML,
		JSON
	};

	void				PrintStatus();
	void				ResponseFail();
	void				ParseType(ReadBuffer& cmd);
	bool				ProcessCommand(ReadBuffer& cmd, UrlParam& params);
	void				ProcessGetMaster();
	ConfigMessage*		ProcessControllerCommand(ReadBuffer& cmd, UrlParam& params);
	ConfigMessage*		ProcessCreateQuorum(UrlParam& params);
	ConfigMessage*		ProcessIncreaseQuorum(UrlParam& params);
	ConfigMessage*		ProcessDecreaseQuorum(UrlParam& params);
	ConfigMessage*		ProcessCreateDatabase(UrlParam& params);
	ConfigMessage*		ProcessRenameDatabase(UrlParam& params);
	ConfigMessage*		ProcessDeleteDatabase(UrlParam& params);
	ConfigMessage*		ProcessCreateTable(UrlParam& params);
	ConfigMessage*		ProcessRenameTable(UrlParam& params);
	ConfigMessage*		ProcessDeleteTable(UrlParam& params);

	Controller*			controller;
	// Generic HTTPSession
	HTTPConnection*		conn;
	Type				type;
};

#endif
