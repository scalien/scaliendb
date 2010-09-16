#ifndef HTTPCONTROLLERSESSION_H
#define HTTPCONTROLLERSESSION_H

#include "Application/Common/ClientSession.h"
#include "Application/Common/HTTPSession.h"
#include "HTTPControllerContext.h"

class Controller;		// forward
class ConfigMessage;	// forward
class UrlParam;			// forward

/*
===============================================================================================

 HTTPControllerSession

===============================================================================================
*/

class HTTPControllerSession : public ClientSession
{
public:

	void				SetController(Controller* controller);
	void				SetConnection(HTTPConnection* conn);

	bool				HandleRequest(HTTPRequest& request);

	// ClientSession interface
	virtual void		OnComplete(Message* message, bool status);
	virtual bool		IsActive();

private:
	void				PrintStatus();
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
	HTTPSession			session;
};

#endif
