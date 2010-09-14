#ifndef SDBPCONTROLLERCONTEXT_H
#define SDBPCONTROLLERCONTEXT_H

#include "Application/SDBP/SDBPContext.h"
#include "Controller.h"

/*
===============================================================================

 SDBPControllerContext

===============================================================================
*/

class SDBPControllerContext : public SDBPContext
{
public:
	void			SetController(Controller* controller);

	/* ---------------------------------------------------------------------------------------- */
	/* SDBPContext interface:																	*/
	/*																							*/
	bool			IsValidRequest(ClientRequest& request);
	bool			ProcessRequest(SDBPConnection* conn, ClientRequest& request);
	void			OnComplete(SDBPConnection* conn, Message* message);
	/* ---------------------------------------------------------------------------------------- */

	ConfigMessage	ConvertRequest(ClientRequest& request);
	
private:
	Controller*		controller;
};


#endif
