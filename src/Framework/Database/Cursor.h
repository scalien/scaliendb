#ifndef CURSOR_HPP
#define CURSOR_HPP

#include "System/Buffers/Buffer.h"
#include <db_cxx.h>

class Table; // forward

/*
===============================================================================

 Cursor class is a database cursor representation.

===============================================================================
*/

class Cursor
{
	friend class Table;

public:
	bool	Start(const Buffer &key);
	bool	Delete();

	bool	Next(Buffer &key, Buffer &value);
	bool	Prev(Buffer &key, Buffer &value);

	bool	Close();

private:
	Dbc*	cursor;
};

#endif
