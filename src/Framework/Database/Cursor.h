#ifndef CURSOR_HPP
#define CURSOR_HPP

#include "System/Buffer/Buffer.h"
#include <db_cxx.h>

class Table; // forward

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
