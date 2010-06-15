#ifndef CURSOR_HPP
#define CURSOR_HPP

#include "System/Buffers/ByteWrap.h"
#include <db_cxx.h>

class Table; // forward

class Cursor
{
friend class Table;

public:
	bool	Start(const ByteString &key);
	bool	Delete();

	bool	Next(ByteWrap &key, ByteWrap &value);
	bool	Prev(ByteWrap &key, ByteWrap &value);

	bool	Close();

private:
	Dbc*	cursor;
};

#endif
