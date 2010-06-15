#include "Cursor.h"

bool Cursor::Start(const ByteString &startKey)
{
	Dbt dbkey, dbvalue;
	int	flags;

	dbkey.set_data(startKey.Buffer());
	dbkey.set_size(startKey.Length());
	flags = DB_SET_RANGE;
	
	if (cursor->get(&dbkey, &dbvalue, flags) == 0)
		return true;
	
	return false;
}

bool Cursor::Delete()
{
	return (cursor->del(0) == 0);
}

bool Cursor::Next(ByteWrap &key, ByteWrap &value)
{
	Dbt dbkey, dbvalue;
	
	if (cursor->get(&dbkey, &dbvalue, DB_NEXT) != 0)
		return false;

	if (dbkey.get_size() > key.Length() || 
	    dbvalue.get_size() > value.Length())
	{
		return false;
	}

	key.Write((char*)dbkey.get_data(), dbkey.get_size());
	value.Write((char*)dbvalue.get_data(), dbvalue.get_size());

	return true;
}

bool Cursor::Prev(ByteWrap &key, ByteWrap &value)
{
	Dbt dbkey, dbvalue;
	
	if (cursor->get(&dbkey, &dbvalue, DB_PREV) != 0)
		return false;

	if (dbkey.get_size() > key.Length() || 
	    dbvalue.get_size() > value.Length())
	{
		return false;
	}

	key.Write((char*)dbkey.get_data(), dbkey.get_size());
	value.Write((char*)dbvalue.get_data(), dbvalue.get_size());

	return true;
}

bool Cursor::Close()
{
	return (cursor->close() == 0);
}
