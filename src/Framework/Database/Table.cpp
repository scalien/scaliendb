#include <db_cxx.h>

#include "Table.h"
#include "Database.h"
#include "System/Buffers/ByteWrap.h"
#include "System/Buffers/StaticArray.h"
#include "Transaction.h"

Table::Table(Database* database, const char *name, int pageSize) :
database(database)
{
	DbTxn *txnid = NULL;
	const char *filename = name;
	const char *dbname = NULL;
	DBTYPE type = DB_BTREE;
	u_int32_t flags = DB_CREATE | DB_AUTO_COMMIT |
	DB_NOMMAP 
#ifdef DB_READ_UNCOMMITTED
	| DB_READ_UNCOMMITTED
#endif
	;

	int mode = 0;
	
	db = new Db(database->env, 0);
	if (pageSize != 0)
		db->set_pagesize(pageSize);
		
	Log_Trace();
	if (db->open(txnid, filename, dbname, type, flags, mode) != 0)
	{
		db->close(0);
		if (isdir(filename))
		{
			STOP_FAIL(rprintf(
					  "Could not create database file '%s' "
					  "because a folder '%s' exists",
					  filename, filename), 1);
		}
		STOP_FAIL("Could not open database", 1);
	}
	Log_Trace();
}

Table::~Table()
{
	db->close(0);

	// Bug #222: On Windows deleting the BDB database object causes crash
	// On other platforms it works and causes a memleak if not deleted.
#ifndef PLATFORM_WINDOWS
	delete db;
#endif
}

bool Table::Iterate(Transaction* tx, Cursor& cursor)
{
	DbTxn* txn = NULL;
	u_int32_t flags;

	flags = 0;

	if (tx)
		txn = tx->txn;
	
	if (db->cursor(txn, &cursor.cursor, flags) == 0)
		return true;
	else
		return false;
}

bool Table::Get(Transaction* tx,
				const ByteString &key,
				ByteWrap &value)
{
	Dbt dbtKey;
	Dbt dbtValue;
	DbTxn* txn = NULL;
	int ret;
	
	if (tx)
		txn = tx->txn;

	dbtKey.set_flags(DB_DBT_USERMEM);
	dbtKey.set_data(key.Buffer());
	dbtKey.set_ulen(key.Length());
	dbtKey.set_size(key.Length());
	
	dbtValue.set_flags(DB_DBT_USERMEM);
	dbtValue.set_data(value.Buffer());
	dbtValue.set_ulen(value.Size());
	
	ret = db->get(txn, &dbtKey, &dbtValue, 0);

	// DB_PAGE_NOTFOUND can occur with parallel PRUNE and GET operations
	// probably because we have DB_READ_UNCOMMITED turned on
	if (ret == DB_KEYEMPTY || ret == DB_NOTFOUND || ret == DB_PAGE_NOTFOUND)
		return false;
	
	if (dbtValue.get_size() > (size_t) value.Size())
		return false;

	value.SetLength(dbtValue.get_size());
	
	return true;
}

bool Table::Get(Transaction* tx, const char* key, ByteWrap &value)
{
	int len;
	
	len = strlen(key);
	
	ByteString bsKey((char*) key, len);
	
	return Table::Get(tx, bsKey, value);
}

bool Table::Get(Transaction* tx, const char* key, uint64_t& value)
{
	StaticArray<32> ba;
	ByteWrap bw;
	unsigned nread;
	
	bw = ba.ToByteWrap();
	if (!Get(tx, key, bw))
		return false;
	
	value = strntouint64(ba.Buffer(), ba.Length(), &nread);
	
	if (nread != ba.Length())
		return false;
	
	return true;
}

bool Table::Set(Transaction* tx,
				const ByteString &key,
				const ByteString &value)
{
	Dbt dbtKey(key.Buffer(), key.Length());
	Dbt dbtValue(value.Buffer(), value.Length());
	DbTxn* txn = NULL;
	int ret;

	if (tx)
		txn = tx->txn;
	
	ret = db->put(txn, &dbtKey, &dbtValue, 0);
	if (ret != 0)
		return false;
	
	return true;
}

bool Table::Set(Transaction* tx,
				const char* key,
				const ByteString &value)
{
	int len;
	
	len = strlen(key);
	
	ByteString bsKey((char*) key, len);
	
	return Table::Set(tx, bsKey, value);
}

bool Table::Set(Transaction* tx,
				const char* key,
				const char* value)
{
	int len;
	
	len = strlen(key);
	ByteString bsKey((char*) key, len);
	
	len = strlen(value);
	ByteString bsValue((char*) value, len);
	
	return Table::Set(tx, bsKey, bsValue);
}

bool Table::Delete(Transaction* tx, const ByteString &key)
{
	Dbt dbtKey(key.Buffer(), key.Length());
	DbTxn* txn = NULL;
	int ret;

	if (tx)
		txn = tx->txn;
	
	ret = db->del(txn, &dbtKey, 0);
	if (ret < 0)
		return false;
	
	return true;
}

bool Table::Prune(Transaction* tx, const ByteString &prefix)
{
	Dbc* cursor = NULL;
	u_int32_t flags = DB_NEXT;

	DbTxn* txn;
	
	if (tx == NULL)
		txn = NULL;
	else
		txn = tx->txn;

	if (db->cursor(txn, &cursor, 0) != 0)
		return false;
	
	Dbt key, value;
	if (prefix.Length() > 0)
	{
		key.set_data(prefix.Buffer());
		key.set_size(prefix.Length());
		flags = DB_SET_RANGE;		
	}
	
	while (cursor->get(&key, &value, flags) == 0)
	{
		if (key.get_size() < prefix.Length())
			break;
		
		if (strncmp(prefix.Buffer(), (char*)key.get_data(), prefix.Length()) != 0)
			break;
		
		// don't delete keys starting with @@
		if (!(key.get_size() >= 2 &&
		 ((char*)key.get_data())[0] == '@' &&
		 ((char*)key.get_data())[1] == '@'))
			cursor->del(0);

		flags = DB_NEXT;
	}
	
	if (cursor)
		cursor->close();
	
	return true;
}

bool Table::Truncate(Transaction* tx)
{
	Log_Trace();

	u_int32_t count;
	u_int32_t flags = 0;
	int ret;
	DbTxn* txn;
	
	txn = tx ? tx->txn : NULL;
	// TODO error handling
	if ((ret = db->truncate(txn, &count, flags)) != 0)
		Log_Trace("truncate() failed");
	return true;
}

bool Table::Visit(TableVisitor &tv)
{
	if (!tv.IsForward())
		return VisitBackward(tv);

	ByteString bsKey, bsValue;
	Dbc* cursor = NULL;
	bool ret = true;
	u_int32_t flags = DB_NEXT;

	// TODO call tv.OnComplete() or error handling
	if (db->cursor(NULL, &cursor, 0) != 0)
		return false;
	
	Dbt key, value;
	if (tv.GetStartKey() && tv.GetStartKey()->Length() > 0)
	{
		key.set_data(tv.GetStartKey()->Buffer());
		key.set_size(tv.GetStartKey()->Length());
		flags = DB_SET_RANGE;		
	}
	
	while (cursor->get(&key, &value, flags) == 0)
	{
		bsKey.SetLength(key.get_size());
		bsKey.SetBuffer((char*) key.get_data());
		
		bsValue.SetLength(value.get_size());
		bsValue.SetBuffer((char*) value.get_data());

		ret = tv.Accept(bsKey, bsValue);
		if (!ret)
			break;
		
		if (bsKey.Length() > 2 && bsKey.CharAt(0) == '@' && bsKey.CharAt(1) == '@')
		{
			key.set_data((void*)"@@~");
			key.set_size(3);
			flags = DB_SET_RANGE;
		}
		else
			flags = DB_NEXT;
	}
	
	cursor->close();	
	tv.OnComplete();
	
	return ret;
}

bool Table::VisitBackward(TableVisitor &tv)
{
	ByteString bsKey, bsValue;
	Dbc* cursor = NULL;
	bool ret = true;
	u_int32_t flags = DB_PREV;

	// TODO call tv.OnComplete() or error handling
	if (db->cursor(NULL, &cursor, 0) != 0)
		return false;
	
	Dbt key, value;
	if (tv.GetStartKey() && tv.GetStartKey()->Length() > 0)
	{
		key.set_data(tv.GetStartKey()->Buffer());
		key.set_size(tv.GetStartKey()->Length());
		flags = DB_SET_RANGE;		

		// as DB_SET_RANGE finds the smallest key greater than or equal to the
		// specified key, in order to visit the database backwards, move to the
		// first matching elem, then move backwards
		if (cursor->get(&key, &value, flags) != 0)
		{
			// end of database
			cursor->close();
			if (db->cursor(NULL, &cursor, 0) != 0)
				return false;
		}
		else
		{
			// if there is a match, call the acceptor, otherwise move to the
			// previous elem in the database
			if (memcmp(tv.GetStartKey()->Buffer(), key.get_data(),
				MIN(tv.GetStartKey()->Length(), key.get_size())) == 0)
			{
				bsKey.SetLength(key.get_size());
				bsKey.SetBuffer((char*) key.get_data());
				
				bsValue.SetLength(value.get_size());
				bsValue.SetBuffer((char*) value.get_data());

				ret = tv.Accept(bsKey, bsValue);
			}
		}
	}
		
	flags = DB_PREV;
	while (ret && cursor->get(&key, &value, flags) == 0)
	{
		bsKey.SetLength(key.get_size());
		bsKey.SetBuffer((char*) key.get_data());
		
		bsValue.SetLength(value.get_size());
		bsValue.SetBuffer((char*) value.get_data());

		ret = tv.Accept(bsKey, bsValue);
		if (!ret)
			break;

		flags = DB_PREV;
	}
	
	cursor->close();	
	tv.OnComplete();
	
	return ret;

}
