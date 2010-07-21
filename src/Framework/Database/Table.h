#ifndef TABLE_H
#define TABLE_H

#include "Database.h"
#include "Cursor.h"
#include "System/Buffers/ReadBuffer.h"

/*
===============================================================================

 TableVisitor interface

===============================================================================
*/

class TableVisitor
{
public:
	virtual	~TableVisitor() {}
	virtual bool Accept(const ReadBuffer &key, const ReadBuffer &value) = 0;
	virtual const ReadBuffer* GetStartKey() { return 0; }
	virtual void OnComplete() {}
	virtual bool IsForward() { return true; }
};

/*
===============================================================================

 Table 

===============================================================================
*/

class Table
{
	friend class Transaction;
	
public:
	Table(Database* database, const char *name, int pageSize = 0);
	~Table();
	
	bool		Iterate(Transaction* tx, Cursor& cursor);
	
	bool		Get(Transaction* tx, const Buffer &key, Buffer &value);
	bool		Get(Transaction* tx, const char* key, Buffer &value);
	bool		Get(Transaction* tx, const char* key, unsigned klen, Buffer &value);
	bool		Get(Transaction* tx, const char* key, uint64_t &value);

	bool		Set(Transaction* tx, const Buffer &key, const Buffer &value);
	bool		Set(Transaction* tx, const char* key, const Buffer &value);
	bool		Set(Transaction* tx, const char* key, const char* value);
	bool		Set(Transaction* tx, const char* key, unsigned klen,
				 const char* value, unsigned vlen);
		
	bool		Delete(Transaction* tx, const Buffer &key);
	bool		Prune(Transaction* tx, const Buffer &prefix);
	bool		Truncate(Transaction* tx = NULL);
	
	bool		Visit(TableVisitor &tv);
	
private:
	Database*	database;
	Db*			db;
	
	bool		VisitBackward(TableVisitor &tv);
};


#endif
