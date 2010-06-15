#ifndef TABLE_H
#define TABLE_H

#include "Database.h"
#include "Cursor.h"
#include "System/Buffers/ByteWrap.h"

class TableVisitor
{
public:
	virtual	~TableVisitor() {}
	virtual bool Accept(const ByteString &key, const ByteString &value) = 0;
	virtual const ByteString* GetStartKey() { return 0; }
	virtual void OnComplete() {}
	virtual bool IsForward() { return true; }
};

class Table
{
	friend class Transaction;
	
public:
	Table(Database* database, const char *name, int pageSize = 0);
	~Table();
	
	bool		Iterate(Transaction* tx, Cursor& cursor);
	
	bool		Get(Transaction* tx, const ByteString &key, ByteWrap &value);
	bool		Get(Transaction* tx, const char* key, ByteWrap &value);
	bool		Get(Transaction* tx, const char* key, uint64_t &value);
	
	bool		Set(Transaction* tx, const ByteString &key, const ByteString &value);
	bool		Set(Transaction* tx, const char* key, const ByteString &value);
	bool		Set(Transaction* tx, const char* key, const char* value);
	
	bool		Delete(Transaction* tx, const ByteString &key);
	bool		Prune(Transaction* tx, const ByteString &prefix);
	bool		Truncate(Transaction* tx = NULL);
	
	bool		Visit(TableVisitor &tv);
	
private:
	Database*	database;
	Db*			db;
	
	bool		VisitBackward(TableVisitor &tv);
};


#endif
