#ifndef TRANSACTION_H
#define TRANSACTION_H

#include "Database.h"
#include "Table.h"

/*
===============================================================================

 Transaction

===============================================================================
*/

class Transaction
{
	friend class Table;
	
public:
	Transaction();
	Transaction(Database* database);
	Transaction(Table* table);
	
	void		Set(Database* database);	
	void		Set(Table* table);
	bool		IsActive();
	bool		Begin();
	bool		Commit();
	bool		Rollback();
	
private:
	Database*	database;
	DbTxn*		txn;
	bool		active;
};

#endif
