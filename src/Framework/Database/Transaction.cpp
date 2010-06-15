#include "Transaction.h"

Transaction::Transaction()
{
	active = false;
	database = NULL;
}

Transaction::Transaction(Database* database_)
{
	active = false;
	database = database_;
}

Transaction::Transaction(Table* table)
{
	active = false;
	database = table->database;
}

void Transaction::Set(Database* database_)
{
	database = database_;
}

void Transaction::Set(Table* table)
{
	database = table->database;
}

bool Transaction::IsActive()
{
	return active;
}

bool Transaction::Begin()
{
	Log_Trace();
	
	if (database->env->txn_begin(NULL, &txn, 0) != 0)
		return false;
	
	active = true;
	return true;
}

bool Transaction::Commit()
{
	Log_Trace();
	
	active = false;
	
	if (txn->commit(0) != 0)
		return false;
	
	return true;
}

bool Transaction::Rollback()
{
	Log_Trace();
	
	active = false;
	
	if (txn->abort() != 0)
		return false;
	
	return true;
}
