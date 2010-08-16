#include "Section.h"
#include "Table.h"
#include "Transaction.h"

void Section::Init(Table* table_, const char* prefix_)
{
	table = table_;
	prefix.Write(prefix_);
}

bool Section::Get(Transaction* tx, Buffer &key, Buffer &value)
{
	Buffer pkey;
	
	pkey.Append(prefix);
	pkey.Append(key);

	return table->Get(tx, pkey, value);
}

bool Section::Get(Transaction* tx, const char* key, Buffer &value)
{
	Buffer pkey;
	
	pkey.Append(prefix);
	pkey.Append(key);

	return table->Get(tx, pkey, value);
}

bool Section::Get(Transaction* tx, const char* key, unsigned klen, Buffer &value)
{
	Buffer pkey;
	
	pkey.Append(prefix);
	pkey.Append(key, klen);

	return table->Get(tx, pkey, value);
}

bool Section::Get(Transaction* tx, const char* key, uint64_t& value)
{
	Buffer pkey;
	
	pkey.Append(prefix);
	pkey.Append(key);
	pkey.NullTerminate();

	return table->Get(tx, pkey.GetBuffer(), value);
}

bool Section::Set(Transaction* tx, Buffer &key, Buffer &value)
{
	Buffer pkey;
	
	pkey.Append(prefix);
	pkey.Append(key);

	return table->Set(tx, pkey, value);
}

bool Section::Set(Transaction* tx, const char* key, Buffer &value)
{
	Buffer pkey;
	
	pkey.Append(prefix);
	pkey.Append(key);

	return table->Set(tx, pkey, value);
}

bool Section::Set(Transaction* tx, const char* key, const char* value)
{
	Buffer pkey;
	
	pkey.Append(prefix);
	pkey.Append(key);

	return table->Set(tx, pkey.GetBuffer(), pkey.GetLength(), value, strlen(value));
}

bool Section::Set(Transaction* tx, const char* key, unsigned klen, const char* value, unsigned vlen)
{
	Buffer pkey;
	Buffer buffer;
	
	pkey.Append(prefix);
	pkey.Append(key, klen);

	buffer.Append(value, vlen);
	
	return table->Set(tx, pkey, buffer);
}

bool Section::Set(Transaction* tx, const char* key, uint64_t value)
{
	Buffer pkey;
	Buffer buffer;
	
	buffer.Writef("%U", value);
	pkey.Append(prefix);
	pkey.Append(key);

	return table->Set(tx, pkey, buffer);
}

bool Section::Delete(Transaction* tx, Buffer &key)
{
	Buffer pkey;
	
	pkey.Append(prefix);
	pkey.Append(key);

	return table->Delete(tx, pkey);
}

bool Section::Prune(Transaction* tx, Buffer &prefix_)
{
	Buffer pkey;
	
	pkey.Append(prefix);
	pkey.Append(prefix_);

	return table->Prune(tx, pkey);
}
