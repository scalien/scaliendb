#ifndef SECTION_H
#define SECTION_H

#include "System/Buffers/Buffer.h"

class Table;
class Transaction;

class Section
{
public:
	void		Init(Table* table, const char *prefix);

	bool		Get(Transaction* tx, Buffer &key, Buffer &value);
	bool		Get(Transaction* tx, const char* key, Buffer &value);
	bool		Get(Transaction* tx, const char* key, unsigned klen, Buffer &value);
	bool		Get(Transaction* tx, const char* key, uint64_t &value);

	bool		Set(Transaction* tx, Buffer &key, Buffer &value);
	bool		Set(Transaction* tx, const char* key, Buffer &value);
	bool		Set(Transaction* tx, const char* key, const char* value);
	bool		Set(Transaction* tx, const char* key, unsigned klen,
				 const char* value, unsigned vlen);
	bool		Set(Transaction* tx, const char* key, uint64_t value);

	bool		Delete(Transaction* tx, Buffer &key);
	bool		Prune(Transaction* tx, Buffer &prefix);

private:
	Table*		table;
	Buffer		prefix;
};

#endif
