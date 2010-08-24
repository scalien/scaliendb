#ifndef CATALOG_H
#define CATALOG_H

class ReadBuffer;

class Catalog
{
public:
	void					Open(char* filepath);
	void					Flush();
	void					Close();
	
	bool					Get(ReadBuffer& key, ReadBuffer& value);
	bool					Set(ReadBuffer& key, ReadBuffer& value, bool copy = true);
	void					Delete(ReadBuffer& key);	
};

#endif
