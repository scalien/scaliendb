#ifndef FILE_H
#define FILE_H

#include "IndexPage.h"
#include "DataPage.h"

#define DEFAULT_KEY_LIMIT			1000
#define DEFAULT_DATAPAGE_SIZE		65536
#define DEFAULT_NUM_DATAPAGES		256		// 16.7 MB wort of data pages
#define DEFAULT_INDEXPAGE_SIZE		262144  // to fit 256 keys

// default total filesize: 4+4+4+262144+256*65536 = 17M

/*
===============================================================================

 File

===============================================================================
*/

class File
{
public:
	File();
	
	bool					Create(char* filepath,
							 uint32_t indexPageSize,
							 uint32_t dataPageSize,
							 uint32_t numDataPages);

//	void					Open(char* filepath);
//	void					Write();
//	void					Flush();
	
	bool					Get(ReadBuffer& key, ReadBuffer& value);
	bool					Set(ReadBuffer& key, ReadBuffer& value, bool copy = true);
	void					Delete(ReadBuffer& key);

//	bool					IsEmpty();
//	bool					MustSplit();
	
private:
	int32_t					Locate(ReadBuffer& key);
	void					LoadDataPage(uint32_t index);
	void					MarkPageDirty(Page* page);
	
	uint32_t				indexPageSize;
	uint32_t				dataPageSize;
	uint32_t				numDataPageSlots;
	bool					mustSplit;
	
	IndexPage				indexPage;
	DataPage**				dataPages;
	uint32_t				numDataPages;
	InList<Page>			dirtyPages;
};

#endif
