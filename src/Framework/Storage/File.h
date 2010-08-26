#ifndef FILE_H
#define FILE_H

#include "IndexPage.h"
#include "DataPage.h"

#define DEFAULT_KEY_LIMIT			1000
#define DEFAULT_DATAPAGE_SIZE		64*1024
#define DEFAULT_NUM_DATAPAGES		256			// 16.7 MB wort of data pages
#define DEFAULT_INDEXPAGE_SIZE		256*1024	// to fit 256 keys

#define INDEXPAGE_OFFSET			12

// default total filesize: 4+4+4+262144+256*65536 ~= 17M

/*
===============================================================================

 File

===============================================================================
*/

class File
{
public:
	File();
	~File();
	
	void					Open(char* filepath);
	void					Flush();
	void					Close();
	
	bool					Get(ReadBuffer& key, ReadBuffer& value);
	bool					Set(ReadBuffer& key, ReadBuffer& value, bool copy = true);
	void					Delete(ReadBuffer& key);

	ReadBuffer				FirstKey();

	bool					IsOverflowing();
	File*					SplitFile();

	void					Read();
	void					ReadRest();
	void					Write();

	File*					prev;
	File*					next;
	
private:
	int32_t					Locate(ReadBuffer& key);
	void					LoadDataPage(uint32_t index);
	void					MarkPageDirty(Page* page);
	void					SplitDataPage(uint32_t index);
	void					ReorderPages();
	void					ReorderFile();
	
	int						fd;
	uint32_t				indexPageSize;
	uint32_t				dataPageSize;
	uint32_t				numDataPageSlots;
	bool					isOverflowing;
	bool					newFile;
	Buffer					filepath;
	IndexPage				indexPage;
	DataPage**				dataPages;
	uint32_t				numDataPages;
	InList<Page>			dirtyPages;
};

#endif
