#include "File.h"

File::File()
{
	uint32_t i;
	
	indexPageSize = DEFAULT_INDEXPAGE_SIZE;
	dataPageSize = DEFAULT_DATAPAGE_SIZE;
	numDataPageSlots = DEFAULT_NUM_DATAPAGES;

	mustSplit = false;
	
	indexPage.SetPageSize(indexPageSize);
	indexPage.SetNumDataPageSlots(numDataPageSlots);

	dataPages = (DataPage**) malloc(sizeof(DataPage*) * numDataPageSlots);
	for (i = 0; i < numDataPageSlots; i++)
		dataPages[i] = NULL;
	numDataPages = 0;
}

//bool File::Create(char* filepath,
// uint32_t indexPageSize, uint32_t dataPageSize, uint32_t numDataPages)
//{
//}
//
//void File::Open(char* filepath)
//{
//	// read indexPage
//}
//
//void File::Write()
//{
//	// for each dirty, write
//}
//
//void File::Flush()
//{
//	// sync
//}

bool File::Get(ReadBuffer& key, ReadBuffer& value)
{
	int32_t index;
	
	index = Locate(key);
	
	if (index < 0)
		return false;
	
	return dataPages[index]->Get(key, value);
}

bool File::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
	int32_t		index, newIndex;
	DataPage*	newPage;
	
	if (key.GetLength() + value.GetLength() > DATAPAGE_MAX_KV_SIZE(dataPageSize))
			return false;
	
	index = Locate(key);
	
	if (index < 0)
	{
		index = 0;
		if (dataPages[index] == NULL)
			LoadDataPage(index);

		indexPage.Remove(index);
		indexPage.Add(key, index, false); // TODO
		MarkPageDirty(&indexPage);
	}
	
	dataPages[index]->Set(key, value, copy);
	MarkPageDirty(dataPages[index]);
	
	if (dataPages[index]->MustSplit())
	{
		if (numDataPages < numDataPageSlots)
		{
			newPage = dataPages[index]->Split();
			numDataPages++;
			newIndex = indexPage.NextFreeDataPage();
			assert(dataPages[newIndex] == NULL);
			dataPages[newIndex] = newPage;
			indexPage.Add(newPage->FirstKey(), newIndex, false); // TODO
//			if (newPage->MustSplit())
//			{
//				if (numDataPages < numDataPageSlots)
//				{
//					newPage = newPage->Split();
//					assert(newPage->MustSplit() == false);
// TOOD
//					newIndex = indexPage.NextFreeDataPage();
//					indexPage.Add(newPage->FirstKey(), newIndex);
//				}
//				else
//					mustSplit = true;
//			}
		}
		else
			mustSplit = true;
	}
	
	return true;
}

void File::Delete(ReadBuffer& key)
{
	int32_t index;
	
	index = Locate(key);
	
	if (index < 0)
		return;
	
	dataPages[index]->Delete(key);
	MarkPageDirty(dataPages[index]);

	if (dataPages[index]->IsEmpty())
	{
		indexPage.Remove(index);
		MarkPageDirty(&indexPage);
	}
}

//bool File::IsEmpty()
//{
//}
//
//bool File::MustSplit()
//{
//	return mustSplit || indexPage.MustSplit();
//}

int32_t File::Locate(ReadBuffer& key)
{
	int32_t	index;
	
	index = indexPage.Locate(key);
	
	if (index < 0)
		return index; // not in file
	
	if (dataPages[index] == NULL)
		LoadDataPage(index);
	
	return index;
}

void File::LoadDataPage(uint32_t index)
{
	dataPages[index] = new DataPage;
	dataPages[index]->SetPageSize(dataPageSize);
	numDataPages++;
}

void File::MarkPageDirty(Page* page)
{
	if (!page->dirty)
	{
		page->dirty = true;
		dirtyPages.Append(page);
	}
}
