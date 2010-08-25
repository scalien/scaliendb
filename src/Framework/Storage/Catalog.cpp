#include "Catalog.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

void Catalog::Open(char* filepath_)
{
	struct stat st;

	maxFileIndex = 0;

	filepath.Write(filepath_);
	filepath.NullTerminate();

	fd = open(filepath.GetBuffer(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	if (fd < 0)
		ASSERT_FAIL();

	fstat(fd, &st);
	if (st.st_size > 0)
		Read(st.st_size);
}

void Catalog::Flush()
{
}

void Catalog::Close()
{
	Write();
	
	close(fd);
}

bool Catalog::Get(ReadBuffer& key, ReadBuffer& value)
{
	FileIndex* fi;
	
	fi = Locate(key);

	if (fi == NULL)
		return false;
	
	else return fi->file.Get(key, value);
}

bool Catalog::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
	FileIndex*	fi;
	Buffer		tmp;
	
	fi = Locate(key);

	if (fi == NULL)
	{
		fi = new FileIndex;
		fi->index = maxFileIndex++;
		fi->SetKey(key, true); // TODO: buffer management
		tmp.Write(filepath);
		tmp.SetLength(filepath.GetLength() - 1); // get rid of trailing \0
		tmp.Appendf(".%u", fi->index);
		tmp.NullTerminate();
		fi->file.Open(tmp.GetBuffer());
		files.Add(fi);
	}
	
	if (!fi->file.Set(key, value, copy))
		return false;
		
	// TODO: file splitting
	
	return true;
}

void Catalog::Delete(ReadBuffer& key)
{
}

void Catalog::Read(uint32_t length)
{
	uint32_t	i, numFiles;
	unsigned	len;
	char*		p;
	FileIndex*	fi;
	
	buffer.Allocate(length);
	if (read(fd, (void*) buffer.GetBuffer(), length) < 0)
		ASSERT_FAIL();
	p = buffer.GetBuffer();
	numFiles = FromLittle32(*((uint32_t*) p));
	p += 4;
	for (i = 0; i < numFiles; i++)
	{
		fi = new FileIndex;
		fi->index = FromLittle32(*((uint32_t*) p));
		p += 4;
		len = FromLittle32(*((uint32_t*) p));
		p += 4;
		fi->key.SetLength(len);
		fi->key.SetBuffer(p);
		p += len;
		files.Add(fi);

		fi->filepath.Write(filepath);
		fi->filepath.SetLength(filepath.GetLength() - 1); // get rid of trailing \0
		fi->filepath.Appendf(".%u", fi->index);
		fi->filepath.NullTerminate();
		
		if (fi->index > maxFileIndex)
			maxFileIndex = fi->index;
	}	
}

void Catalog::Write()
{
	char*			p;
	uint32_t		size, len;
	FileIndex		*it;
	
	lseek(fd, 0, SEEK_SET);
	ftruncate(fd, 0);
	
	buffer.Allocate(4);
	p = buffer.GetBuffer();
	len = files.GetLength();
	*((uint32_t*) p) = ToLittle32(len);
	p += 4;
	
	if (write(fd, (const void *) buffer.GetBuffer(), 4) < 0)
		ASSERT_FAIL();

	for (it = files.Head(); it != NULL; it = files.Next(it))
	{
		if (!it->file.IsOpen())
			continue;
		
		size = 8 + it->key.GetLength();
		buffer.Allocate(size);
		p = buffer.GetBuffer();
		*((uint32_t*) p) = ToLittle32(it->index);
		p += 4;
		len = it->key.GetLength();
		*((uint32_t*) p) = ToLittle32(len);
		p += 4;
		memcpy(p, it->key.GetBuffer(), len);
		p += len;
		if (write(fd, (const void *) buffer.GetBuffer(), size) < 0)
			ASSERT_FAIL();
	}

	for (it = files.Head(); it != NULL; it = files.Next(it))
		it->file.Write();
}

FileIndex* Catalog::Locate(ReadBuffer& key)
{
	FileIndex* fi;
	
	if (files.GetLength() == 0)
		return NULL;
		
	if (LessThan(key, files.Head()->key))
	{
		fi = files.Head();
		goto OpenFile;
	}
	
	for (fi = files.Head(); fi != NULL; fi = files.Next(fi))
	{
		if (LessThan(key, fi->key))
		{
			fi = files.Prev(fi);
			goto OpenFile;
		}
	}
	
	fi = files.Tail();
	
	OpenFile:
	if (!fi->file.IsOpen())
		fi->file.Open(fi->filepath.GetBuffer());
	
	return fi;
}

FileIndex::FileIndex()
{
	keyBuffer = NULL;
	prev = this;
	next = this;
}

FileIndex::~FileIndex()
{
	if (keyBuffer != NULL)
		delete keyBuffer;
}

void FileIndex::SetKey(ReadBuffer& key_, bool copy)
{
	if (keyBuffer != NULL && !copy)
		delete keyBuffer;
	
	if (copy)
	{
		if (keyBuffer == NULL)
			keyBuffer = new Buffer; // TODO: allocation strategy
		keyBuffer->Write(key_);
		key.Wrap(*keyBuffer);
	}
	else
		key = key_;
}

