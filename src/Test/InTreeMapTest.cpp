#include "Test.h"

#include "System/Containers/InTreeMap.h"
#include "System/Stopwatch.h"
#include "Framework/Storage/StorageDataPage.h"
#include <map>

static inline int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
	return ReadBuffer::Cmp(a, b);
}

//static bool operator< (const ReadBuffer& left, const ReadBuffer& right)
//{
//	return ReadBuffer::LessThan(left, right);
//}

static const ReadBuffer& Key(StorageKeyValue* kv)
{
	return kv->key;
}

TEST_DEFINE(TestInTreeMap)
{
	InTreeMap<StorageKeyValue>				kvs;
	ReadBuffer								rb;
	ReadBuffer								rk;
	ReadBuffer								rv;
	Buffer									buf;
	StorageKeyValue*						kv;
	StorageKeyValue*						it;
	Stopwatch								sw;
	const unsigned							num = 1000000;
	unsigned								ksize;
	unsigned								vsize;
	char*									area;
	char*									kvarea;
	char*									p;
	int										len;
	int										cmpres;
	std::map<ReadBuffer, ReadBuffer>		bufmap;

	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));
	kvarea = (char*) malloc(num * sizeof(StorageKeyValue));
	
	//sw.Start();
	for (unsigned u = 0; u < num; u++)
	{
		p = area + u*(ksize+vsize);
		len = snprintf(p, ksize, "%u", u);
		rk.SetBuffer(p);
		rk.SetLength(len);
		p += ksize;
		len = snprintf(p, vsize, "%.100f", (float) u);
		rv.SetBuffer(p);
		rv.SetLength(len);

//		buf.Writef("%u", u);
//		rb.Wrap(buf);
//		kv = new StorageKeyValue;
//		kv->SetKey(rb, true);
//		kv->SetValue(rb, true);

		kv = (StorageKeyValue*) (kvarea + u * sizeof(StorageKeyValue));
		kv->SetKey(rk, false);
		kv->SetValue(rv, false);
		
		sw.Start();
		kvs.Insert(kv);
		//bufmap.insert(std::pair<ReadBuffer, ReadBuffer>(rk, rv));
		sw.Stop();
	}
	printf("insert time: %ld\n", sw.Elapsed());

	//return 0;

	sw.Reset();
	sw.Start();
	for (unsigned u = 0; u < num; u++)
	{
		buf.Writef("%u", u);
		rb.Wrap(buf);
		kv = kvs.Get<ReadBuffer&>(rb);
		if (kv == NULL)
			TEST_FAIL();
	}
	sw.Stop();		
	printf("get time: %ld\n", sw.Elapsed());

	sw.Reset();
	sw.Start();
	for (it = kvs.First(); it != NULL; it = kvs.Next(it))
	{
		//printf("it->value = %.*s\n", P(&it->value));
		kv = it; // dummy op
	}
	sw.Stop();		
	printf("iteration time: %ld\n", sw.Elapsed());


	sw.Reset();
	sw.Start();
	//for (unsigned u = 0; u < num; u++)
	for (unsigned u = num - 1; u < num; u--)
	{
		buf.Writef("%u", u);
		rb.Wrap(buf);
		kv = kvs.Get<ReadBuffer&>(rb);
		if (kv == NULL)
			TEST_FAIL();
		kvs.Remove(kv);
	}
	sw.Stop();		
	printf("delete time: %ld\n", sw.Elapsed());

	sw.Reset();
	sw.Start();
	for (unsigned u = 0; u < num; u++)
	{
		buf.Writef("%u", u);
		rb.Wrap(buf);
		kv = kvs.Get<ReadBuffer&>(rb);
		if (kv != NULL)
			TEST_FAIL();
	}
	sw.Stop();
	printf("get time: %ld\n", sw.Elapsed());

	sw.Reset();
	for (unsigned u = 0; u < num; u++)
	{
		p = area + u*(ksize+vsize);
		rk.SetBuffer(p);
		rk.SetLength(ksize);
		RandomBuffer(p, ksize);
		p += ksize;
		rv.SetBuffer(p);
		rv.SetLength(vsize);
		RandomBuffer(p, vsize);

		sw.Start();
		it = kvs.Locate<ReadBuffer&>(rk, cmpres);
		if (cmpres == 0 && it != NULL)
		{
			// found, overwrite value
			it->value = rv;
		}
		else
		{
			kv = (StorageKeyValue*) (kvarea + u * sizeof(StorageKeyValue));
			kv->SetKey(rk, false);
			kv->SetValue(rv, false);
			kvs.InsertAt(kv, it, cmpres);
		}
		sw.Stop();
	}
	printf("random insert time: %ld\n", sw.Elapsed());

	unsigned u = 0;
	sw.Reset();
	sw.Start();
	for (it = kvs.First(); it != NULL; it = kvs.Next(it))
	{
		//printf("it->key = %.*s\n", P(&it->key));
		//kv = it; // dummy op
		u++;
	}
	sw.Stop();
	printf("found: %u, count: %u\n", u, kvs.GetCount());
	printf("iteration time: %ld\n", sw.Elapsed());

	if (u != kvs.GetCount())
		return TEST_FAILURE;

	return 0;
}

TEST_DEFINE(TestInTreeMapInsert)
{
	InTreeMap<StorageKeyValue>		kvs;
	ReadBuffer						rb;
	ReadBuffer						rk;
	ReadBuffer						rv;
	Buffer							buf;
	StorageKeyValue*				kv;
	StorageKeyValue*				it;
	char*							p;
	char*							area;
	char*							kvarea;
	int								ksize;
	int								vsize;
	int								num = 2;
	
	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));
	kvarea = (char*) malloc(num * sizeof(StorageKeyValue));

	for (int i = 0; i < num; i++)
	{
		p = area + i*(ksize+vsize);
		rk.SetBuffer(p);
		rk.SetLength(ksize);
		snprintf(p, ksize, "key");
		p += ksize;
		rv.SetBuffer(p);
		rv.SetLength(vsize);
		snprintf(p, vsize, "value");

		kv = (StorageKeyValue*) (kvarea + i * sizeof(StorageKeyValue));
		kv->SetKey(rk, false);
		kv->SetValue(rv, false);
		it = kvs.Insert(kv);
		if (i == 0 && it != NULL)
			TEST_FAIL();
		if (i > 0 && it == NULL)
			TEST_FAIL();
	}

	free(area);
	free(kvarea);

	return TEST_SUCCESS;
}

TEST_DEFINE(TestInTreeMapInsertRandom)
{
	InTreeMap<StorageKeyValue>		kvs;
	ReadBuffer						rb;
	ReadBuffer						rk;
	ReadBuffer						rv;
	Buffer							buf;
	Stopwatch						sw;
	StorageKeyValue*				kv;
	StorageKeyValue*				it;
	char*							p;
	char*							area;
	char*							kvarea;
	int								ksize;
	int								vsize;
	int								num = 100000;
	
	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));
	kvarea = (char*) malloc(num * sizeof(StorageKeyValue));

	for (int i = 0; i < num; i++)
	{
		p = area + i*(ksize+vsize);
		rk.SetBuffer(p);
		rk.SetLength(ksize);
		RandomBuffer(p, ksize);
		p += ksize;
		rv.SetBuffer(p);
		rv.SetLength(vsize);
		RandomBuffer(p, vsize);

		kv = (StorageKeyValue*) (kvarea + i * sizeof(StorageKeyValue));
		kv->SetKey(rk, true);
		kv->SetValue(rv, true);
		sw.Start();
		it = kvs.Insert(kv);
		sw.Stop();
	}
	
	printf("random insert time: %ld\n", sw.Elapsed());

	free(area);
	free(kvarea);

	return TEST_SUCCESS;
}

TEST_DEFINE(TestInTreeMapRemoveRandom)
{
	InTreeMap<StorageKeyValue>		kvs;
	ReadBuffer						rb;
	ReadBuffer						rk;
	ReadBuffer						rv;
	Buffer							buf;
	Stopwatch						sw;
	StorageKeyValue*				kv;
	StorageKeyValue*				it;
	char*							p;
	char*							area;
	char*							kvarea;
	int								ksize;
	int								vsize;
	int								len;
	int								num = 10000;
	int								numbers[num];
	
	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));
	kvarea = (char*) malloc(num * sizeof(StorageKeyValue));

	for (int i = 0; i < num; i++)
	{
		p = area + i*(ksize+vsize);
		len = snprintf(p, ksize, "%010d", i);
		rk.SetBuffer(p);
		rk.SetLength(len);
		p += len;
		len = snprintf(p, vsize, "%.100f", (float) i);
		rv.SetBuffer(p);
		rv.SetLength(len);

		kv = (StorageKeyValue*) (kvarea + i * sizeof(StorageKeyValue));
		kv->SetKey(rk, true);
		kv->SetValue(rv, true);
		sw.Start();
		it = kvs.Insert(kv);
		sw.Stop();
	}
	
	printf("insert time: %ld\n", sw.Elapsed());

	// check if the elements are all in order
	int count = 0;
	for (it = kvs.First(); it != NULL; it = kvs.Next(it))
	{
		StorageKeyValue* next;
		
		count++;
		next = kvs.Next(it);
		if (next == NULL)
			break;

		if (ReadBuffer::Cmp(it->key, next->key) > 0)
			TEST_FAIL();
	}
	if (count != num)
		TEST_FAIL();

	// shuffle numbers
	for (int i = 0; i < num; i++)
		numbers[i] = i;
	for (int i = 0; i < num; i++)
	{
		int rndpos = RandomInt(0, num - 1);
		int tmp = numbers[i];
		numbers[i] = numbers[rndpos];
		numbers[rndpos] = tmp;
	}

	int max = num - 1;
	for (int i = 0; i < num; i++)
	{
		int j = numbers[i];
		kv = (StorageKeyValue*) (kvarea + j * sizeof(StorageKeyValue));
		it = kvs.Remove(kv);
		if (j == num - 1)
		{
			if (it != NULL)
				TEST_FAIL();
		}
		else
		{
			if (it && ReadBuffer::Cmp(it->key, kv->key) <= 0)
				TEST_FAIL();
			if (!it)
			{
				if (j > max)
					TEST_FAIL();
				max = j;
			}
		}
	}
	
	if (kvs.GetCount() != 0)
		TEST_FAIL();

	free(area);
	free(kvarea);

	return TEST_SUCCESS;
}

TEST_MAIN(TestInTreeMap);
