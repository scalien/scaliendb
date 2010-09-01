#include "Test.h"

#include "System/Containers/InTreeMap.h"
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

static ReadBuffer& Key(KeyValue* kv)
{
	return kv->key;
}

TEST_DEFINE(TestInTreeMap)
{
	InTreeMap<KeyValue>						kvs;
	ReadBuffer								rb;
	ReadBuffer								rk;
	ReadBuffer								rv;
	Buffer									buf;
	KeyValue*								kv;
	KeyValue*								it;
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
	kvarea = (char*) malloc(num * sizeof(KeyValue));
	
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
//		kv = new KeyValue;
//		kv->SetKey(rb, true);
//		kv->SetValue(rb, true);

		kv = (KeyValue*) (kvarea + u * sizeof(KeyValue));
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


//	sw.Reset();
//	sw.Start();
//	//for (unsigned u = 0; u < num; u++)
//	for (unsigned u = num - 1; u < num; u--)
//	{
//		buf.Writef("%u", u);
//		rb.Wrap(buf);
//		kv = kvs.Remove<ReadBuffer&>(rb);
//		if (kv == NULL)
//			TEST_FAIL();
//	}
//	sw.Stop();		
//	printf("delete time: %ld\n", sw.Elapsed());

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
			kv = (KeyValue*) (kvarea + u * sizeof(KeyValue));
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
	printf("found: %u\n", u);
	printf("iteration time: %ld\n", sw.Elapsed());

	return 0;
}

TEST_MAIN(TestInTreeMap);
