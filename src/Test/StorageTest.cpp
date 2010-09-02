#include "Test.h"

#include "Framework/Storage/StorageDatabase.h"
#include "System/Stopwatch.h"

TEST_DEFINE(TestStorage)
{
#define PRINT()			{v.Write(rv); v.NullTerminate(); k.NullTerminate(); printf("%s => %s\n", k.GetBuffer(), v.GetBuffer());}
	StorageDatabase		db;
	StorageTable*		table;
	Buffer				k, v;
	ReadBuffer			rk, rv;
	Stopwatch			sw;
	long				elapsed;
	unsigned			num, len, ksize, vsize;
	char*				area;
	char*				p;
	uint64_t			clock;
	
	StartClock();

	num = 1*1000*1000;
	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));

	db.Open("test");
	table = db.GetTable("dogs");

	clock = NowClock();
	sw.Start();
	for (unsigned i = 0; i < num; i++)
	{
		p = area + i*(ksize+vsize);
		len = snprintf(p, ksize, "%d", i);
		rk.SetBuffer(p);
		rk.SetLength(len);
		p += ksize;
		len = snprintf(p, vsize, "%.100f", (float) i);
		rv.SetBuffer(p);
		rv.SetLength(len);
		table->Set(rk, rv, false);

		if (NowClock() - clock >= 1000)
		{
			printf("syncing...\n");
			db.Commit();
			clock = NowClock();
		}
	}
	elapsed = sw.Stop();
	printf("%u sets took %ld msec\n", num, elapsed);

	sw.Reset();
	sw.Start();
	for (unsigned i = 0; i < num; i++)
	{
		k.Writef("%d", i);
		rk.Wrap(k);
		if (table->Get(rk, rv))
			;//PRINT()
		else
			ASSERT_FAIL();
	}	
	elapsed = sw.Stop();
	printf("%u gets took %ld msec\n", num, elapsed);

//	sw.Reset();
//	sw.Start();
//	for (int i = 0; i < num; i++)
//	{
//		k.Writef("%d", i);
//		rk.Wrap(k);
//		catalog.Delete(rk); 
//	}	
//	elapsed = sw.Stop();
//	printf("%u deletes took %ld msec\n", num, elapsed);
	
	sw.Reset();
	sw.Start();
	db.Close();
	elapsed = sw.Stop();
	printf("Close() took %ld msec\n", elapsed);
	
	free(area);
	
	return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageCapacity)
{
	StorageDatabase		db;
	StorageTable*		table;
	Buffer				k, v;
	ReadBuffer			rk, rv;
	Stopwatch			sw;
	long				elapsed;
	unsigned			num, len, ksize, vsize;
	char*				area;
	char*				p;
	unsigned			round;

	round = 10;
	num = 100*1000;
	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));

	db.Open("test");
	// a million key-value pairs take up 248M disk space
	for (unsigned r = 0; r < round; r++)
	{
		table = db.GetTable("dogs");

		sw.Reset();
		for (unsigned i = 0; i < num; i++)
		{
			p = area + i*(ksize+vsize);
			len = snprintf(p, ksize, "%d", i + r * num); // takes 100 ms
			rk.SetBuffer(p);
			rk.SetLength(len);
			//printf("%s\n", p);
			p += ksize;
			len = snprintf(p, vsize, "%.100f", (float) i + r * num); // takes 100 ms
			rv.SetBuffer(p);
			rv.SetLength(len);
			sw.Start();
			table->Set(rk, rv, false);
			sw.Stop();
		}
//		printf("reads took %ld msec\n", StorageFile::sw_reads.Elapsed());
//		printf("test took %ld msec\n", StorageFile::sw_test.Elapsed());
		
		printf("%u sets took %ld msec\n", num, sw.Elapsed());

//		sw.Restart();
//		for (unsigned i = 0; i < num; i++)
//		{
//			k.Writef("%d", i + r * num);
//			rk.Wrap(k);
//			if (table->Get(rk, rv))
//				;//PRINT()
//			else
//				ASSERT_FAIL();
//		}	
//		elapsed = sw.Stop();
//		printf("Round %u: %u gets took %ld msec\n", r, num, elapsed);

		sw.Reset();
		sw.Start();
		table->Commit(true, false);
		elapsed = sw.Stop();
		printf("Commit() took %ld msec\n", elapsed);
	}
	db.Close();
	
	free(area);

	return TEST_SUCCESS;
}

TEST_MAIN(TestStorage, TestStorageCapacity);
