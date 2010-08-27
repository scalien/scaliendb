#include "Test.h"

#include "Framework/Storage/StorageCatalog.h"

int TestStorage()
{
#define PRINT()			{v.Write(rv); v.NullTerminate(); k.NullTerminate(); printf("%s => %s\n", k.GetBuffer(), v.GetBuffer());}
	StorageCatalog	catalog;
	Buffer			k, v;
	ReadBuffer		rk, rv;
	Stopwatch		sw;
	long			elapsed;
	unsigned		num, len, ksize, vsize;
	char*			area;
	char*			p;
	uint64_t		clock;
	
//	return TestTreeMap();
//	return TestClock();
	
//	W(k, rk, "ki");
//	W(v, rv, "atka");
//	file.Set(rk, rv);
//
//	W(k, rk, "hol");
//	W(v, rv, "budapest");
//	file.Set(rk, rv);
//
//	W(k, rk, "ki");
//	W(v, rv, "atka");
//	file.Set(rk, rv);
//
//	W(k, rk, "hol");
//	W(v, rv, "budapest");
//	file.Set(rk, rv);
//
//	W(k, rk, "ki");
//	file.Get(rk, rv);
//	P();
//
//	W(k, rk, "hol");
//	file.Get(rk, rv);
//	P();

	StartClock();

	num = 1*1000*1000;
	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));

	catalog.Open("dogs");

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
		catalog.Set(rk, rv, false);

		if (NowClock() - clock >= 1000)
		{
			printf("syncing...\n");
			catalog.Flush();
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
		if (catalog.Get(rk, rv))
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
	catalog.Close();
	elapsed = sw.Stop();
	printf("Close() took %ld msec\n", elapsed);
	
	free(area);
	
	return TEST_SUCCESS;
}

int TestStorageCapacity()
{
	StorageCatalog	catalog;
	Buffer			k, v;
	ReadBuffer		rk, rv;
	Stopwatch		sw;
	long			elapsed;
	unsigned		num, len, ksize, vsize;
	char*			area;
	char*			p;
	unsigned		round;

	round = 10;
	num = 100*1000;
	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));

	// a million key-value pairs take up 248M disk space
	for (unsigned r = 0; r < round; r++)
	{
		catalog.Open("dogs");

		sw.Start();
		for (unsigned i = 0; i < num; i++)
		{
			p = area + i*(ksize+vsize);
			len = snprintf(p, ksize, "%d", i + r * num);
			rk.SetBuffer(p);
			rk.SetLength(len);
			p += ksize;
			len = snprintf(p, vsize, "%.100f", (float) i + r * num);
			rv.SetBuffer(p);
			rv.SetLength(len);
			catalog.Set(rk, rv, false);
		}
		elapsed = sw.Stop();
		printf("%u sets took %ld msec\n", num, elapsed);		

		sw.Restart();
		for (unsigned i = 0; i < num; i++)
		{
			k.Writef("%d", i + r * num);
			rk.Wrap(k);
			if (catalog.Get(rk, rv))
				;//PRINT()
			else
				ASSERT_FAIL();
		}	
		elapsed = sw.Stop();
		printf("Round %u: %u gets took %ld msec\n", r, num, elapsed);

		sw.Reset();
		sw.Start();
		catalog.Close();
		elapsed = sw.Stop();
		printf("Close() took %ld msec\n", elapsed);
	}
	
	free(area);

	return TEST_SUCCESS;
}

