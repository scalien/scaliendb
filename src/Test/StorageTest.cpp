#include "Test.h"

#include "Framework/Storage/StorageEnvironment.h"
#include "Framework/Storage/StorageDataPage.h"
#include "Framework/Storage/StorageDataCache.h"
#include "System/Stopwatch.h"
#include "System/Common.h"

TEST_DEFINE(TestStorage)
{
#define PRINT()         {v.Write(rv); v.NullTerminate(); k.NullTerminate(); printf("%s => %s\n", k.GetBuffer(), v.GetBuffer());}
    StorageDatabase     db;
    StorageTable*       table;
    Buffer              k, v;
    ReadBuffer          rk, rv;
    Stopwatch           sw;
    long                elapsed;
    unsigned            num, len, ksize, vsize;
    char*               area;
    char*               p;
    uint64_t            clock;
    
    StartClock();

    num = 1*1000*1000;
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init(100000000);

    db.Open(".", "db");
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
            TEST_LOG("syncing...");
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

//  sw.Reset();
//  sw.Start();
//  for (int i = 0; i < num; i++)
//  {
//      k.Writef("%d", i);
//      rk.Wrap(k);
//      catalog.Delete(rk); 
//  }   
//  elapsed = sw.Stop();
//  printf("%u deletes took %ld msec\n", num, elapsed);
    
    sw.Reset();
    sw.Start();
    db.Close();
    elapsed = sw.Stop();
    printf("Close() took %ld msec\n", elapsed);
    
    DCACHE->Shutdown();
    
    free(area);
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageCapacity)
{
    StorageDatabase     db;
    StorageTable*       table;
    Buffer              k, v;
    ReadBuffer          rk, rv;
    Stopwatch           sw;
    long                elapsed;
    unsigned            num, len, ksize, vsize;
    char*               area;
    char*               p;
    unsigned            round;

    round = 10;
    num = 100*1000;
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init(10000000);

    db.Open(".", "db");
    // a million key-value pairs take up 248M disk space
    for (unsigned r = 0; r < round; r++)
    {
        table = db.GetTable("dogs");

        sw.Reset();
        for (unsigned i = 0; i < num; i++)
        {
            p = area + i*(ksize+vsize);
            len = snprintf(p, ksize, "%011d", i + r * num); // takes 100 ms
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
//      printf("reads took %ld msec\n", StorageFile::sw_reads.Elapsed());
//      printf("test took %ld msec\n", StorageFile::sw_test.Elapsed());
        
        printf("%u sets took %ld msec\n", num, sw.Elapsed());

//      sw.Restart();
//      for (unsigned i = 0; i < num; i++)
//      {
//          k.Writef("%d", i + r * num);
//          rk.Wrap(k);
//          if (table->Get(rk, rv))
//              ;//PRINT()
//          else
//              ASSERT_FAIL();
//      }   
//      elapsed = sw.Stop();
//      printf("Round %u: %u gets took %ld msec\n", r, num, elapsed);

        sw.Reset();
        sw.Start();
        table->Commit(true /*recovery*/, false /*flush*/);
        elapsed = sw.Stop();
        printf("Commit() took %ld msec\n", elapsed);
    }
    db.Close();

    DCACHE->Shutdown();
    
    free(area);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageBigTransaction)
{
    StorageDatabase     db;
    StorageTable*       table;
    Buffer              k, v;
    ReadBuffer          rk, rv;
    Stopwatch           sw;
    long                elapsed;
    unsigned            num, len, ksize, vsize;
    char*               area;
    char*               p;

    num = 1000*1000;
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init(10000000);

    db.Open(".", "db");
    table = db.GetTable("dogs");
    sw.Reset();
    for (unsigned i = 0; i < num; i++)
    {
        p = area + i*(ksize+vsize);
        len = snprintf(p, ksize, "%011d", i); // takes 100 ms
        rk.SetBuffer(p);
        rk.SetLength(len);
        p += ksize;
        len = snprintf(p, vsize, "%.100f", (float) i); // takes 100 ms
        rv.SetBuffer(p);
        rv.SetLength(len);
        sw.Start();
        table->Set(rk, rv, false);
        sw.Stop();
    }
    printf("%u sets took %ld msec\n", num, sw.Elapsed());
    sw.Restart();
    table->Commit(true /*recovery*/, false /*flush*/);
    elapsed = sw.Stop();
    printf("Commit() took %ld msec\n", elapsed);

    db.Close();

    DCACHE->Shutdown();
    
    free(area);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageBigRandomTransaction)
{
    StorageDatabase     db;
    StorageTable*       table;
    Buffer              k, v;
    ReadBuffer          rk, rv;
    Stopwatch           sw;
    long                elapsed;
    unsigned            num, ksize, vsize;
    char*               area;
    char*               p;

    num = 1000*1000;
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init(10000000);

    db.Open(".", "db");
    table = db.GetTable("dogs");
    sw.Reset();
    for (unsigned i = 0; i < num; i++)
    {
        p = area + i*(ksize+vsize);
        RandomBuffer(p, ksize);
        rk.SetBuffer(p);
        rk.SetLength(ksize);
        p += ksize;
        RandomBuffer(p, vsize);
        rv.SetBuffer(p);
        rv.SetLength(vsize);
        sw.Start();
        table->Set(rk, rv, false);
        sw.Stop();
    }
    printf("%u sets took %ld msec\n", num, sw.Elapsed());
    sw.Restart();
    table->Commit(true /*recovery*/, false /*flush*/);
    elapsed = sw.Stop();
    printf("Commit() took %ld msec\n", elapsed);

    db.Close();

    DCACHE->Shutdown();
    
    free(area);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageShardSize)
{
    StorageDatabase     db;
    StorageTable*       table;
    Buffer              k, v;
    ReadBuffer          rk, rv;
    Stopwatch           sw;
    long                elapsed;
    unsigned            num, len, ksize, vsize;
    char*               area;
    char*               p;
    unsigned            round;

    round = 10;
    num = 100*1000;
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init(10000000);

    db.Open(".", "db");
    // a million key-value pairs take up 248M disk space
    for (unsigned r = 0; r < round; r++)
    {
        table = db.GetTable("dogs");

        sw.Reset();
        for (unsigned i = 0; i < num; i++)
        {
            p = area + i*(ksize+vsize);
            len = snprintf(p, ksize, "%011d", i + r * num); // takes 100 ms
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
        
        printf("%u sets took %ld msec\n", num, sw.Elapsed());

        sw.Reset();
        sw.Start();
        table->Commit(true /*recovery*/, false /*flush*/);
        elapsed = sw.Stop();
        printf("Commit() took %ld msec\n", elapsed);
        printf("Shard size: %s\n", SIBytes(table->GetSize()));
    }
    db.Close();

    DCACHE->Shutdown();

    free(area);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageShardSplit)
{
    StorageDatabase     db;
    StorageTable*       table;
    Buffer              k, v;
    ReadBuffer          rk, rv;
    Stopwatch           sw;
    long                elapsed;
    unsigned            num, len, ksize, vsize;
    char*               area;
    char*               p;
    unsigned            round;
    char                splitKey[] = "00000033620";

    round = 10;
    num = 100*1000;
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init(10000000);

    db.Open(".", "db");
    // a million key-value pairs take up 248M disk space
    for (unsigned r = 0; r < round; r++)
    {
        table = db.GetTable("dogs");

        sw.Reset();
        for (unsigned i = 0; i < num; i++)
        {
            p = area + i*(ksize+vsize);
            len = snprintf(p, ksize, "%011d", i + r * num); // takes 100 ms
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
        
        printf("%u sets took %ld msec\n", num, sw.Elapsed());

        sw.Reset();
        sw.Start();
        table->Commit(true /*recovery*/, false /*flush*/);
        elapsed = sw.Stop();
        printf("Commit() took %ld msec\n", elapsed);
        printf("Shard size: %s\n", SIBytes(table->GetSize()));
    }
    
    
    rk.Wrap(splitKey, sizeof(splitKey) - 1); 
    table->SplitShard(0, 1, rk);
    
    db.Close();
    DCACHE->Shutdown();
    
    free(area);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageFileSplit)
{
    StorageEnvironment  env;
    StorageDatabase*    db;
    StorageTable*       table;
    char                keyvalue[DATAPAGE_MAX_KV_SIZE(DEFAULT_DATAPAGE_SIZE) / 2 + 1];
    ReadBuffer          key;
    ReadBuffer          value;

    env.InitCache(STORAGE_DEFAULT_CACHE_SIZE);
    env.Open("db");
    db = env.GetDatabase("test");
    table = db->GetTable("split");

    // key is empty
    if (!table->ShardExists(key))
        table->CreateShard(0, key);

    // init keyvalues
    for (unsigned i = 0; i < sizeof(keyvalue); i++)
        keyvalue[i] = i % 10 + '0';
    
    key.SetBuffer(keyvalue);
    key.SetLength(DEFAULT_KEY_LIMIT);
    
    value.SetBuffer(keyvalue + DEFAULT_KEY_LIMIT);
    value.SetLength(sizeof(keyvalue) - DEFAULT_KEY_LIMIT);

    // first delete any existing values
    for (unsigned char i = 1; i <= 3; i++)
    {
        memset(keyvalue, i % 10 + '0', DEFAULT_KEY_LIMIT);
        table->Delete(key);
    }
    
    table->Commit();
    
    for (int i = 1; i <= 3; i++)
    {
        memset(keyvalue, i % 10 + '0', DEFAULT_KEY_LIMIT);
        table->Set(key, value, true);
    }
    
    table->Commit();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageFileThreeWaySplit)
{
    StorageEnvironment  env;
    StorageDatabase*    db;
    StorageTable*       table;
    char                keyvalue[DATAPAGE_MAX_KV_SIZE(DEFAULT_DATAPAGE_SIZE)];
    ReadBuffer          key;
    ReadBuffer          value;
    unsigned            i;

    env.InitCache(STORAGE_DEFAULT_CACHE_SIZE);
    env.Open("db");
    db = env.GetDatabase("test");
    table = db->GetTable("threewaysplit");

    // key is empty
    if (!table->ShardExists(key))
        table->CreateShard(0, key);

    // init keyvalues
    for (i = 0; i < sizeof(keyvalue); i++)
        keyvalue[i] = i % 10 + '0';
    
    key.SetBuffer(keyvalue);
    key.SetLength(DEFAULT_KEY_LIMIT);
    
    value.SetBuffer(keyvalue + DEFAULT_KEY_LIMIT);

    // first delete any existing values
    for (unsigned char i = 1; i <= 3; i++)
    {
        memset(keyvalue, i % 10 + '0', DEFAULT_KEY_LIMIT);
        table->Delete(key);
    }
    
    table->Commit();

    i = 1;
    memset(keyvalue, i % 10 + '0', DEFAULT_KEY_LIMIT);
    // TODO: -4 is a correction that yet to be investigated
    value.SetLength((sizeof(keyvalue) / 2) - DEFAULT_KEY_LIMIT - 4);
    table->Set(key, value, true);

    i = 3;
    memset(keyvalue, i % 10 + '0', DEFAULT_KEY_LIMIT);
    value.SetLength((sizeof(keyvalue) / 2) - DEFAULT_KEY_LIMIT - 4);
    table->Set(key, value, true);

    // insert between '1' and '3' a new value which has PAGESIZE size
    i = 2;
    memset(keyvalue, i % 10 + '0', DEFAULT_KEY_LIMIT);
    value.SetLength(sizeof(keyvalue) - DEFAULT_KEY_LIMIT);
    table->Set(key, value, true);
    
    table->Commit();
    
    return TEST_SUCCESS;
}

TEST_MAIN(TestStorage, TestStorageCapacity);
