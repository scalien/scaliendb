#include "Test.h"

#include "Framework/Storage/StorageEnvironment.h"
#include "Framework/Storage/StorageDataPage.h"
#include "Framework/Storage/StorageDataCache.h"
#include "System/FileSystem.h"
#include "System/Stopwatch.h"
#include "System/Common.h"

#define TEST_DATABASE_PATH "."
#define TEST_DATABASE "db"

#define PRINTKV() \
{ \
    v.Write(rv); v.NullTerminate(); k.NullTerminate(); \
    TEST_LOG("%s => %s", k.GetBuffer(), v.GetBuffer()); \
}

// This can be used for ensuring the database is in a blank state
// It is intentionally named with zero in the middle of the name in 
// order to became first when sorted alphabetically.
TEST_DEFINE(TestStorage0DeleteTestDatabase)
{
    Buffer  path;
    char    sep;
    
    sep = FS_Separator();
    path.Append(TEST_DATABASE_PATH);
    path.Append(&sep, 1);
    path.Append(TEST_DATABASE);
    path.NullTerminate();
    
    if (!FS_Exists(path.GetBuffer()))
        return TEST_SUCCESS;
    if (!FS_IsDirectory(path.GetBuffer()))
        TEST_FAIL();
        
    if (!FS_RecDeleteDir(path.GetBuffer()))
        TEST_FAIL();

    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageBasic)
{
    StorageDatabase     db;
    StorageTable*       table;
    StorageDataCache*   cache;
    Buffer              k, v;
    ReadBuffer          rk, rv;
    Stopwatch           sw;
    long                elapsed;
    unsigned            len;
    char*               area;
    char*               p;
    uint64_t            clock;
    
    // Initialization ==============================================================================
    StartClock();

    const unsigned num = 100*1000;
    const unsigned ksize = 20;
    const unsigned vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    cache = DCACHE;
    DCACHE->Init(100000000);

    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    //==============================================================================================
    //
    // TestStorage SET test
    //
    // This test might not work on fast machines, because it commits every 1000 msec, but if there
    // are more sets between commits than the cache can contain, then it will assert!
    //
    //==============================================================================================
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
    db.Commit();
    elapsed = sw.Stop();
    TEST_LOG("%u sets took %ld msec", num, elapsed);

    // GET all keys ================================================================================
    sw.Reset();
    sw.Start();
    for (unsigned i = 0; i < num; i++)
    {
        k.Writef("%d", i);
        if (table->Get(k, rv))
            ;//PRINTKV()
        else
            ASSERT_FAIL();
    }   
    elapsed = sw.Stop();
    TEST_LOG("%u gets took %ld msec", num, elapsed);

    // DELETE all keys =============================================================================
    sw.Reset();
    sw.Start();
    for (unsigned i = 0; i < num; i++)
    {
        k.Writef("%d", i);
        TEST_ASSERT(table->Delete(k));
    }
    db.Commit();
    elapsed = sw.Stop();
    TEST_LOG("%u deletes took %ld msec", num, elapsed);
    
    // Shutdown ====================================================================================
    sw.Reset();
    sw.Start();
    db.Close();
    elapsed = sw.Stop();
    TEST_LOG("Close() took %ld msec", elapsed);
    
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
    unsigned            len;
    char*               area;
    char*               p;
    unsigned            round;

    // Initialization ==============================================================================
    round = 1000;
    const unsigned num = 100*1000;
    const unsigned ksize = 20;
    const unsigned vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init(100000000);
    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    //==============================================================================================
    //
    // test the number of SETs depending on the size of DCACHE and transaction size
    // e.g. a million key-value pairs take up 248M disk space
    //
    //==============================================================================================
    for (unsigned r = 0; r < round; r++)
    {
        // Set key-values ==========================================================================
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
        TEST_LOG("Round %u: %u sets took %ld msec", r, num, (long) sw.Elapsed());

        // Read back key-values ====================================================================
        sw.Restart();
        for (unsigned i = 0; i < num; i++)
        {
            char key[ksize];
            len = snprintf(key, ksize, "%011d", i + r * num); // takes 100 ms
            rk.SetBuffer(key);
            rk.SetLength(len);
            // TODO: change rk to k for crash!
            // The problem is that creating and writing files does not use recovery
            // so if the program crashes while creating new files, the result will
            // be an inconsistent database.
            if (table->Get(rk, rv))
                ;//PRINT()
            else
                ASSERT_FAIL();
        }   
        elapsed = sw.Stop();
        TEST_LOG("Round %u: %u gets took %ld msec", r, num, elapsed);

        // Commit changes ==========================================================================
        sw.Reset();
        sw.Start();
        table->Commit(true /*recovery*/, false /*flush*/);
        elapsed = sw.Stop();
        TEST_LOG("Round %u: Commit() took %ld msec", r, elapsed);
    }

    // Shutdown ====================================================================================
    db.Close();
    DCACHE->Shutdown();
    free(area);

    return TEST_SUCCESS;
}

int UInt64ToBuffer(char* buf, size_t bufsize, uint64_t value);
TEST_DEFINE(TestStorageCapacitySet)
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

    // Initialization ==============================================================================
    round = 1000;
    num = 100*1000;
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init(100000000);
    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    //==============================================================================================
    //
    // test the number of SETs depending on the size of DCACHE and transaction size
    // e.g. a million key-value pairs take up 248M disk space
    //
    //==============================================================================================
    for (unsigned r = 0; r < round; r++)
    {
        // Set key-values ==========================================================================
        sw.Reset();
        for (unsigned i = 0; i < num; i++)
        {
            p = area + i*(ksize+vsize);
            len = UInt64ToBuffer(p, ksize, i + r * num);
            rk.SetBuffer(p);
            rk.SetLength(len);
            //printf("%s\n", p);
            p += ksize;
            //len = snprintf(p, vsize, "%.100f", (float) i + r * num); // takes 100 ms
            len = vsize;
            rv.SetBuffer(p);
            rv.SetLength(len);
            sw.Start();
            table->Set(rk, rv, false);
            sw.Stop();
        }
        TEST_LOG("Round %u: %u sets took %ld msec", r, num, (long) sw.Elapsed());

        // Commit changes ==========================================================================
        sw.Reset();
        sw.Start();
        table->Commit(true /*recovery*/, false /*flush*/);
        elapsed = sw.Stop();
        TEST_LOG("Round %u: Commit() took %ld msec", r, elapsed);
    }

    // Shutdown ====================================================================================
    db.Close();
    DCACHE->Shutdown();
    free(area);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageBigTransaction)
{
    StorageDatabase     db;
    StorageTable*       table;
    StorageDataCache*   cache;
    Buffer              k, v;
    ReadBuffer          rk, rv;
    Stopwatch           sw;
    long                elapsed;
    unsigned            num, len, ksize, vsize;
    char*               area;
    char*               p;

    // Initialization ==============================================================================
    num = 1000*1000;
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init((ksize + vsize) * 2 * num);
    cache = DCACHE;
    TEST_ASSERT(cache != NULL);

    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    // SET key-values ==============================================================================
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
    TEST_LOG("%u sets took %ld msec", num, (long) sw.Elapsed());

    // Commit changes ==============================================================================
    sw.Restart();
    table->Commit(true /*recovery*/, false /*flush*/);
    elapsed = sw.Stop();
    TEST_LOG("Commit() took %ld msec", elapsed);

    // Shutdown ====================================================================================
    db.Close();
    DCACHE->Shutdown();
    free(area);

    return TEST_SUCCESS;
}

/*
====================================================================================================

 TestStorageBigRandomTransaction

====================================================================================================
*/
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

    // Initialization ==============================================================================
    num = 1000*1000;
    ksize = 20;
    vsize = 64;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init((ksize + vsize) * 2 * num);
    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    // SET key-values ==============================================================================
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
    TEST_LOG("%u sets took %ld msec", num, (long) sw.Elapsed());

    // Commit changes ==============================================================================
    sw.Restart();
    table->Commit(true /*recovery*/, false /*flush*/);
    elapsed = sw.Stop();
    TEST_LOG("Commit() took %ld msec", elapsed);

    // Shutdown ====================================================================================
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

    // Initialization ==============================================================================
    round = 10;
    num = 1000*1000;
    ksize = 20;
    vsize = 64;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init((ksize + vsize) * 4 * num);

    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    // SET key-values ==============================================================================
    // a million key-value pairs take up 248M disk space
    for (unsigned r = 0; r < round; r++)
    {
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
        
        printf("%u sets took %ld msec\n", num, (long) sw.Elapsed());

        sw.Reset();
        sw.Start();
        table->Commit(true /*recovery*/, false /*flush*/);
        elapsed = sw.Stop();
        printf("Commit() took %ld msec\n", elapsed);
        printf("Shard size: %s\n", SI_BYTES(table->GetSize()));
    }

    // Shutdown ====================================================================================
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

    // Initialization ==============================================================================
    round = 10;
    num = 100*1000;
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init((ksize + vsize) * 4 * num);

    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    // Write to database in rounds =================================================================
    // a million key-value pairs take up 248M disk space
    for (unsigned r = 0; r < round; r++)
    {

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
        
        printf("%u sets took %ld msec\n", num, (long) sw.Elapsed());

        sw.Reset();
        sw.Start();
        table->Commit(true /*recovery*/, false /*flush*/);
        elapsed = sw.Stop();
        printf("Commit() took %ld msec\n", elapsed);
        printf("Shard size: %s\n", SI_BYTES(table->GetSize()));
    }
    
    // Split on a predefined key ===================================================================    
    rk.Wrap(splitKey, sizeof(splitKey) - 1);
    
    // TODO: calculate the new shardID instead of hardcoded value
    if (!table->GetShard(1))
        table->SplitShard(0, 1, rk);
    
    // Shutdown ====================================================================================
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

    // Initialization ==============================================================================
    env.InitCache(STORAGE_DEFAULT_CACHE_SIZE);
    env.Open(TEST_DATABASE_PATH);
    db = env.GetDatabase(TEST_DATABASE);
    table = db->GetTable(__func__);

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
    
    // create three big keyvalue pairs that will definitely split a page
    for (int i = 1; i <= 3; i++)
    {
        memset(keyvalue, i % 10 + '0', DEFAULT_KEY_LIMIT);
        table->Set(key, value, true);
    }
    table->Commit();
    
    // Shutdown ====================================================================================
    env.Close();
    
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

    // Initialization ==============================================================================
    env.InitCache(STORAGE_DEFAULT_CACHE_SIZE);
    env.Open(TEST_DATABASE_PATH);
    db = env.GetDatabase(TEST_DATABASE);
    table = db->GetTable(__func__);

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
    value.SetLength((sizeof(keyvalue) / 2) - DEFAULT_KEY_LIMIT - (DATAPAGE_KV_OVERHEAD / 2));
    table->Set(key, value, true);

    i = 3;
    memset(keyvalue, i % 10 + '0', DEFAULT_KEY_LIMIT);
    value.SetLength((sizeof(keyvalue) / 2) - DEFAULT_KEY_LIMIT - (DATAPAGE_KV_OVERHEAD / 2));
    table->Set(key, value, true);

    // insert between '1' and '3' a new value which has PAGESIZE size
    i = 2;
    memset(keyvalue, i % 10 + '0', DEFAULT_KEY_LIMIT);
    value.SetLength(sizeof(keyvalue) - DEFAULT_KEY_LIMIT);
    table->Set(key, value, true);

    table->Commit();

    // Shutdown ====================================================================================
    env.Close();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageBinaryData)
{
    StorageEnvironment  env;
    StorageDatabase*    db;
    StorageTable*       table;
    char                keyvalue[DATAPAGE_MAX_KV_SIZE(DEFAULT_DATAPAGE_SIZE)];
    ReadBuffer          key;
    ReadBuffer          value;
    unsigned            i;
    unsigned            num;

    // Initialization ==============================================================================
    num = 1000;
    env.InitCache(sizeof(keyvalue) * 2 * num);
    env.Open(TEST_DATABASE_PATH);
    db = env.GetDatabase(TEST_DATABASE);
    table = db->GetTable(__func__);

    // key is empty
    if (!table->ShardExists(key))
        table->CreateShard(0, key);

    // init keyvalues
    for (i = 0; i < sizeof(keyvalue); i++)
        keyvalue[i] = i % 256;
    
    key.SetBuffer(keyvalue);
    key.SetLength(DEFAULT_KEY_LIMIT);
    value.SetBuffer(keyvalue + DEFAULT_KEY_LIMIT);
    value.SetLength((sizeof(keyvalue) / 2) - DEFAULT_KEY_LIMIT - (DATAPAGE_KV_OVERHEAD / 2));
    
    for (i = 0; i < num; i++)
    {
        snprintf(keyvalue, sizeof(keyvalue), "%u", i);
        table->Set(key, value, true);        
    }
    
    table->Commit();

    // Shutdown ====================================================================================
    env.Close();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageWindowsSync)
{
    StorageDatabase     db;
    StorageTable*       table;
    Buffer              k, v;
    ReadBuffer          rk, rv;
    Stopwatch           sw;
    unsigned            num, ksize, vsize;
    char*               area;
    unsigned            round;
    bool                found;

    // Initialization ==============================================================================
    round = 1000 * 1000;
    num = 1000;
    ksize = 10;
    vsize = 10;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init((ksize + vsize) * 2 * 1000 * num);
    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    // Get key-values ==========================================================================

    rk.SetBuffer("egyikkulcs");
    rk.SetLength(10);
    found = table->Get(rk, rv);

    rk.SetBuffer("masikkulcs");
    rk.SetLength(10);
    found = table->Get(rk, rv);

    // Set key-values ==========================================================================

    rk.SetBuffer("egyikkulcs");
    rk.SetLength(10);
    rv.SetBuffer("egyikertek");
    rv.SetLength(10);
    table->Set(rk, rv, true);

    // Commit changes ==========================================================================
    table->Commit(true /*recovery*/, true /*flush*/);
     
    // Set key-values ==========================================================================

    rk.SetBuffer("masikkulcs");
    rk.SetLength(10);
    rv.SetBuffer("masikertek");
    rv.SetLength(10);
    table->Set(rk, rv, true);

    // Commit changes ==========================================================================
    table->Commit(true /*recovery*/, true /*flush*/);

    // Shutdown ====================================================================================
    db.Close();
    DCACHE->Shutdown();
    free(area);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageRandomGetSetDelete)
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

    // Initialization ==============================================================================
    round = 1000 * 1000;
    num = 1000;
    ksize = 10;
    vsize = 10;
    area = (char*) malloc(num*(ksize+vsize));

    DCACHE->Init((ksize + vsize) * 2 * 1000 * num);
    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    //==============================================================================================
    //
    // test the number of SETs depending on the size of DCACHE and transaction size
    // e.g. a million key-value pairs take up 248M disk space
    //
    //==============================================================================================
    for (unsigned r = 0; r < round; r++)
    {
        // Set key-values ==========================================================================
        sw.Reset();
        for (unsigned i = 0; i < num; i++)
        {
            p = area + i*(ksize+vsize);
            len = snprintf(p, ksize + 1, "%010d", RandomInt(0, 10 * 1000 * num));
            rk.SetBuffer(p);
            rk.SetLength(len);
            //printf("%s\n", p);
            p += ksize;
            len = snprintf(p, vsize + 1, "%010d", i); // takes 100 ms
            rv.SetBuffer(p);
            rv.SetLength(vsize);
            sw.Start();
            switch (RandomInt(0, 3))
            {
            case 0:
            case 1:
                TEST_LOG("Set, %.*s", rk.GetLength(), rk.GetBuffer());
                table->Set(rk, rv, true);
                break;
            case 2:
                TEST_LOG("Delete, %.*s", rk.GetLength(), rk.GetBuffer());
                table->Delete(rk);
                break;
            case 3:
                TEST_LOG("Get, %.*s", rk.GetLength(), rk.GetBuffer());
                table->Get(rk, rv);
                break;
            default:
                ASSERT_FAIL();
            }
            sw.Stop();
        }
        TEST_LOG("Round %u: %u sets took %ld msec", r, num, (long) sw.Elapsed());

        // Commit changes ==========================================================================
        sw.Reset();
        sw.Start();
        table->Commit(true /*recovery*/, true /*flush*/);
        elapsed = sw.Stop();
        TEST_LOG("Round %u: Commit() took %ld msec", r, elapsed);
    }

    // Shutdown ====================================================================================
    db.Close();
    DCACHE->Shutdown();
    free(area);

    return TEST_SUCCESS;
}


TEST_MAIN(TestStorage, TestStorageCapacitySet);

TEST_DEFINE(TestStorageAppend)
{
    StorageDatabase     db;
    StorageTable*       table;
    StorageDataCache*   cache;
    Buffer              k, v;
    ReadBuffer          rk, rv;
    Stopwatch           sw;
    long                elapsed;
    unsigned            len;
    char*               area;
    char*               p;
    uint64_t            clock;
    
    // Initialization ==============================================================================
    StartClock();

    const unsigned num = 100*1000;
    const unsigned ksize = 20;
    const unsigned vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));

    cache = DCACHE;
    DCACHE->Init(100000000);

    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    //==============================================================================================
    //
    // TestStorage SET test
    //
    // This test might not work on fast machines, because it commits every 1000 msec, but if there
    // are more sets between commits than the cache can contain, then it will assert!
    //
    //==============================================================================================
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
    db.Commit();
    elapsed = sw.Stop();
    TEST_LOG("%u sets took %ld msec", num, elapsed);

    // APPEND "a" to all values ================================================================================
    rv.Wrap("a");
    sw.Reset();
    sw.Start();
    for (unsigned i = 0; i < num; i++)
    {
        k.Writef("%d", i);
        if (table->Append(k, rv))
            ;//PRINTKV()
        else
            ASSERT_FAIL();
    }   
    db.Commit();
    elapsed = sw.Stop();
    TEST_LOG("%u appends took %ld msec", num, elapsed);

    // GET all keys ================================================================================
    sw.Reset();
    sw.Start();
    for (unsigned i = 0; i < num; i++)
    {
        k.Writef("%d", i);
        if (table->Get(k, rv))
            ;//PRINTKV()
        else
            ASSERT_FAIL();
    }   
    elapsed = sw.Stop();
    TEST_LOG("%u gets took %ld msec", num, elapsed);

    // DELETE all keys =============================================================================
    sw.Reset();
    sw.Start();
    for (unsigned i = 0; i < num; i++)
    {
        k.Writef("%d", i);
        TEST_ASSERT(table->Delete(k));
    }
    db.Commit();
    elapsed = sw.Stop();
    TEST_LOG("%u deletes took %ld msec", num, elapsed);
    
    // Shutdown ====================================================================================
    sw.Reset();
    sw.Start();
    db.Close();
    elapsed = sw.Stop();
    TEST_LOG("Close() took %ld msec", elapsed);
    
    DCACHE->Shutdown();
    free(area);
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageCrashCase1)
{
    const int ksize = 20;
    const int vsize = 128;

    char key[ksize];
    char value[vsize];
    ReadBuffer rk;
    ReadBuffer rv;
    ReadBuffer tmp;

    StorageDatabase     db;
    StorageTable*       table;
   
    memset(key, 0, ksize);
    memset(value, 0, vsize);
    rk.Wrap(key, ksize);
    rv.Wrap(value, vsize);

    DCACHE->Init(100000000);

    db.Open(TEST_DATABASE_PATH, TEST_DATABASE);
    table = db.GetTable(__func__);

    snprintf(key, ksize, "00003943829"); table->Delete(rk);
    snprintf(key, ksize, "00007984400"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001975514"); table->Set(rk, rv);
    snprintf(key, ksize, "00007682296"); table->Set(rk, rv);
    snprintf(key, ksize, "00005539700"); table->Set(rk, rv);
    snprintf(key, ksize, "00006288709"); table->Set(rk, rv);
    snprintf(key, ksize, "00005134009"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009161951"); table->Delete(rk);
    snprintf(key, ksize, "00007172969"); table->Set(rk, rv);
    snprintf(key, ksize, "00006069689"); table->Set(rk, rv);
    snprintf(key, ksize, "00002428868"); table->Set(rk, rv);
    snprintf(key, ksize, "00008041768"); table->Set(rk, rv);
    snprintf(key, ksize, "00004009444"); table->Set(rk, rv);
    snprintf(key, ksize, "00001088088"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002182569"); table->Delete(rk);
    snprintf(key, ksize, "00008391122"); table->Delete(rk);
    snprintf(key, ksize, "00002960316"); table->Delete(rk);
    snprintf(key, ksize, "00005242872"); table->Set(rk, rv);
    snprintf(key, ksize, "00009727750"); table->Set(rk, rv);
    snprintf(key, ksize, "00007713577"); table->Delete(rk);
    snprintf(key, ksize, "00007699138"); table->Set(rk, rv);
    snprintf(key, ksize, "00008915295"); table->Set(rk, rv);
    snprintf(key, ksize, "00003524583"); table->Delete(rk);
    snprintf(key, ksize, "00009190265"); table->Set(rk, rv);
    snprintf(key, ksize, "00009493271"); table->Delete(rk);
    snprintf(key, ksize, "00000860558"); table->Set(rk, rv);
    snprintf(key, ksize, "00006632269"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003488929"); table->Set(rk, rv);
    snprintf(key, ksize, "00000200230"); table->Set(rk, rv);
    snprintf(key, ksize, "00000630958"); table->Set(rk, rv);
    snprintf(key, ksize, "00009706341"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008509198"); table->Set(rk, rv);
    snprintf(key, ksize, "00005397603"); table->Set(rk, rv);
    snprintf(key, ksize, "00007602487"); table->Delete(rk);
    snprintf(key, ksize, "00006677238"); table->Delete(rk);
    snprintf(key, ksize, "00000392803"); table->Set(rk, rv);
    snprintf(key, ksize, "00009318351"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007209523"); table->Set(rk, rv);
    snprintf(key, ksize, "00007385343"); table->Delete(rk);
    snprintf(key, ksize, "00003540487"); table->Delete(rk);
    snprintf(key, ksize, "00001659742"); table->Set(rk, rv);
    snprintf(key, ksize, "00008800752"); table->Delete(rk);
    snprintf(key, ksize, "00003303371"); table->Set(rk, rv);
    snprintf(key, ksize, "00008933724"); table->Set(rk, rv);
    snprintf(key, ksize, "00006866699"); table->Get(rk, tmp);
    snprintf(key, ksize, "00005886401"); table->Delete(rk);
    snprintf(key, ksize, "00008586763"); table->Set(rk, rv);
    snprintf(key, ksize, "00009239698"); table->Set(rk, rv);
    snprintf(key, ksize, "00008147669"); table->Delete(rk);
    snprintf(key, ksize, "00009109720"); table->Set(rk, rv);
    snprintf(key, ksize, "00002158250"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009201283"); table->Set(rk, rv);
    snprintf(key, ksize, "00008810622"); table->Delete(rk);
    snprintf(key, ksize, "00004319534"); table->Delete(rk);
    snprintf(key, ksize, "00002810594"); table->Delete(rk);
    snprintf(key, ksize, "00003074579"); table->Set(rk, rv);
    snprintf(key, ksize, "00002261066"); table->Set(rk, rv);
    snprintf(key, ksize, "00002762347"); table->Delete(rk);
    snprintf(key, ksize, "00004165013"); table->Set(rk, rv);
    snprintf(key, ksize, "00009068039"); table->Set(rk, rv);
    snprintf(key, ksize, "00001260753"); table->Set(rk, rv);
    snprintf(key, ksize, "00007604752"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009350040"); table->Delete(rk);
    snprintf(key, ksize, "00003831883"); table->Delete(rk);
    snprintf(key, ksize, "00003686635"); table->Set(rk, rv);
    snprintf(key, ksize, "00002322615"); table->Delete(rk);
    snprintf(key, ksize, "00002444127"); table->Set(rk, rv);
    snprintf(key, ksize, "00007321485"); table->Set(rk, rv);
    snprintf(key, ksize, "00007934704"); table->Set(rk, rv);
    snprintf(key, ksize, "00007450714"); table->Set(rk, rv);
    snprintf(key, ksize, "00009501040"); table->Set(rk, rv);
    snprintf(key, ksize, "00005215634"); table->Set(rk, rv);
    snprintf(key, ksize, "00002400624"); table->Delete(rk);
    snprintf(key, ksize, "00007326544"); table->Delete(rk);
    snprintf(key, ksize, "00009674051"); table->Delete(rk);
    snprintf(key, ksize, "00007597348"); table->Set(rk, rv);
    snprintf(key, ksize, "00001349024"); table->Delete(rk);
    snprintf(key, ksize, "00000782321"); table->Set(rk, rv);
    snprintf(key, ksize, "00002046551"); table->Set(rk, rv);
    snprintf(key, ksize, "00008196773"); table->Delete(rk);
    snprintf(key, ksize, "00007555808"); table->Set(rk, rv);
    snprintf(key, ksize, "00001578071"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002043286"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001254685"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000540576"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000723288"); table->Set(rk, rv);
    snprintf(key, ksize, "00009230691"); table->Delete(rk);
    snprintf(key, ksize, "00001803723"); table->Set(rk, rv);
    snprintf(key, ksize, "00003916902"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008196952"); table->Set(rk, rv);
    snprintf(key, ksize, "00005524850"); table->Delete(rk);
    snprintf(key, ksize, "00004525758"); table->Delete(rk);
    snprintf(key, ksize, "00000996401"); table->Delete(rk);
    snprintf(key, ksize, "00007572938"); table->Set(rk, rv);
    snprintf(key, ksize, "00009922285"); table->Delete(rk);
    snprintf(key, ksize, "00008776138"); table->Delete(rk);
    snprintf(key, ksize, "00006289099"); table->Set(rk, rv);
    snprintf(key, ksize, "00007478029"); table->Delete(rk);
    snprintf(key, ksize, "00009253766"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008310375"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007438112"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009835957"); table->Delete(rk);
    snprintf(key, ksize, "00004972585"); table->Set(rk, rv);
    snprintf(key, ksize, "00008300118"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000769947"); table->Delete(rk);
    snprintf(key, ksize, "00002480441"); table->Delete(rk);
    snprintf(key, ksize, "00002291370"); table->Delete(rk);
    snprintf(key, ksize, "00003168671"); table->Set(rk, rv);
    snprintf(key, ksize, "00002314280"); table->Set(rk, rv);
    snprintf(key, ksize, "00006330722"); table->Set(rk, rv);
    snprintf(key, ksize, "00006511321"); table->Delete(rk);
    snprintf(key, ksize, "00009714657"); table->Set(rk, rv);
    snprintf(key, ksize, "00005461069"); table->Delete(rk);
    snprintf(key, ksize, "00001132806"); table->Set(rk, rv);
    snprintf(key, ksize, "00005925399"); table->Get(rk, tmp);
    snprintf(key, ksize, "00004509176"); table->Set(rk, rv);
    snprintf(key, ksize, "00008476844"); table->Set(rk, rv);
    snprintf(key, ksize, "00000032315"); table->Set(rk, rv);
    snprintf(key, ksize, "00005984813"); table->Delete(rk);
    snprintf(key, ksize, "00002338917"); table->Delete(rk);
    snprintf(key, ksize, "00004829503"); table->Set(rk, rv);
    snprintf(key, ksize, "00003049557"); table->Delete(rk);
    snprintf(key, ksize, "00001825558"); table->Delete(rk);
    snprintf(key, ksize, "00000408643"); table->Set(rk, rv);
    snprintf(key, ksize, "00006959838"); table->Delete(rk);
    snprintf(key, ksize, "00006376402"); table->Set(rk, rv);
    snprintf(key, ksize, "00001846225"); table->Delete(rk);
    snprintf(key, ksize, "00006271579"); table->Delete(rk);
    snprintf(key, ksize, "00003283744"); table->Delete(rk);
    snprintf(key, ksize, "00002022128"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006847565"); table->Delete(rk);
    snprintf(key, ksize, "00002572655"); table->Delete(rk);
    snprintf(key, ksize, "00000876436"); table->Set(rk, rv);
    snprintf(key, ksize, "00008773839"); table->Delete(rk);
    snprintf(key, ksize, "00000937402"); table->Set(rk, rv);
    snprintf(key, ksize, "00003616009"); table->Delete(rk);
    snprintf(key, ksize, "00005932115"); table->Delete(rk);
    snprintf(key, ksize, "00002887778"); table->Delete(rk);
    snprintf(key, ksize, "00002883794"); table->Set(rk, rv);
    snprintf(key, ksize, "00001897510"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000035786"); table->Delete(rk);
    snprintf(key, ksize, "00003314791"); table->Set(rk, rv);
    snprintf(key, ksize, "00004364970"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009189304"); table->Delete(rk);
    snprintf(key, ksize, "00006990754"); table->Set(rk, rv);
    snprintf(key, ksize, "00006857858"); table->Set(rk, rv);
    snprintf(key, ksize, "00007742735"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009162729"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002035482"); table->Delete(rk);
    snprintf(key, ksize, "00005480420"); table->Set(rk, rv);
    snprintf(key, ksize, "00009049325"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008739790"); table->Set(rk, rv);
    snprintf(key, ksize, "00005761995"); table->Set(rk, rv);
    snprintf(key, ksize, "00002739112"); table->Get(rk, tmp);
    snprintf(key, ksize, "00004923988"); table->Set(rk, rv);
    snprintf(key, ksize, "00008489422"); table->Set(rk, rv);
    snprintf(key, ksize, "00002910533"); table->Set(rk, rv);
    snprintf(key, ksize, "00006841784"); table->Delete(rk);
    snprintf(key, ksize, "00001390582"); table->Delete(rk);
    snprintf(key, ksize, "00004924217"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007242520"); table->Set(rk, rv);
    snprintf(key, ksize, "00002219655"); table->Set(rk, rv);
    snprintf(key, ksize, "00001212588"); table->Set(rk, rv);
    snprintf(key, ksize, "00003604426"); table->Set(rk, rv);
    snprintf(key, ksize, "00009318953"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006220955"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008181276"); table->Set(rk, rv);
    snprintf(key, ksize, "00003349717"); table->Set(rk, rv);
    snprintf(key, ksize, "00006588312"); table->Delete(rk);
    snprintf(key, ksize, "00002589061"); table->Set(rk, rv);
    snprintf(key, ksize, "00000725450"); table->Set(rk, rv);
    snprintf(key, ksize, "00006472074"); table->Set(rk, rv);
    snprintf(key, ksize, "00002882695"); table->Set(rk, rv);
    snprintf(key, ksize, "00000911486"); table->Set(rk, rv);
    snprintf(key, ksize, "00009344946"); table->Delete(rk);
    snprintf(key, ksize, "00002654613"); table->Delete(rk);
    snprintf(key, ksize, "00007617778"); table->Set(rk, rv);
    snprintf(key, ksize, "00001572721"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006256653"); table->Delete(rk);
    snprintf(key, ksize, "00002078436"); table->Delete(rk);
    snprintf(key, ksize, "00004261994"); table->Delete(rk);
    snprintf(key, ksize, "00003943884"); table->Set(rk, rv);
    snprintf(key, ksize, "00003260135"); table->Delete(rk);
    snprintf(key, ksize, "00006386541"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003382429"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001360746"); table->Set(rk, rv);
    snprintf(key, ksize, "00000054085"); table->Delete(rk);
    snprintf(key, ksize, "00007743862"); table->Set(rk, rv);
    snprintf(key, ksize, "00001146678"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007210058"); table->Set(rk, rv);
    snprintf(key, ksize, "00004491051"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007079091"); table->Set(rk, rv);
    snprintf(key, ksize, "00004738940"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000939195"); table->Set(rk, rv);
    snprintf(key, ksize, "00003828959"); table->Set(rk, rv);
    snprintf(key, ksize, "00006571199"); table->Delete(rk);
    snprintf(key, ksize, "00001317021"); table->Set(rk, rv);
    snprintf(key, ksize, "00000534223"); table->Set(rk, rv);
    snprintf(key, ksize, "00007808684"); table->Delete(rk);
    snprintf(key, ksize, "00004425602"); table->Set(rk, rv);
    snprintf(key, ksize, "00005896367"); table->Delete(rk);
    snprintf(key, ksize, "00005298992"); table->Delete(rk);
    snprintf(key, ksize, "00003619168"); table->Set(rk, rv);
    snprintf(key, ksize, "00008887233"); table->Set(rk, rv);
    snprintf(key, ksize, "00001698203"); table->Delete(rk);
    snprintf(key, ksize, "00005257471"); table->Delete(rk);
    snprintf(key, ksize, "00005961962"); table->Set(rk, rv);
    snprintf(key, ksize, "00008298083"); table->Set(rk, rv);
    snprintf(key, ksize, "00000988374"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001696495"); table->Set(rk, rv);
    snprintf(key, ksize, "00002254909"); table->Delete(rk);
    snprintf(key, ksize, "00002908287"); table->Set(rk, rv);
    snprintf(key, ksize, "00008782777"); table->Set(rk, rv);
    snprintf(key, ksize, "00008149087"); table->Delete(rk);
    snprintf(key, ksize, "00000363274"); table->Set(rk, rv);
    snprintf(key, ksize, "00007782573"); table->Delete(rk);
    snprintf(key, ksize, "00008361037"); table->Set(rk, rv);
    snprintf(key, ksize, "00002210094"); table->Set(rk, rv);
    snprintf(key, ksize, "00006124420"); table->Set(rk, rv);
    snprintf(key, ksize, "00006746052"); table->Delete(rk);
    snprintf(key, ksize, "00007194618"); table->Set(rk, rv);
    snprintf(key, ksize, "00004011877"); table->Set(rk, rv);
    snprintf(key, ksize, "00004340086"); table->Set(rk, rv);
    snprintf(key, ksize, "00003857482"); table->Delete(rk);
    snprintf(key, ksize, "00001547238"); table->Delete(rk);
    snprintf(key, ksize, "00000145793"); table->Set(rk, rv);
    snprintf(key, ksize, "00003821671"); table->Set(rk, rv);
    snprintf(key, ksize, "00007374077"); table->Set(rk, rv);
    snprintf(key, ksize, "00006496590"); table->Delete(rk);
    snprintf(key, ksize, "00009195909"); table->Delete(rk);
    snprintf(key, ksize, "00008097853"); table->Delete(rk);
    snprintf(key, ksize, "00003119505"); table->Delete(rk);
    snprintf(key, ksize, "00000060048"); table->Delete(rk);
    snprintf(key, ksize, "00008439096"); table->Delete(rk);
    snprintf(key, ksize, "00006426925"); table->Delete(rk);
    snprintf(key, ksize, "00004007090"); table->Set(rk, rv);
    snprintf(key, ksize, "00007188672"); table->Delete(rk);
    snprintf(key, ksize, "00006778124"); table->Set(rk, rv);
    snprintf(key, ksize, "00000328927"); table->Set(rk, rv);
    snprintf(key, ksize, "00006857218"); table->Set(rk, rv);
    snprintf(key, ksize, "00006189583"); table->Delete(rk);
    snprintf(key, ksize, "00005678311"); table->Set(rk, rv);
    snprintf(key, ksize, "00000057091"); table->Set(rk, rv);
    snprintf(key, ksize, "00002615703"); table->Delete(rk);
    snprintf(key, ksize, "00008575552"); table->Set(rk, rv);
    snprintf(key, ksize, "00003413545"); table->Delete(rk);
    snprintf(key, ksize, "00008790094"); table->Delete(rk);
    snprintf(key, ksize, "00003132296"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001862648"); table->Set(rk, rv);
    snprintf(key, ksize, "00005034610"); table->Delete(rk);
    snprintf(key, ksize, "00006756541"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001911116"); table->Set(rk, rv);
    snprintf(key, ksize, "00007060667"); table->Get(rk, tmp);
    snprintf(key, ksize, "00005473971"); table->Delete(rk);
    snprintf(key, ksize, "00009324846"); table->Set(rk, rv);
    snprintf(key, ksize, "00009265758"); table->Delete(rk);
    snprintf(key, ksize, "00009334200"); table->Set(rk, rv);
    snprintf(key, ksize, "00005525684"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007996457"); table->Delete(rk);
    snprintf(key, ksize, "00005944973"); table->Delete(rk);
    snprintf(key, ksize, "00009952999"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003245414"); table->Get(rk, tmp);
    snprintf(key, ksize, "00005891567"); table->Delete(rk);
    snprintf(key, ksize, "00007593236"); table->Delete(rk);
    snprintf(key, ksize, "00007949102"); table->Set(rk, rv);
    snprintf(key, ksize, "00006043788"); table->Set(rk, rv);
    snprintf(key, ksize, "00001669546"); table->Delete(rk);
    snprintf(key, ksize, "00008650855"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006644144"); table->Set(rk, rv);
    snprintf(key, ksize, "00006119807"); table->Delete(rk);
    snprintf(key, ksize, "00006456015"); table->Delete(rk);
    snprintf(key, ksize, "00001483419"); table->Delete(rk);
    snprintf(key, ksize, "00000329634"); table->Delete(rk);
    snprintf(key, ksize, "00005181507"); table->Delete(rk);
    snprintf(key, ksize, "00005150491"); table->Set(rk, rv);
    snprintf(key, ksize, "00004898100"); table->Delete(rk);
    snprintf(key, ksize, "00000484997"); table->Delete(rk);
    snprintf(key, ksize, "00003846584"); table->Delete(rk);
    snprintf(key, ksize, "00004521224"); table->Set(rk, rv);
    snprintf(key, ksize, "00004130778"); table->Set(rk, rv);
    snprintf(key, ksize, "00004067666"); table->Set(rk, rv);
    snprintf(key, ksize, "00007175967"); table->Delete(rk);
    snprintf(key, ksize, "00008129470"); table->Delete(rk);
    snprintf(key, ksize, "00004467425"); table->Set(rk, rv);
    snprintf(key, ksize, "00009951648"); table->Set(rk, rv);
    snprintf(key, ksize, "00000742604"); table->Delete(rk);
    snprintf(key, ksize, "00005972798"); table->Set(rk, rv);
    snprintf(key, ksize, "00002197878"); table->Delete(rk);
    snprintf(key, ksize, "00009235127"); table->Delete(rk);
    snprintf(key, ksize, "00004628522"); table->Set(rk, rv);
    snprintf(key, ksize, "00008505864"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009489108"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007670137"); table->Set(rk, rv);
    snprintf(key, ksize, "00005367425"); table->Set(rk, rv);
    snprintf(key, ksize, "00004775512"); table->Get(rk, tmp);
    snprintf(key, ksize, "00004661686"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009672769"); table->Set(rk, rv);
    snprintf(key, ksize, "00004580390"); table->Delete(rk);
    snprintf(key, ksize, "00007664476"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002575854"); table->Delete(rk);
    snprintf(key, ksize, "00009635048"); table->Set(rk, rv);
    snprintf(key, ksize, "00004023788"); table->Delete(rk);
    snprintf(key, ksize, "00005544482"); table->Delete(rk);
    snprintf(key, ksize, "00001910277"); table->Set(rk, rv);
    snprintf(key, ksize, "00003601051"); table->Delete(rk);
    snprintf(key, ksize, "00009165227"); table->Set(rk, rv);
    snprintf(key, ksize, "00006065422"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001097777"); table->Set(rk, rv);
    snprintf(key, ksize, "00001990027"); table->Delete(rk);
    snprintf(key, ksize, "00005926919"); table->Delete(rk);
    snprintf(key, ksize, "00005963405"); table->Set(rk, rv);
    snprintf(key, ksize, "00005608718"); table->Delete(rk);
    snprintf(key, ksize, "00002426258"); table->Set(rk, rv);
    snprintf(key, ksize, "00003438414"); table->Set(rk, rv);
    snprintf(key, ksize, "00009236923"); table->Delete(rk);
    snprintf(key, ksize, "00007706859"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009332725"); table->Set(rk, rv);
    snprintf(key, ksize, "00004479815"); table->Set(rk, rv);
    snprintf(key, ksize, "00007952313"); table->Delete(rk);
    snprintf(key, ksize, "00009656816"); table->Set(rk, rv);
    snprintf(key, ksize, "00002928891"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003660279"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007476378"); table->Set(rk, rv);
    snprintf(key, ksize, "00002729871"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001223258"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006231945"); table->Delete(rk);
    snprintf(key, ksize, "00009245395"); table->Set(rk, rv);
    snprintf(key, ksize, "00002822838"); table->Set(rk, rv);
    snprintf(key, ksize, "00002029770"); table->Delete(rk);
    snprintf(key, ksize, "00001762388"); table->Set(rk, rv);
    snprintf(key, ksize, "00002275519"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000138663"); table->Set(rk, rv);
    snprintf(key, ksize, "00001199893"); table->Set(rk, rv);
    snprintf(key, ksize, "00006485451"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001008570"); table->Delete(rk);
    snprintf(key, ksize, "00000705570"); table->Set(rk, rv);
    snprintf(key, ksize, "00004964310"); table->Set(rk, rv);
    snprintf(key, ksize, "00002931774"); table->Set(rk, rv);
    snprintf(key, ksize, "00009123905"); table->Delete(rk);
    snprintf(key, ksize, "00001907092"); table->Set(rk, rv);
    snprintf(key, ksize, "00004318435"); table->Delete(rk);
    snprintf(key, ksize, "00007533827"); table->Set(rk, rv);
    snprintf(key, ksize, "00009979699"); table->Set(rk, rv);
    snprintf(key, ksize, "00005235484"); table->Set(rk, rv);
    snprintf(key, ksize, "00006617916"); table->Delete(rk);
    snprintf(key, ksize, "00003276161"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006467118"); table->Set(rk, rv);
    snprintf(key, ksize, "00000501679"); table->Delete(rk);
    snprintf(key, ksize, "00008033302"); table->Delete(rk);
    snprintf(key, ksize, "00006819217"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003129397"); table->Delete(rk);
    snprintf(key, ksize, "00002979332"); table->Delete(rk);
    snprintf(key, ksize, "00001890636"); table->Delete(rk);
    snprintf(key, ksize, "00000534394"); table->Set(rk, rv);
    snprintf(key, ksize, "00001572751"); table->Set(rk, rv);
    snprintf(key, ksize, "00001361705"); table->Delete(rk);
    snprintf(key, ksize, "00000580523"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009455016"); table->Set(rk, rv);
    snprintf(key, ksize, "00009252196"); table->Set(rk, rv);
    snprintf(key, ksize, "00002569691"); table->Delete(rk);
    snprintf(key, ksize, "00001688372"); table->Delete(rk);
    snprintf(key, ksize, "00004763547"); table->Delete(rk);
    snprintf(key, ksize, "00009260675"); table->Delete(rk);
    snprintf(key, ksize, "00005822501"); table->Delete(rk);
    snprintf(key, ksize, "00002252356"); table->Set(rk, rv);
    snprintf(key, ksize, "00006335848"); table->Delete(rk);
    snprintf(key, ksize, "00000166506"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003475460"); table->Set(rk, rv);
    snprintf(key, ksize, "00005226287"); table->Set(rk, rv);
    snprintf(key, ksize, "00003071683"); table->Delete(rk);
    snprintf(key, ksize, "00006451340"); table->Set(rk, rv);
    snprintf(key, ksize, "00002690224"); table->Delete(rk);
    snprintf(key, ksize, "00003328920"); table->Set(rk, rv);
    snprintf(key, ksize, "00007592084"); table->Set(rk, rv);
    snprintf(key, ksize, "00006835740"); table->Set(rk, rv);
    snprintf(key, ksize, "00008451229"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006007626"); table->Set(rk, rv);
    snprintf(key, ksize, "00006679602"); table->Delete(rk);
    snprintf(key, ksize, "00008480001"); table->Set(rk, rv);
    snprintf(key, ksize, "00002562279"); table->Set(rk, rv);
    snprintf(key, ksize, "00005143822"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006114109"); table->Delete(rk);
    snprintf(key, ksize, "00008213308"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007367470"); table->Set(rk, rv);
    snprintf(key, ksize, "00003599422"); table->Set(rk, rv);
    snprintf(key, ksize, "00000238632"); table->Set(rk, rv);
    snprintf(key, ksize, "00004872542"); table->Set(rk, rv);
    snprintf(key, ksize, "00007082625"); table->Delete(rk);
    snprintf(key, ksize, "00005074096"); table->Set(rk, rv);
    snprintf(key, ksize, "00000782579"); table->Set(rk, rv);
    snprintf(key, ksize, "00004836484"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000433947"); table->Set(rk, rv);
    snprintf(key, ksize, "00002448583"); table->Delete(rk);
    snprintf(key, ksize, "00006112412"); table->Set(rk, rv);
    snprintf(key, ksize, "00009615652"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001660941"); table->Set(rk, rv);
    snprintf(key, ksize, "00007572818"); table->Delete(rk);
    snprintf(key, ksize, "00000069801"); table->Delete(rk);
    snprintf(key, ksize, "00007364619"); table->Delete(rk);
    snprintf(key, ksize, "00009225720"); table->Set(rk, rv);
    snprintf(key, ksize, "00007876424"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001014803"); table->Set(rk, rv);
    snprintf(key, ksize, "00002393208"); table->Delete(rk);
    snprintf(key, ksize, "00000950427"); table->Delete(rk);
    snprintf(key, ksize, "00002772136"); table->Set(rk, rv);
    snprintf(key, ksize, "00009377139"); table->Delete(rk);
    snprintf(key, ksize, "00000966814"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008452730"); table->Set(rk, rv);
    snprintf(key, ksize, "00006924635"); table->Set(rk, rv);
    snprintf(key, ksize, "00004343981"); table->Delete(rk);
    snprintf(key, ksize, "00003239832"); table->Delete(rk);
    snprintf(key, ksize, "00001299760"); table->Set(rk, rv);
    snprintf(key, ksize, "00003779971"); table->Set(rk, rv);
    snprintf(key, ksize, "00006598776"); table->Set(rk, rv);
    snprintf(key, ksize, "00008806832"); table->Delete(rk);
    snprintf(key, ksize, "00002108632"); table->Delete(rk);
    snprintf(key, ksize, "00005288848"); table->Set(rk, rv);
    snprintf(key, ksize, "00009432221"); table->Delete(rk);
    snprintf(key, ksize, "00001220862"); table->Set(rk, rv);
    snprintf(key, ksize, "00005149360"); table->Set(rk, rv);
    snprintf(key, ksize, "00002115655"); table->Set(rk, rv);
    snprintf(key, ksize, "00001601619"); table->Set(rk, rv);
    snprintf(key, ksize, "00004337584"); table->Set(rk, rv);
    snprintf(key, ksize, "00006497865"); table->Set(rk, rv);
    snprintf(key, ksize, "00004619491"); table->Set(rk, rv);
    snprintf(key, ksize, "00007802505"); table->Delete(rk);
    snprintf(key, ksize, "00006846768"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008671973"); table->Set(rk, rv);
    snprintf(key, ksize, "00000471826"); table->Delete(rk);
    snprintf(key, ksize, "00001771330"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001095246"); table->Set(rk, rv);
    snprintf(key, ksize, "00005961913"); table->Delete(rk);
    snprintf(key, ksize, "00007003396"); table->Delete(rk);
    snprintf(key, ksize, "00004066151"); table->Delete(rk);
    snprintf(key, ksize, "00005776783"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002217258"); table->Delete(rk);
    snprintf(key, ksize, "00003742008"); table->Set(rk, rv);
    snprintf(key, ksize, "00000974906"); table->Delete(rk);
    snprintf(key, ksize, "00003873225"); table->Delete(rk);
    snprintf(key, ksize, "00009341811"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008314618"); table->Delete(rk);
    snprintf(key, ksize, "00006352039"); table->Delete(rk);
    snprintf(key, ksize, "00006246582"); table->Delete(rk);
    snprintf(key, ksize, "00005788125"); table->Delete(rk);
    snprintf(key, ksize, "00000294762"); table->Delete(rk);
    snprintf(key, ksize, "00005997065"); table->Set(rk, rv);
    snprintf(key, ksize, "00001439416"); table->Set(rk, rv);
    snprintf(key, ksize, "00007774102"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007353113"); table->Set(rk, rv);
    snprintf(key, ksize, "00006667071"); table->Set(rk, rv);
    snprintf(key, ksize, "00001055763"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001022333"); table->Set(rk, rv);
    snprintf(key, ksize, "00002703205"); table->Set(rk, rv);
    snprintf(key, ksize, "00002877364"); table->Delete(rk);
    snprintf(key, ksize, "00009470010"); table->Set(rk, rv);
    snprintf(key, ksize, "00005069147"); table->Delete(rk);
    snprintf(key, ksize, "00009363492"); table->Set(rk, rv);
    snprintf(key, ksize, "00002946013"); table->Delete(rk);
    snprintf(key, ksize, "00006445198"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002328481"); table->Delete(rk);
    snprintf(key, ksize, "00006293593"); table->Delete(rk);
    snprintf(key, ksize, "00008129969"); table->Delete(rk);
    snprintf(key, ksize, "00000284525"); table->Delete(rk);
    snprintf(key, ksize, "00006175822"); table->Delete(rk);
    snprintf(key, ksize, "00007744324"); table->Set(rk, rv);
    snprintf(key, ksize, "00000767534"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001727221"); table->Set(rk, rv);
    snprintf(key, ksize, "00003597858"); table->Set(rk, rv);
    snprintf(key, ksize, "00003787105"); table->Delete(rk);
    snprintf(key, ksize, "00001006857"); table->Set(rk, rv);
    snprintf(key, ksize, "00008694398"); table->Delete(rk);
    snprintf(key, ksize, "00001041742"); table->Delete(rk);
    snprintf(key, ksize, "00007497189"); table->Set(rk, rv);
    snprintf(key, ksize, "00003667963"); table->Set(rk, rv);
    snprintf(key, ksize, "00002721893"); table->Delete(rk);
    snprintf(key, ksize, "00000682348"); table->Get(rk, tmp);
    snprintf(key, ksize, "00004321991"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006748496"); table->Set(rk, rv);
    snprintf(key, ksize, "00004716388"); table->Set(rk, rv);
    snprintf(key, ksize, "00002244155"); table->Set(rk, rv);
    snprintf(key, ksize, "00005767210"); table->Set(rk, rv);
    snprintf(key, ksize, "00001260799"); table->Delete(rk);
    snprintf(key, ksize, "00004801555"); table->Set(rk, rv);
    snprintf(key, ksize, "00001924858"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001333880"); table->Set(rk, rv);
    snprintf(key, ksize, "00001845774"); table->Set(rk, rv);
    snprintf(key, ksize, "00009007718"); table->Set(rk, rv);
    snprintf(key, ksize, "00008086168"); table->Delete(rk);
    snprintf(key, ksize, "00006875271"); table->Set(rk, rv);
    snprintf(key, ksize, "00000447295"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007750576"); table->Set(rk, rv);
    snprintf(key, ksize, "00008612650"); table->Set(rk, rv);
    snprintf(key, ksize, "00009941960"); table->Delete(rk);
    snprintf(key, ksize, "00006679083"); table->Set(rk, rv);
    snprintf(key, ksize, "00008285463"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007119060"); table->Set(rk, rv);
    snprintf(key, ksize, "00001934927"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001547105"); table->Delete(rk);
    snprintf(key, ksize, "00003238517"); table->Set(rk, rv);
    snprintf(key, ksize, "00005325141"); table->Set(rk, rv);
    snprintf(key, ksize, "00006403678"); table->Delete(rk);
    snprintf(key, ksize, "00004600675"); table->Delete(rk);
    snprintf(key, ksize, "00000058432"); table->Set(rk, rv);
    snprintf(key, ksize, "00001916303"); table->Delete(rk);
    snprintf(key, ksize, "00004440973"); table->Set(rk, rv);
    snprintf(key, ksize, "00006530867"); table->Set(rk, rv);
    snprintf(key, ksize, "00003493241"); table->Delete(rk);
    snprintf(key, ksize, "00004264116"); table->Set(rk, rv);
    snprintf(key, ksize, "00000504663"); table->Set(rk, rv);
    snprintf(key, ksize, "00008093548"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009866437"); table->Delete(rk);
    snprintf(key, ksize, "00002842799"); table->Set(rk, rv);
    snprintf(key, ksize, "00003592467"); table->Set(rk, rv);
    snprintf(key, ksize, "00008537845"); table->Delete(rk);
    snprintf(key, ksize, "00007861867"); table->Set(rk, rv);
    snprintf(key, ksize, "00001403380"); table->Set(rk, rv);
    snprintf(key, ksize, "00001033902"); table->Delete(rk);
    snprintf(key, ksize, "00009676941"); table->Set(rk, rv);
    snprintf(key, ksize, "00008690897"); table->Set(rk, rv);
    snprintf(key, ksize, "00008026037"); table->Set(rk, rv);
    snprintf(key, ksize, "00003956842"); table->Set(rk, rv);
    snprintf(key, ksize, "00005323419"); table->Delete(rk);
    snprintf(key, ksize, "00009700421"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000885283"); table->Set(rk, rv);
    snprintf(key, ksize, "00000530733"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008995209"); table->Set(rk, rv);
    snprintf(key, ksize, "00004191439"); table->Set(rk, rv);
    snprintf(key, ksize, "00002198534"); table->Delete(rk);
    snprintf(key, ksize, "00006227913"); table->Set(rk, rv);
    snprintf(key, ksize, "00004614889"); table->Set(rk, rv);
    snprintf(key, ksize, "00004599365"); table->Delete(rk);
    snprintf(key, ksize, "00008355326"); table->Delete(rk);
    snprintf(key, ksize, "00002022323"); table->Delete(rk);
    snprintf(key, ksize, "00006725602"); table->Set(rk, rv);
    snprintf(key, ksize, "00009625511"); table->Set(rk, rv);
    snprintf(key, ksize, "00003845090"); table->Set(rk, rv);
    snprintf(key, ksize, "00009308543"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001032436"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008756044"); table->Set(rk, rv);
    snprintf(key, ksize, "00009214047"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000896550"); table->Delete(rk);
    snprintf(key, ksize, "00009683947"); table->Delete(rk);
    snprintf(key, ksize, "00000047265"); table->Set(rk, rv);
    snprintf(key, ksize, "00002871895"); table->Delete(rk);
    snprintf(key, ksize, "00002618859"); table->Delete(rk);
    snprintf(key, ksize, "00000364959"); table->Delete(rk);
    snprintf(key, ksize, "00003505053"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002851492"); table->Delete(rk);
    snprintf(key, ksize, "00006752551"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006240596"); table->Delete(rk);
    snprintf(key, ksize, "00004328732"); table->Set(rk, rv);
    snprintf(key, ksize, "00009960415"); table->Set(rk, rv);
    snprintf(key, ksize, "00009254195"); table->Set(rk, rv);
    snprintf(key, ksize, "00002646238"); table->Delete(rk);
    snprintf(key, ksize, "00002910570"); table->Set(rk, rv);
    snprintf(key, ksize, "00007297016"); table->Set(rk, rv);
    snprintf(key, ksize, "00000069542"); table->Delete(rk);
    snprintf(key, ksize, "00008895109"); table->Set(rk, rv);
    snprintf(key, ksize, "00008863443"); table->Set(rk, rv);
    snprintf(key, ksize, "00006391985"); table->Set(rk, rv);
    snprintf(key, ksize, "00009253787"); table->Delete(rk);
    snprintf(key, ksize, "00008700527"); table->Set(rk, rv);
    snprintf(key, ksize, "00005477228"); table->Set(rk, rv);
    snprintf(key, ksize, "00008286216"); table->Set(rk, rv);
    snprintf(key, ksize, "00001129112"); table->Set(rk, rv);
    snprintf(key, ksize, "00008607842"); table->Delete(rk);
    snprintf(key, ksize, "00004612498"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009095119"); table->Set(rk, rv);
    snprintf(key, ksize, "00009561109"); table->Set(rk, rv);
    snprintf(key, ksize, "00001876932"); table->Set(rk, rv);
    snprintf(key, ksize, "00003601643"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006278799"); table->Set(rk, rv);
    snprintf(key, ksize, "00006154910"); table->Delete(rk);
    snprintf(key, ksize, "00003787992"); table->Delete(rk);
    snprintf(key, ksize, "00006940911"); table->Set(rk, rv);
    snprintf(key, ksize, "00006500656"); table->Delete(rk);
    snprintf(key, ksize, "00006936921"); table->Delete(rk);
    snprintf(key, ksize, "00008953537"); table->Set(rk, rv);
    snprintf(key, ksize, "00006753201"); table->Delete(rk);
    snprintf(key, ksize, "00004643929"); table->Delete(rk);
    snprintf(key, ksize, "00001766564"); table->Set(rk, rv);
    snprintf(key, ksize, "00003340158"); table->Delete(rk);
    snprintf(key, ksize, "00001820029"); table->Set(rk, rv);
    snprintf(key, ksize, "00000245755"); table->Set(rk, rv);
    snprintf(key, ksize, "00004176634"); table->Set(rk, rv);
    snprintf(key, ksize, "00003852816"); table->Delete(rk);
    snprintf(key, ksize, "00001296634"); table->Set(rk, rv);
    snprintf(key, ksize, "00001449462"); table->Delete(rk);
    snprintf(key, ksize, "00005305522"); table->Delete(rk);
    snprintf(key, ksize, "00002469897"); table->Set(rk, rv);
    snprintf(key, ksize, "00005417430"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008441130"); table->Set(rk, rv);
    snprintf(key, ksize, "00004171736"); table->Delete(rk);
    snprintf(key, ksize, "00004768500"); table->Set(rk, rv);
    snprintf(key, ksize, "00004634420"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008807251"); table->Delete(rk);
    snprintf(key, ksize, "00002664200"); table->Set(rk, rv);
    snprintf(key, ksize, "00002780046"); table->Set(rk, rv);
    snprintf(key, ksize, "00004582686"); table->Set(rk, rv);
    snprintf(key, ksize, "00005865366"); table->Get(rk, tmp);
    snprintf(key, ksize, "00005148488"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006537597"); table->Delete(rk);
    snprintf(key, ksize, "00009849796"); table->Delete(rk);
    snprintf(key, ksize, "00003896666"); table->Delete(rk);
    snprintf(key, ksize, "00003224512"); table->Delete(rk);
    snprintf(key, ksize, "00007401751"); table->Get(rk, tmp);
    snprintf(key, ksize, "00005337117"); table->Delete(rk);
    snprintf(key, ksize, "00000996293"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003237548"); table->Delete(rk);
    snprintf(key, ksize, "00000433790"); table->Delete(rk);
    snprintf(key, ksize, "00005177222"); table->Get(rk, tmp);
    snprintf(key, ksize, "00004272953"); table->Delete(rk);
    snprintf(key, ksize, "00001388450"); table->Delete(rk);
    snprintf(key, ksize, "00002325651"); table->Delete(rk);
    snprintf(key, ksize, "00000078801"); table->Delete(rk);
    snprintf(key, ksize, "00004730455"); table->Delete(rk);
    snprintf(key, ksize, "00007909198"); table->Set(rk, rv);
    snprintf(key, ksize, "00001672411"); table->Delete(rk);
    snprintf(key, ksize, "00009255111"); table->Delete(rk);
    snprintf(key, ksize, "00002914313"); table->Set(rk, rv);
    snprintf(key, ksize, "00001935641"); table->Set(rk, rv);
    snprintf(key, ksize, "00001121566"); table->Delete(rk);
    snprintf(key, ksize, "00006158945"); table->Set(rk, rv);
    snprintf(key, ksize, "00006781610"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007882653"); table->Delete(rk);
    snprintf(key, ksize, "00007268462"); table->Set(rk, rv);
    snprintf(key, ksize, "00006456441"); table->Set(rk, rv);
    snprintf(key, ksize, "00000901297"); table->Delete(rk);
    snprintf(key, ksize, "00008594414"); table->Set(rk, rv);
    snprintf(key, ksize, "00003816026"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001417964"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003900504"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009814533"); table->Delete(rk);
    snprintf(key, ksize, "00007086156"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001141992"); table->Set(rk, rv);
    snprintf(key, ksize, "00001549267"); table->Set(rk, rv);
    snprintf(key, ksize, "00000316532"); table->Set(rk, rv);
    snprintf(key, ksize, "00000350389"); table->Delete(rk);
    snprintf(key, ksize, "00004788691"); table->Delete(rk);
    snprintf(key, ksize, "00005871970"); table->Set(rk, rv);
    snprintf(key, ksize, "00004347399"); table->Set(rk, rv);
    snprintf(key, ksize, "00005731219"); table->Set(rk, rv);
    snprintf(key, ksize, "00004681846"); table->Delete(rk);
    snprintf(key, ksize, "00008648731"); table->Set(rk, rv);
    snprintf(key, ksize, "00009859463"); table->Set(rk, rv);
    snprintf(key, ksize, "00001949475"); table->Set(rk, rv);
    snprintf(key, ksize, "00001011237"); table->Delete(rk);
    snprintf(key, ksize, "00000604589"); table->Set(rk, rv);
    snprintf(key, ksize, "00001422893"); table->Delete(rk);
    snprintf(key, ksize, "00009895413"); table->Set(rk, rv);
    snprintf(key, ksize, "00007691214"); table->Set(rk, rv);
    snprintf(key, ksize, "00005642517"); table->Delete(rk);
    snprintf(key, ksize, "00004115512"); table->Delete(rk);
    snprintf(key, ksize, "00004483223"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003124906"); table->Set(rk, rv);
    snprintf(key, ksize, "00001575547"); table->Delete(rk);
    snprintf(key, ksize, "00003495625"); table->Delete(rk);
    snprintf(key, ksize, "00008276146"); table->Delete(rk);
    snprintf(key, ksize, "00003939281"); table->Delete(rk);
    snprintf(key, ksize, "00001453731"); table->Set(rk, rv);
    snprintf(key, ksize, "00009389633"); table->Set(rk, rv);
    snprintf(key, ksize, "00005076171"); table->Set(rk, rv);
    snprintf(key, ksize, "00009253185"); table->Delete(rk);
    snprintf(key, ksize, "00001226640"); table->Set(rk, rv);
    snprintf(key, ksize, "00003371505"); table->Set(rk, rv);
    snprintf(key, ksize, "00003240963"); table->Set(rk, rv);
    snprintf(key, ksize, "00002566733"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009070465"); table->Delete(rk);
    snprintf(key, ksize, "00004876388"); table->Set(rk, rv);
    snprintf(key, ksize, "00005586448"); table->Delete(rk);
    snprintf(key, ksize, "00003908880"); table->Delete(rk);
    snprintf(key, ksize, "00005473600"); table->Delete(rk);
    snprintf(key, ksize, "00004468760"); table->Set(rk, rv);
    snprintf(key, ksize, "00005581976"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000674623"); table->Delete(rk);
    snprintf(key, ksize, "00002206785"); table->Set(rk, rv);
    snprintf(key, ksize, "00000438913"); table->Delete(rk);
    snprintf(key, ksize, "00000465127"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002963717"); table->Set(rk, rv);
    snprintf(key, ksize, "00000368176"); table->Delete(rk);
    snprintf(key, ksize, "00002813821"); table->Set(rk, rv);
    snprintf(key, ksize, "00007397940"); table->Delete(rk);
    snprintf(key, ksize, "00002492620"); table->Delete(rk);
    snprintf(key, ksize, "00002062799"); table->Delete(rk);
    snprintf(key, ksize, "00000022092"); table->Delete(rk);
    snprintf(key, ksize, "00005370302"); table->Set(rk, rv);
    snprintf(key, ksize, "00004811241"); table->Set(rk, rv);
    snprintf(key, ksize, "00001335478"); table->Get(rk, tmp);
    snprintf(key, ksize, "00004593647"); table->Delete(rk);
    snprintf(key, ksize, "00007688043"); table->Delete(rk);
    snprintf(key, ksize, "00003953160"); table->Get(rk, tmp);
    snprintf(key, ksize, "00005332526"); table->Set(rk, rv);
    snprintf(key, ksize, "00007177785"); table->Delete(rk);
    snprintf(key, ksize, "00004084171"); table->Set(rk, rv);
    snprintf(key, ksize, "00007489420"); table->Set(rk, rv);
    snprintf(key, ksize, "00006476723"); table->Set(rk, rv);
    snprintf(key, ksize, "00008061486"); table->Set(rk, rv);
    snprintf(key, ksize, "00005683794"); table->Set(rk, rv);
    snprintf(key, ksize, "00000343068"); table->Delete(rk);
    snprintf(key, ksize, "00007923115"); table->Set(rk, rv);
    snprintf(key, ksize, "00005395839"); table->Set(rk, rv);
    snprintf(key, ksize, "00004296133"); table->Set(rk, rv);
    snprintf(key, ksize, "00004137318"); table->Delete(rk);
    snprintf(key, ksize, "00009487082"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002549064"); table->Delete(rk);
    snprintf(key, ksize, "00003999236"); table->Delete(rk);
    snprintf(key, ksize, "00007069953"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000894297"); table->Set(rk, rv);
    snprintf(key, ksize, "00005129414"); table->Set(rk, rv);
    snprintf(key, ksize, "00004389239"); table->Set(rk, rv);
    snprintf(key, ksize, "00009430814"); table->Set(rk, rv);
    snprintf(key, ksize, "00002922074"); table->Delete(rk);
    snprintf(key, ksize, "00004740626"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008046407"); table->Delete(rk);
    snprintf(key, ksize, "00006352461"); table->Delete(rk);
    snprintf(key, ksize, "00005448854"); table->Set(rk, rv);
    snprintf(key, ksize, "00009262938"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001955381"); table->Set(rk, rv);
    snprintf(key, ksize, "00005376598"); table->Set(rk, rv);
    snprintf(key, ksize, "00002131222"); table->Delete(rk);
    snprintf(key, ksize, "00008617587"); table->Delete(rk);
    snprintf(key, ksize, "00004427886"); table->Delete(rk);
    snprintf(key, ksize, "00005462219"); table->Delete(rk);
    snprintf(key, ksize, "00009935277"); table->Set(rk, rv);
    snprintf(key, ksize, "00000300651"); table->Set(rk, rv);
    snprintf(key, ksize, "00003210467"); table->Get(rk, tmp);
    snprintf(key, ksize, "00005190478"); table->Delete(rk);
    snprintf(key, ksize, "00007223766"); table->Get(rk, tmp);
    snprintf(key, ksize, "00004738409"); table->Delete(rk);
    snprintf(key, ksize, "00005014798"); table->Set(rk, rv);
    snprintf(key, ksize, "00001239695"); table->Set(rk, rv);
    snprintf(key, ksize, "00002839170"); table->Set(rk, rv);
    snprintf(key, ksize, "00000208639"); table->Set(rk, rv);
    snprintf(key, ksize, "00003902889"); table->Delete(rk);
    snprintf(key, ksize, "00006237013"); table->Delete(rk);
    snprintf(key, ksize, "00003510899"); table->Set(rk, rv);
    snprintf(key, ksize, "00002164569"); table->Delete(rk);
    snprintf(key, ksize, "00000542140"); table->Delete(rk);
    snprintf(key, ksize, "00003260968"); table->Set(rk, rv);
    snprintf(key, ksize, "00008218421"); table->Set(rk, rv);
    snprintf(key, ksize, "00004801934"); table->Set(rk, rv);
    snprintf(key, ksize, "00003293085"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007561429"); table->Set(rk, rv);
    snprintf(key, ksize, "00009923516"); table->Set(rk, rv);
    snprintf(key, ksize, "00005787025"); table->Set(rk, rv);
    snprintf(key, ksize, "00003390708"); table->Delete(rk);
    snprintf(key, ksize, "00005401965"); table->Delete(rk);
    snprintf(key, ksize, "00007529352"); table->Delete(rk);
    snprintf(key, ksize, "00001024429"); table->Set(rk, rv);
    snprintf(key, ksize, "00001195841"); table->Delete(rk);
    snprintf(key, ksize, "00007466352"); table->Set(rk, rv);
    snprintf(key, ksize, "00002116043"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002645525"); table->Set(rk, rv);
    snprintf(key, ksize, "00007257709"); table->Delete(rk);
    snprintf(key, ksize, "00003135601"); table->Delete(rk);
    snprintf(key, ksize, "00009468114"); table->Delete(rk);
    snprintf(key, ksize, "00006905017"); table->Set(rk, rv);
    snprintf(key, ksize, "00007929946"); table->Set(rk, rv);
    snprintf(key, ksize, "00003278050"); table->Delete(rk);
    snprintf(key, ksize, "00006766284"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002791775"); table->Set(rk, rv);
    snprintf(key, ksize, "00006091794"); table->Delete(rk);
    snprintf(key, ksize, "00006386871"); table->Set(rk, rv);
    snprintf(key, ksize, "00003804345"); table->Delete(rk);
    snprintf(key, ksize, "00005053388"); table->Delete(rk);
    snprintf(key, ksize, "00004672742"); table->Set(rk, rv);
    snprintf(key, ksize, "00009706926"); table->Delete(rk);
    snprintf(key, ksize, "00002150661"); table->Set(rk, rv);
    snprintf(key, ksize, "00009446968"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008258946"); table->Set(rk, rv);
    snprintf(key, ksize, "00004884500"); table->Delete(rk);
    snprintf(key, ksize, "00000520103"); table->Set(rk, rv);
    snprintf(key, ksize, "00000488258"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006255963"); table->Set(rk, rv);
    snprintf(key, ksize, "00006303511"); table->Set(rk, rv);
    snprintf(key, ksize, "00002831384"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003179240"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007289027"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002544325"); table->Set(rk, rv);
    snprintf(key, ksize, "00006977410"); table->Delete(rk);
    snprintf(key, ksize, "00006093558"); table->Set(rk, rv);
    snprintf(key, ksize, "00000117453"); table->Delete(rk);
    snprintf(key, ksize, "00008438936"); table->Set(rk, rv);
    snprintf(key, ksize, "00008152936"); table->Delete(rk);
    snprintf(key, ksize, "00001676484"); table->Delete(rk);
    snprintf(key, ksize, "00000468473"); table->Delete(rk);
    snprintf(key, ksize, "00004138941"); table->Set(rk, rv);
    snprintf(key, ksize, "00008350501"); table->Set(rk, rv);
    snprintf(key, ksize, "00009438626"); table->Set(rk, rv);
    snprintf(key, ksize, "00008393507"); table->Delete(rk);
    snprintf(key, ksize, "00007628713"); table->Set(rk, rv);
    snprintf(key, ksize, "00004837424"); table->Set(rk, rv);
    snprintf(key, ksize, "00000148069"); table->Set(rk, rv);
    snprintf(key, ksize, "00000374063"); table->Set(rk, rv);
    snprintf(key, ksize, "00003219822"); table->Delete(rk);
    snprintf(key, ksize, "00000290106"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009001625"); table->Set(rk, rv);
    snprintf(key, ksize, "00005113864"); table->Delete(rk);
    snprintf(key, ksize, "00002675674"); table->Set(rk, rv);
    snprintf(key, ksize, "00005326466"); table->Set(rk, rv);
    snprintf(key, ksize, "00009678681"); table->Delete(rk);
    snprintf(key, ksize, "00000913142"); table->Set(rk, rv);
    snprintf(key, ksize, "00006783515"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008444820"); table->Delete(rk);
    snprintf(key, ksize, "00003870108"); table->Delete(rk);
    snprintf(key, ksize, "00001964277"); table->Set(rk, rv);
    snprintf(key, ksize, "00008063219"); table->Delete(rk);
    snprintf(key, ksize, "00002306774"); table->Delete(rk);
    snprintf(key, ksize, "00008928152"); table->Set(rk, rv);
    snprintf(key, ksize, "00000903681"); table->Set(rk, rv);
    snprintf(key, ksize, "00000032310"); table->Set(rk, rv);
    snprintf(key, ksize, "00001461354"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001601347"); table->Delete(rk);
    snprintf(key, ksize, "00006474497"); table->Set(rk, rv);
    snprintf(key, ksize, "00009842016"); table->Set(rk, rv);
    snprintf(key, ksize, "00008629179"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007595902"); table->Get(rk, tmp);
    snprintf(key, ksize, "00003338319"); table->Set(rk, rv);
    snprintf(key, ksize, "00008805965"); table->Set(rk, rv);
    snprintf(key, ksize, "00000601557"); table->Set(rk, rv);
    snprintf(key, ksize, "00008621467"); table->Set(rk, rv);
    snprintf(key, ksize, "00004174894"); table->Delete(rk);
    snprintf(key, ksize, "00009367535"); table->Delete(rk);
    snprintf(key, ksize, "00004895973"); table->Delete(rk);
    snprintf(key, ksize, "00009162504"); table->Delete(rk);
    snprintf(key, ksize, "00000443661"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006993442"); table->Set(rk, rv);
    snprintf(key, ksize, "00008228751"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008480232"); table->Set(rk, rv);
    snprintf(key, ksize, "00002871810"); table->Delete(rk);
    snprintf(key, ksize, "00006504211"); table->Set(rk, rv);
    snprintf(key, ksize, "00007842945"); table->Set(rk, rv);
    snprintf(key, ksize, "00001043309"); table->Set(rk, rv);
    snprintf(key, ksize, "00008479529"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002964403"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002525347"); table->Set(rk, rv);
    snprintf(key, ksize, "00001646920"); table->Delete(rk);
    snprintf(key, ksize, "00008270556"); table->Set(rk, rv);
    snprintf(key, ksize, "00003181908"); table->Set(rk, rv);
    snprintf(key, ksize, "00009310142"); table->Set(rk, rv);
    snprintf(key, ksize, "00008966184"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001539228"); table->Delete(rk);
    snprintf(key, ksize, "00001658818"); table->Get(rk, tmp);
    snprintf(key, ksize, "00004554416"); table->Set(rk, rv);
    snprintf(key, ksize, "00004471226"); table->Delete(rk);
    snprintf(key, ksize, "00008461297"); table->Set(rk, rv);
    snprintf(key, ksize, "00008927214"); table->Delete(rk);
    snprintf(key, ksize, "00005075550"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007485507"); table->Set(rk, rv);
    snprintf(key, ksize, "00009819798"); table->Set(rk, rv);
    snprintf(key, ksize, "00002636165"); table->Set(rk, rv);
    snprintf(key, ksize, "00002035780"); table->Set(rk, rv);
    snprintf(key, ksize, "00009045385"); table->Set(rk, rv);
    snprintf(key, ksize, "00005297540"); table->Set(rk, rv);
    snprintf(key, ksize, "00003472866"); table->Set(rk, rv);
    snprintf(key, ksize, "00004571706"); table->Set(rk, rv);
    snprintf(key, ksize, "00004361486"); table->Delete(rk);
    snprintf(key, ksize, "00008398677"); table->Delete(rk);
    snprintf(key, ksize, "00005878912"); table->Set(rk, rv);
    snprintf(key, ksize, "00006159353"); table->Set(rk, rv);
    snprintf(key, ksize, "00000379320"); table->Set(rk, rv);
    snprintf(key, ksize, "00001325576"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000924893"); table->Delete(rk);
    snprintf(key, ksize, "00009277058"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009956205"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008860310"); table->Set(rk, rv);
    snprintf(key, ksize, "00001442000"); table->Set(rk, rv);
    snprintf(key, ksize, "00006875455"); table->Set(rk, rv);
    snprintf(key, ksize, "00001202427"); table->Set(rk, rv);
    snprintf(key, ksize, "00002714679"); table->Set(rk, rv);
    snprintf(key, ksize, "00006780678"); table->Delete(rk);
    snprintf(key, ksize, "00007114344"); table->Set(rk, rv);
    snprintf(key, ksize, "00003397319"); table->Delete(rk);
    snprintf(key, ksize, "00007162468"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008466115"); table->Set(rk, rv);
    snprintf(key, ksize, "00009626369"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007942471"); table->Set(rk, rv);
    snprintf(key, ksize, "00008151968"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007353071"); table->Delete(rk);
    snprintf(key, ksize, "00007277763"); table->Delete(rk);
    snprintf(key, ksize, "00006525881"); table->Delete(rk);
    snprintf(key, ksize, "00009901645"); table->Delete(rk);
    snprintf(key, ksize, "00007034162"); table->Delete(rk);
    snprintf(key, ksize, "00008455267"); table->Delete(rk);
    snprintf(key, ksize, "00008950096"); table->Set(rk, rv);
    snprintf(key, ksize, "00002911882"); table->Delete(rk);
    snprintf(key, ksize, "00008456330"); table->Set(rk, rv);
    snprintf(key, ksize, "00006872938"); table->Set(rk, rv);
    snprintf(key, ksize, "00005539247"); table->Set(rk, rv);
    snprintf(key, ksize, "00001129879"); table->Set(rk, rv);
    snprintf(key, ksize, "00007357227"); table->Set(rk, rv);
    snprintf(key, ksize, "00002850796"); table->Delete(rk);
    snprintf(key, ksize, "00001708193"); table->Set(rk, rv);
    snprintf(key, ksize, "00004167060"); table->Get(rk, tmp);
    snprintf(key, ksize, "00008431790"); table->Set(rk, rv);
    snprintf(key, ksize, "00006370540"); table->Set(rk, rv);
    snprintf(key, ksize, "00007582896"); table->Delete(rk);
    snprintf(key, ksize, "00002925553"); table->Set(rk, rv);
    snprintf(key, ksize, "00003049286"); table->Set(rk, rv);
    snprintf(key, ksize, "00002853647"); table->Set(rk, rv);
    snprintf(key, ksize, "00002550765"); table->Delete(rk);
    snprintf(key, ksize, "00007730156"); table->Set(rk, rv);
    snprintf(key, ksize, "00005791755"); table->Set(rk, rv);
    snprintf(key, ksize, "00002860744"); table->Set(rk, rv);
    snprintf(key, ksize, "00008638500"); table->Set(rk, rv);
    snprintf(key, ksize, "00005336364"); table->Delete(rk);
    snprintf(key, ksize, "00004746872"); table->Delete(rk);
    snprintf(key, ksize, "00001295424"); table->Delete(rk);
    snprintf(key, ksize, "00009189924"); table->Delete(rk);
    snprintf(key, ksize, "00005516329"); table->Delete(rk);
    snprintf(key, ksize, "00006907307"); table->Set(rk, rv);
    snprintf(key, ksize, "00002579386"); table->Set(rk, rv);
    snprintf(key, ksize, "00008159054"); table->Delete(rk);
    snprintf(key, ksize, "00009107261"); table->Set(rk, rv);
    snprintf(key, ksize, "00006885759"); table->Set(rk, rv);
    snprintf(key, ksize, "00003207721"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007726438"); table->Set(rk, rv);
    snprintf(key, ksize, "00000443619"); table->Set(rk, rv);
    snprintf(key, ksize, "00005540971"); table->Set(rk, rv);
    snprintf(key, ksize, "00004849195"); table->Set(rk, rv);
    snprintf(key, ksize, "00007294985"); table->Set(rk, rv);
    snprintf(key, ksize, "00000175198"); table->Set(rk, rv);
    snprintf(key, ksize, "00008372718"); table->Set(rk, rv);
    snprintf(key, ksize, "00008496922"); table->Delete(rk);
    snprintf(key, ksize, "00006933106"); table->Set(rk, rv);
    snprintf(key, ksize, "00005184356"); table->Set(rk, rv);
    snprintf(key, ksize, "00005900119"); table->Delete(rk);
    snprintf(key, ksize, "00008330616"); table->Set(rk, rv);
    snprintf(key, ksize, "00003268680"); table->Delete(rk);
    snprintf(key, ksize, "00005267512"); table->Set(rk, rv);
    snprintf(key, ksize, "00009398786"); table->Get(rk, tmp);
    snprintf(key, ksize, "00009590962"); table->Delete(rk);
    snprintf(key, ksize, "00009413109"); table->Set(rk, rv);
    snprintf(key, ksize, "00000643416"); table->Set(rk, rv);
    snprintf(key, ksize, "00003338943"); table->Delete(rk);
    snprintf(key, ksize, "00009133551"); table->Set(rk, rv);
    snprintf(key, ksize, "00005678169"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002675785"); table->Set(rk, rv);
    snprintf(key, ksize, "00000779371"); table->Set(rk, rv);
    snprintf(key, ksize, "00001613528"); table->Delete(rk);
    snprintf(key, ksize, "00005185957"); table->Delete(rk);
    snprintf(key, ksize, "00001552890"); table->Set(rk, rv);
    snprintf(key, ksize, "00004561625"); table->Get(rk, tmp);
    snprintf(key, ksize, "00005145249"); table->Delete(rk);
    snprintf(key, ksize, "00007321384"); table->Set(rk, rv);
    snprintf(key, ksize, "00007984744"); table->Delete(rk);
    snprintf(key, ksize, "00008887993"); table->Delete(rk);
    snprintf(key, ksize, "00003845392"); table->Delete(rk);
    snprintf(key, ksize, "00007610287"); table->Set(rk, rv);
    snprintf(key, ksize, "00003255182"); table->Set(rk, rv);
    snprintf(key, ksize, "00009981419"); table->Set(rk, rv);
    snprintf(key, ksize, "00001583158"); table->Delete(rk);
    snprintf(key, ksize, "00001697482"); table->Set(rk, rv);
    snprintf(key, ksize, "00009710474"); table->Set(rk, rv);
    snprintf(key, ksize, "00005431649"); table->Set(rk, rv);
    snprintf(key, ksize, "00000189329"); table->Set(rk, rv);
    snprintf(key, ksize, "00008121886"); table->Set(rk, rv);
    snprintf(key, ksize, "00001703681"); table->Set(rk, rv);
    snprintf(key, ksize, "00001625725"); table->Delete(rk);
    snprintf(key, ksize, "00000513816"); table->Get(rk, tmp);
    snprintf(key, ksize, "00007261690"); table->Get(rk, tmp);
    snprintf(key, ksize, "00005667278"); table->Delete(rk);
    snprintf(key, ksize, "00006074265"); table->Get(rk, tmp);
    snprintf(key, ksize, "00004450786"); table->Set(rk, rv);
    snprintf(key, ksize, "00004001478"); table->Delete(rk);
    snprintf(key, ksize, "00004633782"); table->Set(rk, rv);
    snprintf(key, ksize, "00000094701"); table->Delete(rk);
    snprintf(key, ksize, "00009642484"); table->Set(rk, rv);
    snprintf(key, ksize, "00000475882"); table->Get(rk, tmp);
    snprintf(key, ksize, "00004269036"); table->Delete(rk);
    snprintf(key, ksize, "00000676960"); table->Set(rk, rv);
    snprintf(key, ksize, "00006525136"); table->Get(rk, tmp);
    snprintf(key, ksize, "00006200584"); table->Delete(rk);
    snprintf(key, ksize, "00001482357"); table->Delete(rk);
    snprintf(key, ksize, "00005077748"); table->Set(rk, rv);
    snprintf(key, ksize, "00006773417"); table->Set(rk, rv);
    snprintf(key, ksize, "00000494732"); table->Set(rk, rv);
    snprintf(key, ksize, "00008489122"); table->Delete(rk);
    snprintf(key, ksize, "00001953365"); table->Set(rk, rv);
    snprintf(key, ksize, "00000253549"); table->Delete(rk);
    snprintf(key, ksize, "00000645876"); table->Set(rk, rv);
    snprintf(key, ksize, "00009937739"); table->Set(rk, rv);
    snprintf(key, ksize, "00001104271"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002532760"); table->Set(rk, rv);
    snprintf(key, ksize, "00008933181"); table->Delete(rk);
    snprintf(key, ksize, "00007487683"); table->Get(rk, tmp);
    snprintf(key, ksize, "00001260161"); table->Set(rk, rv);
    snprintf(key, ksize, "00008408987"); table->Delete(rk);
    snprintf(key, ksize, "00002241637"); table->Get(rk, tmp);
    snprintf(key, ksize, "00005287054"); table->Delete(rk);
    snprintf(key, ksize, "00001887517"); table->Set(rk, rv);
    snprintf(key, ksize, "00009658823"); table->Set(rk, rv);
    snprintf(key, ksize, "00004501166"); table->Delete(rk);
    snprintf(key, ksize, "00008951246"); table->Delete(rk);
    snprintf(key, ksize, "00001087852"); table->Get(rk, tmp);
    snprintf(key, ksize, "00002409373"); table->Set(rk, rv);
    snprintf(key, ksize, "00004092126"); table->Set(rk, rv);
    snprintf(key, ksize, "00002474305"); table->Delete(rk);
    snprintf(key, ksize, "00001927336"); table->Delete(rk);
    snprintf(key, ksize, "00006776549"); table->Set(rk, rv);
    table->Commit();
    snprintf(key, ksize, "00001808862"); table->Set(rk, rv);
    snprintf(key, ksize, "00000470658"); table->Set(rk, rv);
    snprintf(key, ksize, "00008277052"); table->Get(rk, tmp);
    snprintf(key, ksize, "00000529768"); table->Set(rk, rv);
    snprintf(key, ksize, "00008770989"); table->Delete(rk);
    /* crash */

    return TEST_SUCCESS;
}