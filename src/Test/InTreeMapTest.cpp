#include "Test.h"

#include "System/Containers/InTreeMap.h"
#include "System/Stopwatch.h"
#include "System/Buffers/Buffer.h"
#include <map>

class KeyValue
{
    typedef InTreeNode<KeyValue> TreeNode;

public: 
    void        SetKey(ReadBuffer key_, bool copy) 
    {
        if (copy == false)
            key = key_;
        else
        {
            keyBuffer.Write(key_);
            key.Wrap(keyBuffer);
        }
    }

    void        SetValue(ReadBuffer value_, bool copy)
    {
        if (copy == false)
            value = value_;
        else
        {
            valueBuffer.Write(value_);
            value.Wrap(valueBuffer);
        }
    }
    
    
    ReadBuffer  key;
    ReadBuffer  value;
    Buffer      keyBuffer;
    Buffer      valueBuffer;
    TreeNode    treeNode;
    
};

static inline int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

//static bool operator< (const ReadBuffer& left, const ReadBuffer& right)
//{
//  return ReadBuffer::LessThan(left, right);
//}

static const ReadBuffer& Key(KeyValue* kv)
{
    return kv->key;
}

TEST_DEFINE(TestInTreeMap)
{
    InTreeMap<KeyValue>                     kvs;
    ReadBuffer                              rb;
    ReadBuffer                              rk;
    ReadBuffer                              rv;
    Buffer                                  buf;
    KeyValue*                               kv;
    KeyValue*                               it;
    Stopwatch                               sw;
    const unsigned                          num = 1000000;
    unsigned                                ksize;
    unsigned                                vsize;
    char*                                   area;
    char*                                   kvarea;
    char*                                   p;
    int                                     len;
    int                                     cmpres;
    std::map<ReadBuffer, ReadBuffer>        bufmap;

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

//      buf.Writef("%u", u);
//      rb.Wrap(buf);
//      kv = new KeyValue;
//      kv->SetKey(rb, true);
//      kv->SetValue(rb, true);

        kv = (KeyValue*) (kvarea + u * sizeof(KeyValue));
        kv->SetKey(rk, false);
        kv->SetValue(rv, false);
        
        sw.Start();
        kvs.Insert(kv);
        TEST_ASSERT(kv->treeNode.IsInTree());
        //bufmap.insert(std::pair<ReadBuffer, ReadBuffer>(rk, rv));
        sw.Stop();
    }
    printf("insert time: %ld\n", (long) sw.Elapsed());

    //return 0;

    sw.Reset();
    sw.Start();
    for (unsigned u = 0; u < num; u++)
    {
        buf.Writef("%u", u);
        rb.Wrap(buf);
        kv = kvs.Get(rb);
        if (kv == NULL)
            TEST_FAIL();
    }
    sw.Stop();      
    printf("get time: %ld\n", (long) sw.Elapsed());

    sw.Reset();
    sw.Start();
    for (it = kvs.First(); it != NULL; it = kvs.Next(it))
    {
        //printf("it->value = %.*s\n", P(&it->value));
        kv = it; // dummy op
    }
    sw.Stop();      
    printf("iteration time: %ld\n", (long) sw.Elapsed());


    sw.Reset();
    sw.Start();
    //for (unsigned u = 0; u < num; u++)
    for (unsigned u = num - 1; u < num; u--)
    {
        buf.Writef("%u", u);
        rb.Wrap(buf);
        kv = kvs.Get(rb);
        if (kv == NULL)
            TEST_FAIL();
        kvs.Remove(kv);
        TEST_ASSERT(!kv->treeNode.IsInTree());
    }
    sw.Stop();      
    printf("delete time: %ld\n", (long) sw.Elapsed());

    sw.Reset();
    sw.Start();
    for (unsigned u = 0; u < num; u++)
    {
        buf.Writef("%u", u);
        rb.Wrap(buf);
        kv = kvs.Get(rb);
        if (kv != NULL)
            TEST_FAIL();
    }
    sw.Stop();
    printf("get time: %ld\n", (long) sw.Elapsed());

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
            TEST_ASSERT(kv->treeNode.IsInTree());
        }
        sw.Stop();
    }
    printf("random insert time: %ld\n", (long) sw.Elapsed());

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
    printf("iteration time: %ld\n", (long) sw.Elapsed());

    if (u != kvs.GetCount())
        return TEST_FAILURE;

    return 0;
}

TEST_DEFINE(TestInTreeMapInsert)
{
    InTreeMap<KeyValue>             kvs;
    ReadBuffer                      rb;
    ReadBuffer                      rk;
    ReadBuffer                      rv;
    Buffer                          buf;
    KeyValue*                       kv;
    KeyValue*                       it;
    char*                           p;
    char*                           area;
    char*                           kvarea;
    int                             ksize;
    int                             vsize;
    int                             num = 2;
    
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));
    kvarea = (char*) malloc(num * sizeof(KeyValue));

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

        kv = (KeyValue*) (kvarea + i * sizeof(KeyValue));
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

TEST_DEFINE(TestInTreeMapMidpoint)
{
    InTreeMap<KeyValue>             kvs;
    ReadBuffer                      rb;
    ReadBuffer                      rk;
    ReadBuffer                      rv;
    ReadBuffer                      mid;
    Buffer                          buf;
    KeyValue*                       kv;
    KeyValue*                       it;
    char*                           p;
    char*                           area;
    char*                           kvarea;
    int                             ksize;
    int                             vsize;
    int                             num = 5;
    
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));
    kvarea = (char*) malloc(num * sizeof(KeyValue));

    for (int i = 0; i < num; i++)
    {
        p = area + i*(ksize+vsize);
        rk.SetBuffer(p);
        rk.SetLength(ksize);
        snprintf(p, ksize, "%019d", i);
        p += ksize;
        rv.SetBuffer(p);
        rv.SetLength(vsize);
        snprintf(p, vsize, "value");

        kv = (KeyValue*) (kvarea + i * sizeof(KeyValue));
        kv->SetKey(rk, false);
        kv->SetValue(rv, false);
        it = kvs.Insert(kv);
        if (it != NULL)
            TEST_FAIL();
    }

    mid = kvs.Mid()->key;
    TEST_LOG("Mid key: %.*s", mid.GetLength(), mid.GetBuffer());

    free(area);
    free(kvarea);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestInTreeMapInsertRandom)
{
    InTreeMap<KeyValue>             kvs;
    ReadBuffer                      rb;
    ReadBuffer                      rk;
    ReadBuffer                      rv;
    Buffer                          buf;
    Stopwatch                       sw;
    KeyValue*                       kv;
    KeyValue*                       it;
    char*                           p;
    char*                           area;
    char*                           kvarea;
    int                             ksize;
    int                             vsize;
    int                             num = 100000;
    
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));
    kvarea = (char*) malloc(num * sizeof(KeyValue));

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

        kv = (KeyValue*) (kvarea + i * sizeof(KeyValue));
        kv->SetKey(rk, true);
        kv->SetValue(rv, true);
        sw.Start();
        it = kvs.Insert(kv);
        sw.Stop();
    }
    
    printf("random insert time: %ld\n", (long) sw.Elapsed());

    free(area);
    free(kvarea);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestInTreeMapRemoveRandom)
{
    InTreeMap<KeyValue>             kvs;
    ReadBuffer                      rb;
    ReadBuffer                      rk;
    ReadBuffer                      rv;
    Buffer                          buf;
    Stopwatch                       sw;
    KeyValue*                       kv;
    KeyValue*                       it;
    char*                           p;
    char*                           area;
    char*                           kvarea;
    int                             ksize;
    int                             vsize;
    int                             len;
    const int                       num = 10000;
    int                             numbers[num];
    
    ksize = 20;
    vsize = 128;
    area = (char*) malloc(num*(ksize+vsize));
    kvarea = (char*) malloc(num * sizeof(KeyValue));

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

        kv = (KeyValue*) (kvarea + i * sizeof(KeyValue));
        kv->SetKey(rk, true);
        kv->SetValue(rv, true);
        sw.Start();
        it = kvs.Insert(kv);
        sw.Stop();
    }
    
    printf("insert time: %ld\n", (long) sw.Elapsed());

    // check if the elements are all in order
    int count = 0;
    for (it = kvs.First(); it != NULL; it = kvs.Next(it))
    {
        KeyValue* next;
        
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
        kv = (KeyValue*) (kvarea + j * sizeof(KeyValue));
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
