#ifndef INHASHMAP_H
#define INHASHMAP_H

#include "InList.h"

#define IN_HASHMAP(node) (node->next != node)

/*
===============================================================================================

 InHashMap
 
===============================================================================================
*/

template<class T, class K>
class InHashMap
{
public:
    InHashMap(int bucketSize = 16);
    ~InHashMap();
    
    void                    Clear();
    
    T*                      Get(K& key);
    void                    Set(T* node);
    void                    Remove(T* node);

private:
    size_t                  GetHash(K& key);

    InList<T>*              buckets;
    size_t                  bucketSize;
    size_t                  num;
};

/*
===============================================================================================
*/

template<class T, class K>
InHashMap<T, K>::InHashMap(int bucketSize_)
{
    num = 0;
    bucketSize = bucketSize_;
    buckets = new InList<T>[bucketSize];
    memset(buckets, 0, bucketSize * sizeof(InList<T>));
}

template<class T, class K>
InHashMap<T, K>::~InHashMap()
{
    Clear();
    delete[] buckets;
}

template<class T, class K>
void InHashMap<T, K>::Clear()
{
    size_t	    i;
    T*          node;
    T*          next;
    
    for (i = 0; i < bucketSize; i++)
    {
        InList<T>& bucket = buckets[i];
        for (node = bucket.First(); node != NULL; node = next)
        {
            next = node->next;
            delete node;
        }
    }
    num = 0;
}

template<class T, class K>
T* InHashMap<T, K>::Get(K& key)
{
    size_t  hash;
    T*      node;
    
    hash = GetHash(key);
    InList<T>& bucket = buckets[hash];
    for (node = bucket.First(); node != NULL; node = node->next)
    {
        if (node->key == key)
            return node;
    }
    
    return NULL;
}

template<class T, class K>
void InHashMap<T, K>::Set(T* node)
{
    size_t  hash;
    T*      it;
    
    hash = GetHash(node->key);
    InList<T>& bucket = buckets[hash];
    for (it = bucket.First(); it != NULL; it = it->next)
    {
        if (it == node)
            ASSERT_FAIL();
        if (it->key == node->key)
            ASSERT_FAIL();
    }

    bucket.Append(node);
    num++;
}

template<class T, class K>
void InHashMap<T, K>::Remove(T* node)
{
    size_t  hash;
    T*      it;
    
    hash = GetHash(node->key);
    InList<T>& bucket = buckets[hash];
    for (it = bucket.First(); it != NULL; it = it->next)
    {
        if (it->key == node->key)
            ASSERT(it == node);
        if (it == node)
        {
            bucket.Remove(node);
            num--;
            return;
        }
    }

    ASSERT_FAIL();
}

template<class T, class K>
size_t InHashMap<T, K>::GetHash(K& key)
{
    return Hash(key) % bucketSize;
}

#endif