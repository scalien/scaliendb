#include "StorageDataPage.h"
#include "StorageFileChunk.h"
#include "System/Containers/InList.h"
#include "System/Threading/Mutex.h"

#define STORAGE_DATAPAGE_HEADER_SIZE        16

static Mutex dataPageCacheMutex;
static InHashMap<StorageDataPageCacheNode, StorageDataPageCacheKey> hashMap(16*1000);
static InList<StorageDataPage> freeList;
static int64_t cacheSize = 0;
static int64_t maxCacheSize = 0;
static int64_t maxUsedSize = 0;
static uint32_t largestSeen = 0;
static uint64_t numCacheHit = 0;
static uint64_t numCacheMissPoolHit = 0;
static uint64_t numCacheMissPoolMiss = 0;

StorageDataPageCacheNode::StorageDataPageCacheNode(StorageDataPage* dataPage_)
{
    prev = next = this;
    dataPage = dataPage_;
    ASSERT(dataPage->cacheNode == NULL);
    dataPage->cacheNode = this;
    key.chunkID = 0;
    key.index = 0;
    numAcquired = 0;
}

void StorageDataPageCache::Shutdown()
{
    DataPage*   dataPage;

    MutexGuard mutexGuard(dataPageCacheMutex);

    FOREACH_REMOVE (dataPage, freeList)
    {
        ASSERT(dataPage->cacheNode->dataPage == dataPage);
        ASSERT(dataPage->cacheNode->numAcquired == 0);
        ASSERT(IN_HASHMAP(dataPage->cacheNode));
        hashMap.Remove(dataPage->cacheNode);
        delete dataPage->cacheNode;
        delete dataPage;
    }

    ASSERT(hashMap.GetCount() == 0);
}

void StorageDataPageCache::SetMaxCacheSize(uint64_t maxCacheSize_)
{
    maxCacheSize = maxCacheSize_;
}

uint64_t StorageDataPageCache::GetMaxCacheSize()
{
    return maxCacheSize;
}

uint64_t StorageDataPageCache::GetCacheSize()
{
    return cacheSize;
}

uint64_t StorageDataPageCache::GetMaxUsedSize()
{
    return maxUsedSize;
}

uint32_t StorageDataPageCache::GetLargestSeen()
{
    return largestSeen;
}

unsigned StorageDataPageCache::GetFreeListLength()
{
    return freeList.GetLength();
}

uint64_t StorageDataPageCache::GetNumCacheHit()
{
    return numCacheHit;
}

uint64_t StorageDataPageCache::GetNumCacheMissPoolHit()
{
    return numCacheMissPoolHit;
}

uint64_t StorageDataPageCache::GetNumCacheMissPoolMiss()
{
    return numCacheMissPoolMiss;
}

StorageDataPage* StorageDataPageCache::Acquire(uint64_t chunkID, uint32_t index)
{
    CacheKey    key;
    CacheNode*  node;
    DataPage*   dataPage;

    MutexGuard mutexGuard(dataPageCacheMutex);

    Log_Debug("Acquire: cacheSize: %s", HUMAN_BYTES(cacheSize));

    ASSERT(maxCacheSize > 0);

    key.chunkID = chunkID;
    key.index = index;
    node = hashMap.Get(key);

    if (node)
    {
        // data page is preloaded, return it
        Log_Debug("Acquire: Returning preloaded data page from chunk %U index %u numAcquired %d",
         node->key.chunkID, node->key.index, node->numAcquired);
        ASSERT(node->numAcquired >= 0);
        ASSERT(node->dataPage->cacheNode == node);
        if (node->numAcquired == 0)
        {
            // remove from free list
            ASSERT(IN_LIST(node->dataPage));
            freeList.Remove(node->dataPage);
        }
        else
        {
            // it's already out, not on the free list
            ASSERT(!IN_LIST(node->dataPage));
        }
        node->numAcquired++;
        numCacheHit++;
        return node->dataPage;
    }

    // data page is not preloaded
    if (freeList.GetLength() > 0 && maxUsedSize >= maxCacheSize)
    {
        Log_Debug("Acquire: Returning a data page from the free list");

        // return a data page from the free list
        // do _not_ add to the hashMap here
        // this will happen when it is Release()'d
        // since the dataPage will be empty until it's actually loaded
        dataPage = freeList.Pop();
        ASSERT(dataPage->cacheNode != NULL);
        node = dataPage->cacheNode;
        ASSERT(node->dataPage == dataPage);
        ASSERT(node->numAcquired == 0);

        // if in hashmap then remove, since we are changing the index values
        if (IN_HASHMAP(node))
            hashMap.Remove(node);
        numCacheMissPoolHit++;
    }
    else
    {
        Log_Debug("Acquire: Allocating new data page");
        // allocate a new node and data page
        node = new CacheNode(new DataPage());
        cacheSize += node->dataPage->GetMemorySize();
        if (cacheSize > maxUsedSize)
            maxUsedSize = cacheSize;
        ASSERT(node->numAcquired == 0);
        ASSERT(node->dataPage->cacheNode == node);
        if (maxUsedSize >= maxCacheSize)
            numCacheMissPoolMiss++;
    }

    node->numAcquired++;
    return node->dataPage;
}

void StorageDataPageCache::Release(StorageDataPage* dataPage)
{
    CacheNode*  node;

    MutexGuard mutexGuard(dataPageCacheMutex);

    Log_Debug("Release: cacheSize: %s", HUMAN_BYTES(cacheSize));

    ASSERT(maxCacheSize > 0);

    if (dataPage->GetMemorySize() > largestSeen)
        largestSeen = dataPage->GetMemorySize();

    ASSERT(dataPage->cacheNode != NULL);
    node = dataPage->cacheNode;
    ASSERT(node->dataPage == dataPage);

    ASSERT(node->numAcquired > 0);
    node->numAcquired--;

    if (node->numAcquired == 0)
    {
        // node is not being used by the application
        ASSERT(!IN_LIST(node->dataPage));
        if (cacheSize < maxCacheSize)
        {
            Log_Debug("Release: returning data page to the free list; chunk %U index %u numAcquired %d",
             node->key.chunkID, node->key.index, node->numAcquired);
            // we have room, append it to freelist
            freeList.Append(dataPage);
        }
        else
        {
            Log_Debug("Release: deleting data page; %U index %u numAcquired %d",
             node->key.chunkID, node->key.index, node->numAcquired);
            // we have no room, get rid of it
            if (IN_HASHMAP(node))
                hashMap.Remove(node);
            cacheSize -= dataPage->GetMemorySize();
            ASSERT(cacheSize >= 0);
            delete node;
            delete dataPage;
            return;
        }
    }

    if (!hashMap.Get(node->key))
    {
        ASSERT(!IN_HASHMAP(node));
        hashMap.Set(node);
    }
}

void StorageDataPageCache::UpdateDataPageSize(uint32_t oldSize, StorageDataPage* dataPage)
{
    MutexGuard mutexGuard(dataPageCacheMutex);

    cacheSize -= oldSize;
    ASSERT(cacheSize >= 0);
    cacheSize += dataPage->GetMemorySize();
    if (cacheSize > maxUsedSize)
        maxUsedSize = cacheSize;
    
}

StorageDataPage::StorageDataPage()
{
    prev = next = this;
    cacheNode = NULL;
}

StorageDataPage::StorageDataPage(StorageFileChunk* owner_, uint32_t index_, unsigned bufferSize)
{
    prev = next = this;
    Init(owner_, index_, bufferSize);
}

StorageDataPage::~StorageDataPage()
{
}

void StorageDataPage::Init(StorageFileChunk* owner_, uint32_t index_, unsigned bufferSize)
{
    size = 0;
    compressedSize = 0;

    keysBuffer.SetLength(0);
    valuesBuffer.SetLength(0);

    buffer.Allocate(bufferSize);
    buffer.Zero();
    buffer.SetLength(0);

    buffer.AppendLittle32(0); // dummy for size
    buffer.AppendLittle32(0); // dummy for checksum
    buffer.AppendLittle32(0); // dummy for keysSize
    buffer.AppendLittle32(0); // dummy for numKeys
    
    storageFileKeyValueBuffer.SetLength(0);

    owner = owner_;
    index = index_;
}

void StorageDataPage::SetOwner(StorageFileChunk* owner_)
{
    ASSERT(owner == NULL);
    owner = owner_;
}

uint32_t StorageDataPage::GetSize()
{
    return size;
}

uint32_t StorageDataPage::GetMemorySize()
{
    return buffer.GetSize() + keysBuffer.GetSize() + 
        valuesBuffer.GetSize() + storageFileKeyValueBuffer.GetSize();
}

uint32_t StorageDataPage::GetCompressedSize()
{
    return compressedSize;
}

uint32_t StorageDataPage::GetPageBufferSize()
{
    return buffer.GetLength() + keysBuffer.GetLength() + valuesBuffer.GetLength();
}

StorageKeyValue* StorageDataPage::Get(ReadBuffer& key)
{
    StorageFileKeyValue* it;

    int cmpres;
    
    if (GetNumKeys() == 0)
        return NULL;
        
    it = LocateKeyValue(key, cmpres);
    if (cmpres != 0)
        return NULL;

    return it;
}

uint32_t StorageDataPage::GetNumKeys()
{
    return storageFileKeyValueBuffer.GetLength() / sizeof(StorageFileKeyValue);
}

uint32_t StorageDataPage::GetLength()
{
    return STORAGE_DATAPAGE_HEADER_SIZE + keysBuffer.GetLength() + valuesBuffer.GetLength();
}

uint32_t StorageDataPage::GetIncrement(StorageKeyValue* kv)
{
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        return (1 + 2 + kv->GetKey().GetLength() + 4 + kv->GetValue().GetLength());
    else if (kv->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
        return (1 + 2 + kv->GetKey().GetLength());
    else
        ASSERT_FAIL();

    return 0;
}

uint32_t StorageDataPage::GetIndex()
{
    return index;
}

void StorageDataPage::Append(StorageKeyValue* kv, bool keysOnly)
{
    StorageFileKeyValue fkv;

    ASSERT(kv->GetKey().GetLength() > 0);

    keysBuffer.Append(kv->GetType());                               // 1 byte(s)
    keysBuffer.AppendLittle16(kv->GetKey().GetLength());            // 2 byte(s)
    keysBuffer.Append(kv->GetKey());
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET && keysOnly == false)
    {
        valuesBuffer.AppendLittle32(kv->GetValue().GetLength());    // 4 bytes(s)
        valuesBuffer.Append(kv->GetValue());
    }

    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
    {
        if (keysOnly)
            fkv.Set(ReadBuffer(kv->GetKey()), ReadBuffer());
        else
            fkv.Set(ReadBuffer(kv->GetKey()), ReadBuffer(kv->GetValue()));
    }
    else
        fkv.Delete(ReadBuffer(kv->GetKey()));
    
    AppendKeyValue(fkv);
}

void StorageDataPage::Finalize()
{
    uint32_t                div, mod, numKeys, checksum, length, klen, vlen, kit, vit;
    char                    *kpos, *vpos;
    ReadBuffer              dataPart;
    StorageFileKeyValue*    it;
    
    numKeys = GetNumKeys();
    buffer.Append(keysBuffer);
    buffer.Append(valuesBuffer);
    length = buffer.GetLength();

    div = length / STORAGE_DEFAULT_PAGE_GRAN;
    mod = length % STORAGE_DEFAULT_PAGE_GRAN;
    size = div * STORAGE_DEFAULT_PAGE_GRAN;
    if (mod > 0)
        size += STORAGE_DEFAULT_PAGE_GRAN;

    buffer.Allocate(size);
    buffer.ZeroRest();

    // write keysSize
    buffer.SetLength(8);
    buffer.AppendLittle32(keysBuffer.GetLength());

    // write numKeys
    buffer.SetLength(12);
    buffer.AppendLittle32(numKeys);

    // compute checksum
//    dataPart.Wrap(buffer.GetBuffer() + 8, size - 8);
//    checksum = dataPart.GetChecksum();
    checksum = 0;

    buffer.SetLength(0);
    buffer.AppendLittle32(size);
    buffer.AppendLittle32(checksum);

    buffer.SetLength(size);
    
    // set ReadBuffers in tree
    kit = STORAGE_DATAPAGE_HEADER_SIZE;
    vit = STORAGE_DATAPAGE_HEADER_SIZE + keysBuffer.GetLength();
    for (unsigned i = 0; i < numKeys; i++)
    {
        it = GetIndexedKeyValue(i);
        ASSERT(it->GetKey().GetLength() > 0);
        
        kit += 1;                                                // type
        if (it->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            klen = it->GetKey().GetLength();
            vlen = it->GetValue().GetLength();
            
            kit += 2;                                            // keylen
            kpos = buffer.GetBuffer() + kit;
            kit += klen;
            vit += 4;                                            // vlen
            vpos = buffer.GetBuffer() + vit;
            vit += vlen;
            
            it->Set(ReadBuffer(kpos, klen), ReadBuffer(vpos, vlen));
        }
        else
        {
            klen = it->GetKey().GetLength();
            
            kit += 2;                                            // keylen
            kpos = buffer.GetBuffer() + kit;
            kit += klen;
            
            it->Delete(ReadBuffer(kpos, klen));
        }
    }
    
#ifdef STORAGE_DATAPAGE_COMPRESSION
    Buffer                  compressed;
    Compressor              compressor;
    
    compressor.Compress(buffer, compressed);
    buffer.SetLength(0);
    buffer.AppendLittle32(0);
    buffer.AppendLittle32(0);  // checksum
    buffer.AppendLittle32(keysBuffer.GetLength());
    buffer.AppendLittle32(buffer.GetLength());
    buffer.Append(compressed);
    
    length = buffer.GetLength();
    compressedSize = compressed.GetLength();
    buffer.SetLength(0);
    buffer.AppendLittle32(length);
    buffer.SetLength(length);
#else
    compressedSize = size;
#endif

    keysBuffer.Reset();
    valuesBuffer.Reset();
}

void StorageDataPage::Reset()
{
    storageFileKeyValueBuffer.Reset();
    
    keysBuffer.Reset();
    valuesBuffer.Reset();
    buffer.Reset();
    
    buffer.AppendLittle32(0); // dummy for size
    buffer.AppendLittle32(0); // dummy for checksum
    buffer.AppendLittle32(0); // dummy for keysLength
    buffer.AppendLittle32(0); // dummy for numKeys
}

StorageFileKeyValue* StorageDataPage::First()
{
    return GetIndexedKeyValue(0);
}

StorageFileKeyValue* StorageDataPage::Last()
{
    return GetIndexedKeyValue(GetNumKeys() - 1);
}

StorageFileKeyValue* StorageDataPage::Next(StorageFileKeyValue* it)
{
    unsigned index;

    index = ((char*) it - storageFileKeyValueBuffer.GetBuffer()) / sizeof(StorageFileKeyValue);
    return GetIndexedKeyValue(index + 1);
}

StorageFileKeyValue* StorageDataPage::Prev(StorageFileKeyValue* it)
{
    unsigned index;

    index = ((char*) it - storageFileKeyValueBuffer.GetBuffer()) / sizeof(StorageFileKeyValue);
    return GetIndexedKeyValue(index - 1);
}

StorageFileKeyValue* StorageDataPage::GetIndexedKeyValue(unsigned index)
{
    if (index >= (storageFileKeyValueBuffer.GetLength() / sizeof(StorageFileKeyValue)))
        return NULL;
    return (StorageFileKeyValue*) (storageFileKeyValueBuffer.GetBuffer() + index * sizeof(StorageFileKeyValue));
}

StorageFileKeyValue* StorageDataPage::LocateKeyValue(ReadBuffer& key, int& cmpres)
{
    StorageFileKeyValue*    kvIndex;
    unsigned                first;
    unsigned                last;
    unsigned                numKeys;
    unsigned                mid;

    cmpres = 0;
    numKeys = GetNumKeys();
    if (numKeys == 0)
        return NULL;
    
    kvIndex = (StorageFileKeyValue*) storageFileKeyValueBuffer.GetBuffer();
    
    if (key.GetLength() == 0)
    {
        cmpres = -1;
        return GetIndexedKeyValue(0);
    }
    
    first = 0;
    last = numKeys - 1;
    while (first <= last)
    {
        mid = first + ((last - first) / 2);
        cmpres = ReadBuffer::Cmp(key, kvIndex[mid].GetKey());
        if (cmpres == 0)
            return &kvIndex[mid];
        
        if (cmpres < 0)
        {
            if (mid == 0)
                return &kvIndex[mid];

            last = mid - 1;
        }
        else 
            first = mid + 1;
    }
    
    // not found
    return &kvIndex[mid];
}

bool StorageDataPage::Read(Buffer& buffer_, bool keysOnly)
{
    char                    type;
    uint16_t                klen;
    uint32_t                size, /*checksum, compChecksum,*/ numKeys, vlen, i, keysSize;
    ReadBuffer              dataPart, parse, kparse, vparse, key, value;
    StorageFileKeyValue     fkv;
    
#ifdef STORAGE_DATAPAGE_COMPRESSION    
    Compressor              compressor;
    uint32_t                uncompressedSize;

    Key - value seperation not implemented!

    parse.Wrap(buffer_);
    parse.ReadLittle32(compressedSize);
    parse.Advance(sizeof(uint32_t));
    parse.ReadLittle32(uncompressedSize);
    parse.Advance(sizeof(uint32_t));
    compressor.Uncompress(parse, buffer, uncompressedSize);
#else
    ASSERT(GetNumKeys() == 0);
    buffer.Write(buffer_);
#endif
    parse.Wrap(buffer);
    
    // size
    parse.ReadLittle32(size);
    if (size < 12)
        goto Fail;
    if (!keysOnly)
        if (buffer.GetLength() != size)
            goto Fail;
    parse.Advance(4);

    // checksum
//    parse.ReadLittle32(checksum);
//    dataPart.Wrap(buffer.GetBuffer() + 8, buffer.GetLength() - 8);
//    compChecksum = dataPart.GetChecksum();
//    if (compChecksum != checksum)
//        goto Fail;
    parse.Advance(4);

    // keysSize
    parse.ReadLittle32(keysSize);
    parse.Advance(4);
    
    // numkeys
    parse.ReadLittle32(numKeys);
    parse.Advance(4);

    // preallocate keyValue buffer
    storageFileKeyValueBuffer.Allocate(numKeys * sizeof(StorageFileKeyValue));

    // keys
    kparse = parse;
    if (!keysOnly)
    {
        vparse = parse;
        vparse.Advance(keysSize);
    }
    for (i = 0; i < numKeys; i++)
    {
        // type
        if (!kparse.ReadChar(type))
            goto Fail;
        if (type != STORAGE_KEYVALUE_TYPE_SET && type != STORAGE_KEYVALUE_TYPE_DELETE)
            goto Fail;
        kparse.Advance(1);

        // klen
        if (!kparse.ReadLittle16(klen))
            goto Fail;
        kparse.Advance(2);
        ASSERT(klen > 0);
        
        // key
        if (kparse.GetLength() < klen)
            goto Fail;
        key.Wrap(kparse.GetBuffer(), klen);
        kparse.Advance(klen);

        if (type == STORAGE_KEYVALUE_TYPE_SET)
        {
            if (!keysOnly)
            {
                // vlen
                if (!vparse.ReadLittle32(vlen))
                    goto Fail;
                vparse.Advance(4);
                
                // value
                if (vparse.GetLength() < vlen)
                    goto Fail;
                value.Wrap(vparse.GetBuffer(), vlen);
                vparse.Advance(vlen);
            }
            else
                value.Reset();

            fkv.Set(key, value);
            AppendKeyValue(fkv);
        }
        else
        {
            fkv.Delete(key);
            AppendKeyValue(fkv);
        }
    }

#ifdef STORAGE_DATAPAGE_COMPRESSION
    this->size = uncompressedSize;
#else    
    this->size = size;
    this->compressedSize = size;
#endif
    return true;
    
Fail:
    storageFileKeyValueBuffer.Reset();
    buffer.Reset();
    return false;
}

void StorageDataPage::Write(Buffer& buffer_)
{
    buffer_.Write(buffer);
}

unsigned StorageDataPage::Serialize(Buffer& buffer_)
{
    buffer_.Append(buffer);
    return buffer.GetLength();
}

void StorageDataPage::Unload()
{
    Reset();
    owner->OnDataPageEvicted(index);
}

void StorageDataPage::AppendKeyValue(StorageFileKeyValue& kv)
{
    ASSERT(kv.GetKey().GetLength() > 0);
    storageFileKeyValueBuffer.Append((const char*) &kv, sizeof(StorageFileKeyValue));
}

static size_t Hash(StorageDataPageCacheKey& key)
{
    uint64_t hash;
    
    hash = 23;
    hash = hash * 31 + key.chunkID;
    hash = hash * 31 + key.index;

    return hash;
}
