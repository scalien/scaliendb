#ifndef STORAGESHARDINDEX_H
#define STORAGESHARDINDEX_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"

class StorageShard;	// forward

/*
===============================================================================

 StorageShardIndex

===============================================================================
*/

class StorageShardIndex
{
public:
	typedef InTreeNode<StorageShardIndex>	ShardTreeNode;

	StorageShardIndex();
	~StorageShardIndex();

	void					SetStartKey(ReadBuffer key, bool copy);

	ReadBuffer				startKey;
	Buffer*					startKeyBuffer;
	StorageShard*			shard;
	uint64_t				shardID;

	ShardTreeNode			treeNode;
};

#endif
