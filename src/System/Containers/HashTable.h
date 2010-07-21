#ifndef HASH_TABLE_H
#define HASH_TABLE_H

template<class K, class V> class HashTable;

/*
===============================================================================

 HashNode

===============================================================================
*/

template<class K, class V>
class HashNode
{
friend class HashTable<K, V>;
public:

	const K&		Key() const { return key; }
	V&				Value() const { return value; }
	
private:
	HashNode<K,V>*	next;
	K				key;
	V				value;
};

/*
===============================================================================

 HashTable

===============================================================================
*/

template<class K, class V>
class HashTable
{
public:
	typedef HashNode<K, V>	Node;

	HashTable(int bucketSize = 16);
	~HashTable();
	
	void					Clear();
	
	V*						Get(const K& key);
	V*						Set(const K& key, const V& value);

	void					Remove(const K& key);

private:
	Node**					buckets;
	int						bucketSize;
	int						num;
	
	size_t					GetHash(const K& key);
};

/*
===============================================================================
*/

template<class K, class V>
HashTable<K, V>::HashTable(int bucketSize_)
{
	num = 0;
	bucketSize = bucketSize_;
	buckets = new Node*[bucketSize];
	memset(buckets, 0, bucketSize * sizeof(Node*));
}

template<class K, class V>
HashTable<K, V>::~HashTable()
{
	Clear();
	delete[] buckets;
}

template<class K, class V>
void HashTable<K, V>::Clear()
{
	int		i;
	Node*	node;
	Node*	next;
	
	for (i = 0; i < bucketSize; i++)
	{
		for (node = buckets[i]; node; node = next)
		{
			next = node->next;
			delete node;
		}
		buckets[i] = NULL;
	}
	num = 0;
}

template<class K, class V>
V* HashTable<K, V>::Get(const K& key)
{
	size_t	hash;
	Node*	node;
	
	hash = GetHash(key);
	for (node = buckets[hash]; node; node = node->next)
	{
		if (node->key == key)
			return &node->value;
	}
	
	return NULL;
}

template<class K, class V>
V* HashTable<K, V>::Set(const K& key, const V& value)
{
	size_t	hash;
	Node*	node;
	
	hash = GetHash(key);
	for (node = buckets[hash]; node; node = node->next)
	{
		if (node->key == key)
		{
			node->value = value;
			return &node->value;
		}
	}
	
	node = new Node;
	node->key = key;
	node->value = value;
	
	node->next = buckets[hash];
	buckets[hash] = node;
	num++;
	
	// TODO resize if too small
	return &node->value;
}

template<class K, class V>
void HashTable<K, V>::Remove(const K& key)
{
	size_t	hash;
	Node*	node;
	Node*	next;
	Node**	prev;
	
	hash = GetHash(key);
	prev = &buckets[hash];
	for (node = buckets[hash]; node; node = next)
	{
		next = node->next;
		if (node->key == key)
		{
			*prev = next;
			delete node;
			num--;
			break;
		}
		prev = &node->next;
	}
	
	// TODO resize if too big
}

template<class K, class V>
size_t HashTable<K, V>::GetHash(const K& key)
{
	return Hash(key) & (bucketSize - 1);
}

#endif
