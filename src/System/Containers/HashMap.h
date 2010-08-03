#ifndef HASHMAP_H
#define HASHMAP_H

template<class K, class V> class HashMap;

/*
===============================================================================

 HashNode

===============================================================================
*/

template<class K, class V>
class HashNode
{
friend class HashMap<K, V>;
public:

	K&				Key() { return key; }
	V&				Value() { return value; }
	
private:
	HashNode<K,V>*	next;
	K				key;
	V				value;
};

/*
===============================================================================

 HashMap
===============================================================================
*/

template<class K, class V>
class HashMap{
public:
	typedef HashNode<K, V>	Node;

	HashMap(int bucketSize = 16);
	~HashMap();
	
	void					Clear();
	
	V*						Get(K& key);
	V*						Set(K& key, V& value);

	void					Remove(K& key);

private:
	Node**					buckets;
	int						bucketSize;
	int						num;
	
	size_t					GetHash(K& key);
};

/*
===============================================================================
*/

template<class K, class V>
HashMap<K, V>::HashMap(int bucketSize_)
{
	num = 0;
	bucketSize = bucketSize_;
	buckets = new Node*[bucketSize];
	memset(buckets, 0, bucketSize * sizeof(Node*));
}

template<class K, class V>
HashMap<K, V>::~HashMap()
{
	Clear();
	delete[] buckets;
}

template<class K, class V>
void HashMap<K, V>::Clear()
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
V* HashMap<K, V>::Get(K& key)
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
V* HashMap<K, V>::Set(K& key, V& value)
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
void HashMap<K, V>::Remove(K& key)
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
size_t HashMap<K, V>::GetHash(K& key)
{
	return Hash(key) % bucketSize;
}

#endif
