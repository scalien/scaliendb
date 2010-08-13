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
	
	V*						GetPtr(K& key);
	bool					Get(K& key, V& value);
	void					Set(K& key, V& value);

	bool					HasKey(K& key);
	void					Remove(K& key);

	Node*					First();
	Node*					Next(Node* it);

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
V* HashMap<K, V>::GetPtr(K& key)
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
bool HashMap<K, V>::Get(K& key, V& value)
{
	size_t	hash;
	Node*	node;
	
	hash = GetHash(key);
	for (node = buckets[hash]; node; node = node->next)
	{
		if (node->key == key)
		{
			value = node->value;
			return true;
		}
	}
	
	return false;
}

template<class K, class V>
void HashMap<K, V>::Set(K& key, V& value)
{
	size_t	hash;
	Node*	node;
	
	hash = GetHash(key);
	for (node = buckets[hash]; node; node = node->next)
	{
		if (node->key == key)
		{
			node->value = value;
			return;
		}
	}
	
	node = new Node;
	node->key = key;
	node->value = value;
	
	node->next = buckets[hash];
	buckets[hash] = node;
	num++;
	
	// TODO resize if too small
	return;
}

template<class K, class V>
bool HashMap<K, V>::HasKey(K& key)
{
	if (GetPtr(key))
		return true;
		
	return false;
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
HashNode<K, V>* HashMap<K, V>::First()
{
	int		i;
	
	for (i = 0; i < bucketSize; i++)
	{
		if (buckets[i])
			return buckets[i];
	}
	
	return NULL;
}

template<class K, class V>
HashNode<K, V>* HashMap<K, V>::Next(Node* it)
{
	int		i;
	size_t	hash;
	
	if (it->next)
		return it->next;

	hash = GetHash(it->key);
	for (i = hash + 1; i < bucketSize; i++)
	{
		if (buckets[i])
			return buckets[i];
	}
	
	return NULL;
}

template<class K, class V>
size_t HashMap<K, V>::GetHash(K& key)
{
	return Hash(key) % bucketSize;
}

#endif
