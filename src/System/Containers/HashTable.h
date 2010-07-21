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

private:
	Node**					heads;
	int						num;
	int						tableSize;
};

#endif
