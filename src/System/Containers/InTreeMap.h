#ifndef INTREEMAP_H
#define INTREEMAP_H

/*
===============================================================================

 InTreeNode is the datatype that is stored in InTreeMap

===============================================================================
*/

template<typename T>
class InTreeNode
{
public:
	enum Color 
	{
		RED,
		BLACK
	};

	Color					color;
	InTreeNode<T>*			left;
	InTreeNode<T>*			right;
	InTreeNode<T>*			parent;
	T*						owner;
};

/*
===============================================================================

 InTreeMap is an intrusive associative map, implemented with Red-Black tree

===============================================================================
*/

template<typename T, InTreeNode<T> T::*pnode>
class InTreeMap
{
public:
	typedef InTreeNode<T>	Node;

	InTreeMap();
	
	template<typename K>
	T*						Get(K key);
	
	T*						Insert(T* t);
	
	template<typename K>
	T*						Delete(K key);
	
private:
	void					FixColors(Node* node);
	void					FixUncleColors(Node* node);
	void					FixRotation(Node* node);
	void					ReplaceNode(Node* src, Node* dst);
	void					RotateLeft(Node* node);
	void					RotateRight(Node* node);

	bool					IsLeftChild(Node* node);
	bool					IsRightChild(Node* node);
	
	Node*					Left(Node* node);
	Node*					Right(Node* node);
	Node*					Parent(Node* node);
	Node*					Uncle(Node* node);
	Node*					Grandparent(Node* node);
	
	Node*					MaxNode(Node* node);
	
	static T*				GetElem(Node* node);
	static Node*			GetNode(T* t);

	Node*					root;
};


template<typename T, InTreeNode<T> T::*pnode>
InTreeMap<T, pnode>::InTreeMap()
{
	root = NULL;
}

template<typename T, InTreeNode<T> T::*pnode>
template<typename K>
T* InTreeMap<T, pnode>::Get(K key)
{
	Node*	node;
	int		result;
	
	node = root;
	while (node)
	{
		result = KeyCmp(key, Key(node->owner));
		if (result < 0)
			node = Left(node);
		else if (result > 0)
			node = Right(node);
		else
			return GetElem(node);
	}
	return NULL;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::Insert(T* t)
{
	Node*	curr;
	Node*	node;
	int		result;
	
	node = &(t->*pnode);
	node->owner = t;
	node->color = Node::RED;
	if (root == NULL)
	{
		root = node;
		return NULL;
	}
	
	curr = root;
	while (1)
	{
		result = KeyCmp(Key(t), Key(curr->owner));
		if (result == 0)
		{
			// replace the old node with the new one
			node->color = curr->color;
			ReplaceNode(curr, node);
			return curr->owner;
		}
		else if (result < 0)
		{
			if (curr->left == NULL)
			{
				curr->left = node;
				break;
			}
			else
				curr = curr->left;
		}
		else /* result > 0 */
		{
			if (curr->right == NULL)
			{
				curr->right = node;
				break;
			}
			else
				curr = curr->right;
		}
	}
	
	node->parent = curr;
	FixColors(node);
	
	return NULL;
}

//template<typename T, InTreeNode<T> T::*pnode>
//template<typename K>
//T* InTreeMap<T, pnode>::Delete(K key)
//{
//	T*		t;
//	Node*	node;
//	Node*	max;
//	
//	t = Get<K>(key);
//	if (t == NULL)
//		return NULL;
//	
//	node = GetNode(t);
//	if (node->left != NULL && node->right != NULL)
//	{
//		max = MaxNode(node->left);
//	}
//}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::FixColors(Node* node)
{
	// root of the tree
	if (node->parent == NULL)
	{
		node->color = Node::BLACK;
		return;
	}
	
	if (Parent(node)->color == Node::BLACK)
		return;
	
	FixUncleColors(node);
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::FixUncleColors(Node* node)
{
	Node*	uncle;
	
	uncle = Uncle(node);

	if (uncle != NULL && Uncle(node)->color == Node::RED)
	{
		Parent(node)->color = Node::BLACK;
		Uncle(node)->color = Node::BLACK;
		Grandparent(node)->color = Node::RED;
		FixColors(Grandparent(node));
	}
	else
		FixRotation(node);
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::FixRotation(Node* node)
{
	Node*	parent;
	Node*	grandparent;
	
	parent = Parent(node);
	if (IsRightChild(node) && parent && IsLeftChild(parent))
	{
		RotateLeft(parent);
		node = Left(node);
	}
	else if (IsLeftChild(node) && parent && IsRightChild(parent))
	{
		RotateRight(parent);
		node = Right(node);
	}
	
	parent = Parent(node);
	grandparent = NULL;
	if (parent)
	{
		parent->color = Node::BLACK;
		grandparent = Parent(parent);
		if (!grandparent)
			return;
		grandparent->color = Node::RED;
	}
	else
		ASSERT_FAIL();

	if (IsLeftChild(node) && parent && IsLeftChild(parent))
	{
		RotateRight(grandparent);
	}
	else
	{
		assert(IsRightChild(node) && parent && IsRightChild(parent));
		RotateLeft(grandparent);
	}
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::RotateLeft(Node* node)
{
	Node*	right;

	right = Right(node);
	ReplaceNode(node, right);
	node->right = right->left;
	if (right->left != NULL)
		Left(right)->parent = node;
	right->left = node;
	node->parent = right;
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::RotateRight(Node* node)
{
	Node*	left;
	
	left = Left(node);
	ReplaceNode(node, left);
	node->left = left->right;
	if (left->right != NULL)
		Right(left)->parent = node;
	left->right = node;
	node->parent = left;
}

template<typename T, InTreeNode<T> T::*pnode>
bool InTreeMap<T, pnode>::IsLeftChild(Node* node)
{
	Node*	parent;
	
	parent = Parent(node);
	if (parent == NULL)
		return false;
	if (node == Left(parent))
		return true;
	return false;
}

template<typename T, InTreeNode<T> T::*pnode>
bool InTreeMap<T, pnode>::IsRightChild(Node* node)
{
	Node*	parent;
	
	parent = Parent(node);
	if (parent == NULL)
		return false;
	if (node == Right(parent))
		return true;
	return false;
}

template<typename T, InTreeNode<T> T::*pnode>
InTreeNode<T>* InTreeMap<T, pnode>::Left(Node* node)
{
	return node->left;
}

template<typename T, InTreeNode<T> T::*pnode>
InTreeNode<T>* InTreeMap<T, pnode>::Right(Node* node)
{
	return node->right;
}

template<typename T, InTreeNode<T> T::*pnode>
InTreeNode<T>* InTreeMap<T, pnode>::Parent(Node* node)
{
	return node->parent;
}

template<typename T, InTreeNode<T> T::*pnode>
InTreeNode<T>* InTreeMap<T, pnode>::Uncle(Node* node)
{
	Node*	parent;
	Node*	grandparent;
	
	parent = Parent(node);
	if (parent == NULL)
		return NULL;
		
	grandparent = Grandparent(node);
	if (grandparent == NULL)
		return NULL;
		
	if (IsLeftChild(parent))
		return Right(grandparent);
	else
		return Left(grandparent);
}

template<typename T, InTreeNode<T> T::*pnode>
InTreeNode<T>* InTreeMap<T, pnode>::Grandparent(Node* node)
{
	Node*	parent;
	
	parent = Parent(node);
	if (parent == NULL)
		return NULL;
		
	return Parent(parent);
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::ReplaceNode(Node* src, Node* dst)
{
	if (src->parent == NULL)
		root = dst;
	else
	{
		if (src == src->parent->left)
			Parent(src)->left = dst;
		else
			Parent(src)->right = dst;
	}
	if (dst != NULL)
		dst->parent = src->parent;
}

template<typename T, InTreeNode<T> T::*pnode>
InTreeNode<T>* InTreeMap<T, pnode>::MaxNode(Node* node)
{
	while (node->right != NULL)
		node = node->right;
	return node;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::GetElem(Node* node)
{
	return node->owner;
}

template<typename T, InTreeNode<T> T::*pnode>
InTreeNode<T>* InTreeMap<T, pnode>::GetNode(T* t)
{
	return &(t->*pnode);
}


#endif
