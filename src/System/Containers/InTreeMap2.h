#ifndef INTREEMAP2_H
#define INTREEMAP2_H

#include <stdlib.h>
#include <assert.h>

#include "System/Macros.h"

/*
 ===============================================================================================
 
 Check alignment that makes storing the parent and the color in the same memory possible
 
 ===============================================================================================
 */

template<typename T>
struct AligmentCheck
{
    char    c;
    T       t;
};

template <unsigned A, unsigned S>
struct AlignmentLogic
{
    static const size_t value = A < S ? A : S;
};

template<typename T>
struct AlignmentOf 
{
    static const size_t value = AlignmentLogic<
                                 sizeof(AligmentCheck<T>) - sizeof(T), sizeof(T)>::value;
};

/*
 ===============================================================================================
 
 InTreeNode is the datatype that is stored in InTreeMap
 
 ===============================================================================================
 */

#define INTREENODE_COLOR_MASK   0x01

template<typename T>
class InTreeNode
{
public:
    STATIC_ASSERT((AlignmentOf<T*>::value > sizeof(char)), "Pointers are not aligned, bit packing is not possible!");

    enum Color 
    {
        RED = 0,
        BLACK = 1
    };
        
    T*                      left;
    T*                      right;
    uintptr_t               parent;
};

/*
 ===============================================================================================
 
 InTreeMap is an intrusive associative map, implemented with Red-Black tree
 
 ===============================================================================================
 */

template<typename T, InTreeNode<T> T::*pnode = &T::treeNode>
class InTreeMap
{
public:
    typedef InTreeNode<T>   Node;    
    
    InTreeMap();
    
    unsigned                GetCount();
    
    T*                      First();
    T*                      Last();
    T*                      Mid();
    T*                      Next(T* t);
    T*                      Prev(T* t);
    
    template<typename K>
    T*                      Get(K key);
    
    template<typename K>
    T*                      Locate(K key, int& cmpres);
    
    T*                      Insert(T* t);
    void                    InsertAt(T* t, T* pos, int cmpres);
    
    T*                      Remove(T* t);
    
    void                    Clear();
    void                    DeleteTree();
    
private:
    void                    DeleteInner(T* inner);
    void                    FixColorsOnRemoval(T* elem, T* parent);
    void                    FixColors(T* elem);
    void                    FixUncleColors(T* elem);
    void                    FixRotation(T* elem);
    void                    RotateLeft(T* elem);
    void                    RotateRight(T* elem);
    void                    ReplaceNode(T* src, T* dst);    
    void                    MarkNodeRemoved(T* elem);
    
    bool                    IsLeftChild(T* elem) const;
    bool                    IsRightChild(T* elem) const;
    
    T*                      GetLeft(T* elem) const;
    T*                      GetRight(T* elem) const;
    int                     GetColor(T* elem) const;
    T*                      GetParent(T* elem) const;
    T*                      GetUncle(T* elem) const;
    T*                      GetGrandparent(T* elem) const;

    void                    SetLeft(T* elem, T* left);
    void                    SetRight(T* elem, T* right);
    void                    SetParent(T* elem, T* parent);
    void                    SetColor(T* elem, int color);
    
    T*                      root;
    unsigned                count;
};


template<typename T, InTreeNode<T> T::*pnode>
InTreeMap<T, pnode>::InTreeMap()
{
    root = NULL;
    count = 0;
}

template<typename T, InTreeNode<T> T::*pnode>
unsigned InTreeMap<T, pnode>::GetCount()
{
    return count;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::First()
{
    T*  elem;
    T*  left;
    
    if (root == NULL)
        return NULL;
    
    elem = root;
    left = GetLeft(elem);
    while (left)
    {
        elem = left;
        left = GetLeft(elem);
    }
    
    return elem;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::Last()
{
    T*  elem;
    T*  right;
    
    if (root == NULL)
        return NULL;
    
    elem = root;
    right = GetRight(elem);
    while (right)
    {
        elem = right;
        right = GetRight(elem);
    }
    
    return elem;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::Mid()
{
    return root;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::Next(T* t)
{
    T*  elem;
    T*  parent;

    elem = t;
    if (GetRight(elem))
    {
        elem = GetRight(elem);
        while (GetLeft(elem))
            elem = GetLeft(elem);
        
        return elem;
    }
    
    while ((parent = GetParent(elem)) && elem == GetRight(parent))
        elem = parent;
    
    return parent;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::Prev(T* t)
{
    T*  elem;
    T*  parent;
    
    elem = t;
    if (GetLeft(elem))
    {
        elem = GetLeft(elem);
        while (GetRight(elem))
            elem = GetRight(elem);
        
        return elem;
    }
    
    while ((parent = GetParent(elem)) && elem == GetLeft(parent))
        elem = parent;
    
    return parent;
}

template<typename T, InTreeNode<T> T::*pnode>
template<typename K>
T* InTreeMap<T, pnode>::Get(K key)
{
    T*      elem;
    int     result;
    
    elem = root;
    while (elem)
    {
        result = KeyCmp(key, Key(elem));
        if (result < 0)
            elem = GetLeft(elem);
        else if (result > 0)
            elem = GetRight(elem);
        else
            return elem;
    }
    return NULL;
}

template<typename T, InTreeNode<T> T::*pnode>
template<typename K>
T* InTreeMap<T, pnode>::Locate(K key, int& cmpres)
{
    T*      elem;
    
    cmpres = 0;
    elem = root;
    while (elem)
    {
        cmpres = KeyCmp(key, Key(elem));
        if (cmpres < 0)
        {
            if (GetLeft(elem) == NULL)
                break;
            elem = GetLeft(elem);
        }
        else if (cmpres > 0)
        {
            if (GetRight(elem) == NULL)
                break;
            elem = GetRight(elem);
        }
        else
            break;
    }
    
    return elem;
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::InsertAt(T* t, T* pos, int cmpres)
{
    T*      elem;
    T*      curr;
    
    elem = t;
    SetLeft(elem, NULL);
    SetRight(elem, NULL);
    SetParent(elem, NULL);
    SetColor(elem, Node::RED);
    if (root == NULL)
    {
        assert(pos == NULL && cmpres == 0);
        root = elem;
        count = 1;
        return;
    }
    
    curr = pos;
    if (cmpres < 0)
    {
        assert(GetLeft(curr) == NULL);
        SetLeft(curr, elem);
        count++;
    }
    else if (cmpres > 0)
    {
        assert(GetRight(curr) == NULL);
        SetRight(curr, elem);
        count++;
    }
    else
    {
        // overwrite the node and the owner in place
        SetColor(elem, GetColor(curr));
        ReplaceNode(curr, elem);
        return;
    }
    
    SetParent(elem, curr);
    FixColors(elem);
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::Insert(T* t)
{
    T*      curr;
    T*      elem;
    int     result;

    elem = t;
    SetLeft(elem, NULL);
    SetRight(elem, NULL);
    SetParent(elem, NULL);
    SetColor(elem, Node::RED);
    if (root == NULL)
    {
        root = elem;
        count = 1;
        return NULL;
    }
    
    curr = root;
    while (true)
    {
        result = KeyCmp(Key(t), Key(curr));
        if (result == 0)
        {
            // replace the old node with the new one
            SetColor(elem, GetColor(curr));
            ReplaceNode(curr, elem);
            MarkNodeRemoved(curr);
            return curr;
        }
        else if (result < 0)
        {
            if (GetLeft(curr) == NULL)
            {
                SetLeft(curr, elem);
                count++;
                break;
            }
            else
                curr = GetLeft(curr);
        }
        else /* result > 0 */
        {
            if (GetRight(curr) == NULL)
            {
                SetRight(curr, elem);
                count++;
                break;
            }
            else
                curr = GetRight(curr);
        }
    }
    
    SetParent(elem, curr);
    FixColors(elem);
    
    return NULL;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::Remove(T* t)
{
    T*      elem;
    T*      child;
    T*      parent;
    T*      next;
    int     color;
    
    elem = t;
    next = Next(t);
    if (GetLeft(elem) == NULL)
        child = GetRight(elem);
    else if (GetRight(elem) == NULL)
        child = GetLeft(elem);
    else
    {
        T*   old;
        T*   left;
        
        old = elem;
        
        // find the smallest bigger node
        elem = GetRight(elem);
        while ((left = GetLeft(elem)) != NULL)
            elem = GetLeft(elem);
        
        if (GetParent(old))
        {
            if (IsLeftChild(old))
                SetLeft(GetParent(old), elem);
            else
                SetRight(GetParent(old), elem);
        }
        else
            root = elem;
        
        child = GetRight(elem);
        parent = GetParent(elem);
        color = GetColor(elem);
        
        if (parent == old)
            parent = elem;
        else
        {
            if (child)
                SetParent(child, parent);
            SetLeft(parent, child);
            
            SetRight(elem, GetRight(old));
            SetParent(GetRight(old), elem);
        }
        
        SetParent(elem, GetParent(old));
        SetColor(elem, GetColor(old));
        SetLeft(elem, GetLeft(old));
        SetParent(GetLeft(old), elem);
        
        goto color;
    }
    
    parent = GetParent(elem);
    color = GetColor(elem);
    
    if (child)
        SetParent(child, parent);
    if (parent)
    {
        if (GetLeft(parent) == elem)
            SetLeft(parent, child);
        else
            SetRight(parent, child);
    }
    else
        root = child;
    
color:
    if (color == Node::BLACK)
        FixColorsOnRemoval(child, parent);

    MarkNodeRemoved(t);
    count--;
    return next;
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::Clear()
{
    // TODO: Clear recursively like Delete for setting tree nodes to NULL
    // and providing more info when debuggging
    root = NULL;
    count = 0;
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::DeleteTree()
{
    DeleteInner(root);
    root = NULL;
    count = 0;
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::DeleteInner(T* t)
{   
    DeleteInner(GetLeft(t));
    DeleteInner(GetRight(t));
    
    delete t;
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::FixColorsOnRemoval(T* elem, T* parent)
{
    T*  other;
    
    while ((elem == NULL || GetColor(elem) == Node::BLACK) && elem != root)
    {
        if (GetLeft(parent) == elem)
        {
            other = GetRight(parent);
            if (GetColor(other) == Node::RED)
            {
                SetColor(other, Node::BLACK);
                SetColor(parent, Node::RED);
                RotateLeft(parent);
                other = GetRight(parent);
            }
            if ((GetLeft(other) == NULL || GetColor(GetLeft(other)) == Node::BLACK) &&
                (GetRight(other) == NULL || GetColor(GetRight(other)) == Node::BLACK))
            {
                SetColor(other, Node::RED);
                elem = parent;
                parent = GetParent(elem);
            }
            else
            {
                if (GetRight(other) == NULL || GetColor(GetRight(other)) == Node::BLACK)
                {
                    SetColor(GetLeft(other), Node::BLACK);
                    SetColor(other, Node::RED);
                    RotateRight(other);
                    other = GetRight(parent);
                }
                SetColor(other, GetColor(parent));
                SetColor(parent, Node::BLACK);
                SetColor(GetRight(other), Node::BLACK);
                RotateLeft(parent);
                elem = root;
                break;
            }
        }
        else
        {
            other = GetLeft(parent);
            if (GetColor(other) == Node::RED)
            {
                SetColor(other, Node::BLACK);
                SetColor(parent, Node::RED);
                RotateRight(parent);
                other = GetLeft(parent);
            }
            if ((GetLeft(other) == NULL || GetColor(GetLeft(other)) == Node::BLACK) &&
                (GetRight(other) == NULL || GetColor(GetRight(other)) == Node::BLACK))
            {
                SetColor(other, Node::RED);
                elem = parent;
                parent = GetParent(elem);
            }
            else
            {
                if (GetLeft(other) == NULL || GetColor(GetLeft(other)) == Node::BLACK)
                {
                    SetColor(GetRight(other), Node::BLACK);
                    SetColor(other, Node::RED);
                    RotateLeft(other);
                    other = GetLeft(parent);
                }
                SetColor(other, GetColor(parent));
                SetColor(parent, Node::BLACK);
                SetColor(GetLeft(other), Node::BLACK);
                RotateRight(parent);
                elem = root;
                break;
            }
        }
    }
    if (elem)
        SetColor(elem, Node::BLACK);
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::FixColors(T* elem)
{
    // root of the tree
    if (GetParent(elem) == NULL)
    {
        SetColor(elem, Node::BLACK);
        return;
    }
    
    if (GetColor(GetParent(elem)) == Node::BLACK)
        return;
    
    FixUncleColors(elem);
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::FixUncleColors(T* elem)
{
    T*      uncle;
    
    uncle = GetUncle(elem);
    
    if (uncle != NULL && GetColor(uncle) == Node::RED)
    {
        SetColor(GetParent(elem), Node::BLACK);
        SetColor(GetUncle(elem), Node::BLACK);
        SetColor(GetGrandparent(elem), Node::RED);
        FixColors(GetGrandparent(elem));
    }
    else
        FixRotation(elem);
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::FixRotation(T* elem)
{
    T*   parent;
    T*   grandparent;
    
    parent = GetParent(elem);
    if (IsRightChild(elem) && parent && IsLeftChild(parent))
    {
        RotateLeft(parent);
        elem = GetLeft(elem);
    }
    else if (IsLeftChild(elem) && parent && IsRightChild(parent))
    {
        RotateRight(parent);
        elem = GetRight(elem);
    }
    
    parent = GetParent(elem);
    grandparent = NULL;
    if (parent)
    {
        SetColor(parent, Node::BLACK);
        grandparent = GetParent(parent);
        if (!grandparent)
            return;
        SetColor(grandparent, Node::RED);
    }
    else
        assert(false);
    
    if (IsLeftChild(elem) && parent && IsLeftChild(parent))
    {
        RotateRight(grandparent);
    }
    else
    {
        assert(IsRightChild(elem) && parent && IsRightChild(parent));
        RotateLeft(grandparent);
    }
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::RotateLeft(T* elem)
{
    T*   right;
    
    right = GetRight(elem);
    ReplaceNode(elem, right);
    SetRight(elem, GetLeft(right));
    if (GetLeft(right) != NULL)
        SetParent(GetLeft(right), elem);
    SetLeft(right, elem);
    SetParent(elem, right);
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::RotateRight(T* elem)
{
    T*   left;
    
    left = GetLeft(elem);
    ReplaceNode(elem, left);
    SetLeft(elem, GetRight(left));
    if (GetRight(left) != NULL)
        SetParent(GetRight(left), elem);
    SetRight(left, elem);
    SetParent(elem, left);
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::ReplaceNode(T* src, T* dst)
{
    if (GetParent(src) == NULL)
        root = dst;
    else
    {
        if (src == GetLeft(GetParent(src)))
            SetLeft(GetParent(src), dst);
        else
            SetRight(GetParent(src), dst);
    }
    if (dst != NULL)
        SetParent(dst, GetParent(src));
}
                    
template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::MarkNodeRemoved(T* /*elem*/)
{
    // TODO:
}               
                
template<typename T, InTreeNode<T> T::*pnode>
bool InTreeMap<T, pnode>::IsLeftChild(T* elem) const
{
    T*   parent;
    
    parent = GetParent(elem);
    if (parent == NULL)
        return false;
    if (elem == GetLeft(parent))
        return true;
    return false;
}

template<typename T, InTreeNode<T> T::*pnode>
bool InTreeMap<T, pnode>::IsRightChild(T* elem) const
{
    T*   parent;
    
    parent = GetParent(elem);
    if (parent == NULL)
        return false;
    if (elem == GetRight(parent))
        return true;
    return false;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::GetLeft(T* elem) const
{
    return (elem->*pnode).left;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::GetRight(T* elem) const
{
    return (elem->*pnode).right;
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::GetParent(T* elem) const
{
    return (T*)((elem->*pnode).parent & ~INTREENODE_COLOR_MASK);
}

template<typename T, InTreeNode<T> T::*pnode>
int InTreeMap<T, pnode>::GetColor(T* elem) const
{
    return (int)((elem->*pnode).parent & INTREENODE_COLOR_MASK);
}

template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::GetUncle(T* elem) const
{
    T*   parent;
    T*   grandparent;

    parent = GetParent(elem);
    if (parent == NULL)
        return NULL;

    grandparent = GetGrandparent(elem);
    if (grandparent == NULL)
        return NULL;

    if (IsLeftChild(parent))
        return GetRight(grandparent);
    else
        return GetLeft(grandparent);
}
                
template<typename T, InTreeNode<T> T::*pnode>
T* InTreeMap<T, pnode>::GetGrandparent(T* elem) const
{
    T*   parent;
    
    parent = GetParent(elem);
    if (parent == NULL)
        return NULL;
    
    return GetParent(parent);
}
    
template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::SetLeft(T* elem, T* left)
{
    (elem->*pnode).left = left;
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::SetRight(T* elem, T* right)
{
    (elem->*pnode).right = right;
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::SetParent(T* elem, T* parent)
{
    (elem->*pnode).parent = ((elem->*pnode).parent & INTREENODE_COLOR_MASK) | (uintptr_t) parent;
}

template<typename T, InTreeNode<T> T::*pnode>
void InTreeMap<T, pnode>::SetColor(T*elem, int color)
{
    (elem->*pnode).parent = ((elem->*pnode).parent & ~INTREENODE_COLOR_MASK) | color;
}


#endif
