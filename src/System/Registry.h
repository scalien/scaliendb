#ifndef REGISTRY_H
#define REGISTRY_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"

class Registry;

/*
===============================================================================================

 RegistryNode

===============================================================================================
*/

class Registry;

class RegistryNode
{
    typedef InTreeNode<RegistryNode> TreeNode;
    friend class Registry;

    typedef enum { None, Int, Uint, Bool } ValueType;

    Buffer          key;
    int64_t         valueInt;
    uint64_t        valueUint;
    bool            valueBool;
    ValueType       valueType;
    TreeNode        treeNode;

    RegistryNode();

public:
    const Buffer&   GetKey() const;

    void            AppendKey(Buffer& buffer) const;
    void            AppendValue(Buffer& buffer) const;
};

/*
===============================================================================================

 Registry

===============================================================================================
*/

class Registry
{
    typedef InTreeMap<RegistryNode> RegistryTree;

public:
    bool                    Exists(const ReadBuffer& key);

    static int64_t*         GetIntPtr(const ReadBuffer& key);
    static uint64_t*        GetUintPtr(const ReadBuffer& key);
    static bool*            GetBoolPtr(const ReadBuffer& key);

    static RegistryNode*    First();
    static RegistryNode*    Last();
    static RegistryNode*    Next(RegistryNode* t);

private:
    static RegistryNode*    Get(const ReadBuffer& key, RegistryNode::ValueType valueType);

    static RegistryTree     registryTree;
};

#endif
