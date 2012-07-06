#include "Registry.h"

Registry::RegistryTree Registry::registryTree;

static inline int KeyCmp(const Buffer& a, const Buffer& b)
{
    return Buffer::Cmp(a, b);
}

static inline const Buffer& Key(const RegistryNode* node)
{
    return node->GetKey();
}

RegistryNode::RegistryNode()
{
    valueInt = 0;
    valueUint = 0;
    valueBool = false;
}

const Buffer& RegistryNode::GetKey() const
{
    return key;
}

void RegistryNode::AppendValue(Buffer& buffer) const
{
    switch (valueType)
    {
    case Uint:
        buffer.Appendf("%U", valueUint);
        break;
    case Int:
        buffer.Appendf("%I", valueInt);
        break;
    case Bool:
        if (valueBool)
            buffer.Appendf("yes");
        else
            buffer.Appendf("no");
        break;
    default:
        ASSERT_FAIL();
    }
}

void RegistryNode::AppendKey(Buffer& buffer) const
{
    buffer.Append(key);
}

bool Registry::Exists(const ReadBuffer& key)
{
    Buffer          keyBuffer;
    RegistryNode*   node;
    
    keyBuffer.Write(key);
    
    node = registryTree.Get(keyBuffer);
    
    if (node)
        return true;

    return false;
}


int64_t* Registry::GetIntPtr(const ReadBuffer& key)
{
    RegistryNode* node;
    node = Get(key, RegistryNode::Int);
    return &node->valueInt;
}

uint64_t* Registry::GetUintPtr(const ReadBuffer& key)
{
    RegistryNode* node;
    node = Get(key, RegistryNode::Uint);
    return &node->valueUint;

}

bool* Registry::GetBoolPtr(const ReadBuffer& key)
{
    RegistryNode* node;
    node = Get(key, RegistryNode::Bool);
    return &node->valueBool;
}

RegistryNode* Registry::First()
{
    return registryTree.First();
}

RegistryNode* Registry::Last()
{
    return registryTree.Last();
}

RegistryNode* Registry::Next(RegistryNode* node)
{
    return registryTree.Next(node);
}

RegistryNode* Registry::Get(const ReadBuffer& key, RegistryNode::ValueType valueType)
{
    int             cmpres;
    Buffer          keyBuffer;
    RegistryNode*   node;
    RegistryNode*   newNode;
    
    keyBuffer.Write(key);
    
    node = registryTree.Locate(keyBuffer, cmpres);
    
    if (!FOUND_IN_TREE(node, cmpres))
    {
        newNode = new RegistryNode;
        newNode->key.Write(key);
        newNode->valueType = valueType;
        registryTree.InsertAt(newNode, node, cmpres);
        node = newNode;
    }

    ASSERT(node);
    ASSERT(node->valueType == valueType);
    return node;
}
