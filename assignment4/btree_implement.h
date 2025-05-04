#ifndef BTREE_IMPLEMENT_H
#define BTREE_IMPLEMENT_H

#include "btree_mgr.h"
#include "buffer_mgr.h"

typedef struct NodeData {
    RID rid;
} NodeData;

typedef struct Node {
    void ** pointers;
    Value ** keys;
    struct Node * parent;
    bool is_leaf;
    int num_keys;
    struct Node * next;
} Node;

typedef struct B_treeInfo {
    int order;
    int numEntries;
    int no_of_nodes;
    Node * root;
    DataType keyType;
    Node * queue;
    BM_PageHandle pageHandler;
    BM_BufferPool bufferPool;
} B_treeInfo;

typedef struct Sc_mgr {
    Node * node;
    int keyIndex;
    int order;
    int totalKeys;
} Sc_mgr;

NodeData * searchRecord(Node * root, Value * key); 
Node * searchLeaf(Node * root, Value * key);

int root_path(Node * root, Node * child);
void addToQueue(B_treeInfo * tree_Mgr, Node * new_node);
Node * removeFromQueue(B_treeInfo * tree_Mgr);

Node * initializeBTree(B_treeInfo * tree_Mgr, Value * key, NodeData * pointer);
NodeData * createRecordData(RID * rid);
Node * insertKeyInLeaf(B_treeInfo * tree_Mgr, Node * leaf, Value * key, NodeData * pointer);
Node * splitAndInsertInNode(B_treeInfo * tree_Mgr, Node * parent, int left_index, Value * key, Node * right);
Node * splitAndInsertInLeaf(B_treeInfo * tree_Mgr, Node * leaf, Value * key, NodeData * pointer);
Node * insertKeyInNode(B_treeInfo * tree_Mgr, Node * parent, int left_index, Value * key, Node * right);
int getLeftChildIndex(Node * parent, Node * left);
Node * addToParentNode(B_treeInfo * tree_Mgr, Node * left, Value * key, Node * right);
Node * addNewRoot(B_treeInfo * tree_Mgr, Node * left, Value * key, Node * right);
Node * allocateBTreeNode(B_treeInfo * tree_Mgr);
Node * allocateLeafNode(B_treeInfo * tree_Mgr);

Node * mergeNodes(B_treeInfo * tree_Mgr, Node * n, Node * neighbor, int neighbor_index, int k_prime);
Node * delete(B_treeInfo * tree_Mgr, Value * key);
Node * restructureRoot(Node * root);
Node * redistributeNodes(Node * root, Node * n, Node * neighbor, int neighbor_index, int k_prime_index, int k_prime);
Node * removeEntry(B_treeInfo * tree_Mgr, Node * n, Value * key, Node * pointer);
Node * removeKeyFromNode(B_treeInfo * tree_Mgr, Node * n, Value * key, void * pointer);
int findSiblingIndex(Node * n);

bool isKeyLess(Value * key1, Value * key2);
bool areKeysEqual(Value * key1, Value * key2);
bool isKeyGreater(Value * key1, Value * key2);


#endif
