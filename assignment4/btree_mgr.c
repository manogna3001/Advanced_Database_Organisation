#include "buffer_mgr.h"
#include<stdlib.h>
#include "tables.h"
#include "dberror.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "btree_implement.h"

B_treeInfo *tree_manager = NULL;

// intializing the indexmanager
RC initIndexManager(void *mgmtData)
{
    initStorageManager();
    return RC_OK;
}

//  shutingdown b+tree manager
RC shutdownIndexManager()
{
    static B_treeInfo *const nullPtr = NULL;
    tree_manager = nullPtr;
    return RC_OK;
}


// creating the b+tree
RC createBtree(char *idxId, DataType keyType, int n)
{
    int maxNodesPerPage = PAGE_SIZE / sizeof(Node);

    (n > maxNodesPerPage) && (printf("\n n = %d > Max. Nodes = %d \n", n, maxNodesPerPage), exit(RC_ORDER_TOO_HIGH_FOR_PAGE), 0);

    tree_manager = (B_treeInfo *)calloc(1, sizeof(B_treeInfo));

    tree_manager->order = n + 2;
    tree_manager->keyType = keyType;

    BM_BufferPool *temp = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    tree_manager->bufferPool = temp ? *temp : (BM_BufferPool){0};

    SM_FileHandle fileHandler = {0};
    RC result = 0;

    char data[PAGE_SIZE] = {0};

    (result = createPageFile(idxId)) != RC_OK ? result : (void)0;

    (result = openPageFile(idxId, &fileHandler)) != RC_OK ? result : (void)0;

    (result = writeBlock(0, &fileHandler, data)) != RC_OK ? result : (void)0;

    (result = closePageFile(&fileHandler)) != RC_OK ? result : (void)0;

    return (RC_OK);

    free(temp);
    free(tree_manager);
    closePageFile(&fileHandler);
    return result;
}

// open the b+tree from given idxId
RC openBtree(BTreeHandle **tree, char *idxId)
{
    if (!tree || !idxId)
        return RC_ERROR;

    *tree = (BTreeHandle *)malloc(sizeof(BTreeHandle));
    if (!*tree)
        return RC_MEMORY_ALLOCATION_FAIL;

    (*tree)->mgmtData = tree_manager;

    if (!tree_manager)
        return RC_ERROR;

    RC result = initBufferPool(&tree_manager->bufferPool, idxId, 1000, RS_FIFO, NULL);

    return result == RC_OK ? RC_OK : (printf("Failed to initialize buffer pool\n"), result);
}

// close the b+tree and marks page dirty
RC closeBtree(BTreeHandle *tree)
{
    if (tree == NULL || tree->mgmtData == NULL)
    {
        return RC_ERROR;
    }

    B_treeInfo *tree_manager = (B_treeInfo *)tree->mgmtData;

    if (&tree_manager->bufferPool == NULL || &tree_manager->pageHandler == NULL)
    {
        return RC_ERROR;
    }

    markDirty(&tree_manager->bufferPool, &tree_manager->pageHandler);

    if (&tree_manager->bufferPool != NULL)
    {
        shutdownBufferPool(&tree_manager->bufferPool);
    }

    if (tree_manager != NULL)
    {
        free(tree_manager);
    }

    if (tree != NULL)
    {
        free(tree);
    }

    return RC_OK;
}

// delete b+tree 
RC deleteBtree(char *idxId)
{
    RC result = RC_ERROR;

    result = destroyPageFile(idxId);

    return (result == RC_OK) ? RC_OK : RC_FILE_NOT_FOUND;
}

// get no of nodes from b+tree
RC getNumNodes(BTreeHandle *tree, int *result)
{
    if (!tree || !result || !tree->mgmtData)
    {
        return RC_ERROR;
    }

    B_treeInfo *tree_manager = (B_treeInfo *)tree->mgmtData;

    *result = tree_manager ? tree_manager->no_of_nodes : -1;

    return (*result >= 0) ? RC_OK : RC_ERROR;
}

// get no of entries from b+tree

RC getNumEntries(BTreeHandle *tree, int *result)
{
    if (!tree || !result || !tree->mgmtData)
    {
        return RC_ERROR;
    }

    B_treeInfo *tree_manager = (B_treeInfo *)tree->mgmtData;

    *result = tree_manager ? tree_manager->numEntries : -1;

    return (*result >= 0) ? RC_OK : RC_ERROR;
}

// get key datatype from b+tree
RC getKeyType(BTreeHandle *tree, DataType *result)
{
    if (!tree || !result || !tree->mgmtData)
    {
        return RC_ERROR;
    }

    B_treeInfo *tree_manager = (B_treeInfo *)tree->mgmtData;

    *result = tree_manager ? tree_manager->keyType : -1;

    return RC_OK;
}

// search given key in b+tree
extern RC findKey(BTreeHandle *tree, Value *key, RID *result)
{
    if (!tree || !result || !tree->mgmtData)
    {
        return RC_ERROR;
    }

    B_treeInfo *tree_manager = (B_treeInfo *)tree->mgmtData;

    if (!tree_manager)
    {
        return RC_ERROR;
    }

    NodeData *foundRecord = searchRecord(tree_manager->root, key);
    return (foundRecord)
               ? (*result = foundRecord->rid, RC_OK)
               : RC_IM_KEY_NOT_FOUND;
}

// inserting the key with given rid into b+tree
RC insertKey(BTreeHandle *tree, Value *key, RID rid)
{
    B_treeInfo *tree_manager = (tree && tree->mgmtData) ? (B_treeInfo *)tree->mgmtData : NULL;
    if (!tree_manager)
        return RC_ERROR;

    NodeData *pointer = NULL;
    Node *leaf = NULL;
    int bTreeOrder = (tree_manager) ? tree_manager->order : 0;
    if (bTreeOrder <= 0)
        return RC_ERROR;

    NodeData *existingRecord = searchRecord(tree_manager->root, key);
    if (existingRecord)
    {
        printf("\n insertKey :: KEY EXISTS");
        return RC_IM_KEY_ALREADY_EXISTS;
    }

    pointer = createRecordData(&rid);
    if (!pointer)
        return RC_ERROR;

    if (!tree_manager->root)
    {
        tree_manager->root = initializeBTree(tree_manager, key, pointer);
        return tree_manager->root ? RC_OK : RC_ERROR;
    }

    leaf = searchLeaf(tree_manager->root, key);
    if (!leaf)
        return RC_ERROR;

    leaf = (leaf->num_keys < bTreeOrder - 1)
               ? insertKeyInLeaf(tree_manager, leaf, key, pointer)
               : (tree_manager->root = splitAndInsertInLeaf(tree_manager, leaf, key, pointer), tree_manager->root);

    return (leaf) ? RC_OK : RC_ERROR;
}

// delete given key record from b+tree

RC deleteKey(BTreeHandle *tree, Value *key)
{
    if (!tree || !tree->mgmtData)
        return RC_ERROR;

    B_treeInfo *tree_manager = (B_treeInfo *)tree->mgmtData;

    if (!tree_manager || !key)
        return RC_ERROR;

    Node *newRoot = delete (tree_manager, key);
    if (!newRoot && tree_manager->root)
        return RC_IM_KEY_NOT_FOUND;

    tree_manager->root = newRoot;

    return RC_OK;
}

// scanning b+tree
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle)
{
    if (!tree || !handle || !tree->mgmtData)
        return RC_ERROR;

    B_treeInfo *tree_manager = (B_treeInfo *)tree->mgmtData;

    Sc_mgr *scandata = (Sc_mgr *)malloc(sizeof(Sc_mgr));
    if (!scandata)
        return RC_MEMORY_ALLOCATION_FAIL;

    *handle = (BT_ScanHandle *)malloc(sizeof(BT_ScanHandle));
    if (!*handle)
    {
        free(scandata);
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    (*handle)->mgmtData = scandata;

    if (!tree_manager->root)
    {
        free(scandata);
        free(*handle);
        return RC_NO_RECORDS_TO_SCAN;
    }

    Node *node = tree_manager->root;
    while (node && !node->is_leaf)
    {
        node = node->pointers[0];
    }

    *scandata = (Sc_mgr){
        .keyIndex = 0,
        .totalKeys = node->num_keys,
        .node = node,
        .order = tree_manager->order};
    (*handle)->mgmtData = scandata;

    return RC_OK;
}

// retrieve next entry from b+tree
RC nextEntry(BT_ScanHandle *handle, RID *result)
{
    Sc_mgr *scandata = handle ? (Sc_mgr *)handle->mgmtData : NULL;
    Node *node = scandata ? scandata->node : NULL;
    int keyIndex = scandata ? scandata->keyIndex : 0;
    int totalKeys = scandata ? scandata->totalKeys : 0;
    int bTreeOrder = scandata ? scandata->order : 0;

    if (!node)
        return RC_IM_NO_MORE_ENTRIES;

    if (keyIndex < totalKeys)
    {
        *result = ((NodeData *)node->pointers[keyIndex])->rid;
        scandata->keyIndex++;
    }
    else
    {
        Node *nextNode = node->pointers[bTreeOrder - 1];

        if (nextNode)
        {
            scandata->keyIndex = 1;
            scandata->totalKeys = nextNode->num_keys;
            scandata->node = nextNode;
            *result = ((NodeData *)nextNode->pointers[0])->rid;
        }
        else
        {
            return RC_IM_NO_MORE_ENTRIES;
        }
    }

    return RC_OK;
}

// close b+tree scan 
extern RC closeTreeScan(BT_ScanHandle *handle)
{
    if (!handle)
    {
        return RC_ERROR;
    }

    if (handle->mgmtData)
    {
        handle->mgmtData = NULL;
    }
    else
    {
        printf("Warning: handle->mgmtData is already NULL.\n");
    }

    if (handle)
    {
        free(handle);
    }
    else
    {
        printf("Warning: handle is NULL, nothing to free.\n");
    }

    return RC_OK;
}

//  print b+tree in a readable format
extern char *printTree(BTreeHandle *tree)
{
    B_treeInfo *tree_manager = (B_treeInfo *)tree->mgmtData;
    printf("\nPRINTING TREE:\n");
    Node *n = NULL;
    int i = 0;
    int rank = 0;
    int new_rank = 0;

    while (tree_manager->root == NULL)
    {
        printf("Empty tree.\n");
        return '\0';
        break;
    }

    tree_manager->queue = NULL;
    addToQueue(tree_manager, tree_manager->root);

    while (tree_manager->queue != NULL)
    {
        n = removeFromQueue(tree_manager);

        (n->parent != NULL && n == n->parent->pointers[0]) &&
            (new_rank = root_path(tree_manager->root, n),

             (new_rank != rank && (rank = new_rank, printf("\n"), 1)));

        for (int i = 0; i < n->num_keys; i++)
        {
            Value *currentKey = n->keys[i];

            if (tree_manager->keyType == DT_INT)
            {
                printf("%d ", currentKey->v.intV);
            }
            else if (tree_manager->keyType == DT_FLOAT)
            {
                printf("%.02f ", currentKey->v.floatV);
            }
            else if (tree_manager->keyType == DT_STRING)
            {
                printf("%s ", currentKey->v.stringV);
            }
            else if (tree_manager->keyType == DT_BOOL)
            {
                printf("%d ", currentKey->v.boolV);
            }

            RID *rid = (RID *)n->pointers[i];
            printf("(%d - %d) ", rid->page, rid->slot);
        }

        int i = 0;

        while (!n->is_leaf)
        {
            while (i <= n->num_keys)
            {
                addToQueue(tree_manager, n->pointers[i]);
                i++;
            }
            break;
        }

        printf("| ");
    }
    return '\0';
}
