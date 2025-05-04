#include "btree_implement.h"
#include "dt.h"
#include "string.h"


Node *initializeBTree(B_treeInfo *tree_manager, Value *key, NodeData *pointer)
{

    if (!tree_manager || !key || !pointer)
    {
        perror("Invalid input: tree_manager, key, or pointer is NULL.");
        exit(RC_INSERT_ERROR);
    }

    Node *root = allocateLeafNode(tree_manager);

    if (!root)
    {
        perror("Failed to create leaf node.");
        exit(RC_INSERT_ERROR);
    }

    *(root->keys) = key;
    *(root->pointers) = pointer;

    *(root->pointers + tree_manager->order - 1) = NULL;
    root->parent = (Node *)0;
    root->num_keys = 1;

    ++tree_manager->numEntries;

    return root;
}

NodeData *createRecordData(RID *rid)
{
    NodeData *record = malloc(sizeof(*record));
    if (!record)
    {
        perror("Failed to allocate NodeData");
        exit(RC_INSERT_ERROR);
    }

    record->rid = *rid;

    return record;
}

Node *addToParentNode(B_treeInfo *tree_manager, Node *left, Value *key, Node *right)
{
    Node *parent = left->parent;
    int bTreeOrder = tree_manager->order;

    return parent
               ? (parent->num_keys < bTreeOrder - 1
                      ? insertKeyInNode(tree_manager, parent, getLeftChildIndex(parent, left), key, right)
                      : splitAndInsertInNode(tree_manager, parent, getLeftChildIndex(parent, left), key, right))
               : addNewRoot(tree_manager, left, key, right);
}

Node *insertKeyInLeaf(B_treeInfo *tree_manager, Node *leaf, Value *key, NodeData *pointer)
{

    tree_manager->numEntries++;

    int insertion_point;
    for (insertion_point = 0; insertion_point < leaf->num_keys && isKeyLess(leaf->keys[insertion_point], key); insertion_point++)
        ;

    if (insertion_point < 0 || insertion_point > leaf->num_keys)
    {
        perror("Insertion point out of bounds");
        return NULL;
    }

    int i = leaf->num_keys;
    int MAX_KEYS = tree_manager->order - 1;

    while (i > insertion_point)
    {
        if (i >= MAX_KEYS || (i - 1) >= MAX_KEYS)
        {
            perror("Exceeded maximum keys limit");
            return NULL;
        }
        leaf->keys[i] = leaf->keys[i - 1];
        leaf->pointers[i] = leaf->pointers[i - 1];
        i--;
    }

    leaf->keys[insertion_point] = key;
    leaf->pointers[insertion_point] = pointer;
    leaf->num_keys++;

    if (leaf->num_keys > MAX_KEYS)
    {
        perror("Exceeded maximum keys in leaf");
        leaf->num_keys--;
        return NULL;
    }

    return leaf;
}

Node *splitAndInsertInLeaf(B_treeInfo *tree_manager, Node *leaf, Value *key, NodeData *pointer)
{
    Node *new_leaf = allocateLeafNode(tree_manager);
    int bTreeOrder = tree_manager->order;
    int split;

    Value **temp_keys = malloc(bTreeOrder * sizeof(Value *));
    temp_keys = temp_keys ? temp_keys : (perror("Memory allocation failed for temporary keys"), exit(RC_INSERT_ERROR), NULL);

    void **temp_pointers = malloc(bTreeOrder * sizeof(void *));
    temp_pointers = temp_pointers ? temp_pointers : (perror("Memory allocation failed for temporary pointers"), free(temp_keys), exit(RC_INSERT_ERROR), NULL);

    int insertion_index;
    for (insertion_index = 0;
         insertion_index < leaf->num_keys && isKeyLess(leaf->keys[insertion_index], key);
         insertion_index++)
        ;

    memcpy(temp_keys, leaf->keys, insertion_index * sizeof(Value *));
    memcpy(temp_pointers, leaf->pointers, insertion_index * sizeof(void *));
    temp_keys[insertion_index] = key;
    temp_pointers[insertion_index] = pointer;
    memcpy(temp_keys + insertion_index + 1, leaf->keys + insertion_index, (leaf->num_keys - insertion_index) * sizeof(Value *));
    memcpy(temp_pointers + insertion_index + 1, leaf->pointers + insertion_index, (leaf->num_keys - insertion_index) * sizeof(void *));

    leaf->num_keys = 0;

    split = (bTreeOrder + 1) / 2;

    memcpy(leaf->keys, temp_keys, split * sizeof(Value *));
    memcpy(leaf->pointers, temp_pointers, split * sizeof(void *));
    leaf->num_keys = split;

    memcpy(new_leaf->keys, temp_keys + split, (bTreeOrder - split) * sizeof(Value *));
    memcpy(new_leaf->pointers, temp_pointers + split, (bTreeOrder - split) * sizeof(void *));
    new_leaf->num_keys = bTreeOrder - split;

    free(temp_keys);
    free(temp_pointers);

    new_leaf->pointers[bTreeOrder - 1] = leaf->pointers[bTreeOrder - 1];
    leaf->pointers[bTreeOrder - 1] = new_leaf;

    memset(&leaf->pointers[leaf->num_keys], 0, (bTreeOrder - 1 - leaf->num_keys) * sizeof(void *));
    memset(&new_leaf->pointers[new_leaf->num_keys], 0, (bTreeOrder - 1 - new_leaf->num_keys) * sizeof(void *));

    new_leaf->parent = leaf->parent;

    Value *new_key = new_leaf->keys[0];
    tree_manager->numEntries++;

    return addToParentNode(tree_manager, leaf, new_key, new_leaf);
}

Node *splitAndInsertInNode(B_treeInfo *tree_manager, Node *old_node, int left_index, Value *key, Node *right)
{
    int bTreeOrder = tree_manager->order;
    int split = (bTreeOrder + 1) / 2;
    Node *new_node = allocateBTreeNode(tree_manager);
    Node *child;
    Value **temp_keys = malloc(bTreeOrder * sizeof(Value *));
    Node **temp_pointers = malloc((bTreeOrder + 1) * sizeof(Node *));

    if (!temp_keys || !temp_pointers)
    {
        perror("Memory allocation failed for temporary arrays");
        free(temp_keys);
        free(temp_pointers);
        exit(RC_INSERT_ERROR);
    }

    memcpy(temp_pointers, old_node->pointers, left_index * sizeof(Node *));
    temp_pointers[left_index] = right;
    memcpy(temp_pointers + left_index + 1, old_node->pointers + left_index, (old_node->num_keys + 1 - left_index) * sizeof(Node *));

    memcpy(temp_keys, old_node->keys, left_index * sizeof(Value *));
    temp_keys[left_index] = key;
    memcpy(temp_keys + left_index + 1, old_node->keys + left_index, (old_node->num_keys - left_index) * sizeof(Value *));

    old_node->num_keys = 0;

    memcpy(old_node->pointers, temp_pointers, split * sizeof(Node *));
    memcpy(old_node->keys, temp_keys, (split - 1) * sizeof(Value *));
    old_node->num_keys = split - 1;

    Value *k_prime = temp_keys[split - 1];

    memcpy(new_node->pointers, temp_pointers + split, (bTreeOrder - split + 1) * sizeof(Node *));
    memcpy(new_node->keys, temp_keys + split, (bTreeOrder - split) * sizeof(Value *));
    new_node->num_keys = bTreeOrder - split;

    for (int i = 0; i <= new_node->num_keys; i++)
    {
        child = new_node->pointers[i];
        if (child)
            child->parent = new_node;
    }

    new_node->parent = old_node->parent;

    free(temp_pointers);
    free(temp_keys);

    tree_manager->numEntries++;

    return addToParentNode(tree_manager, old_node, k_prime, new_node);
}

int getLeftChildIndex(Node *parent, Node *left)
{
    if (!parent || !left)
    {
        perror("Invalid input: parent or left node is NULL.");
        return -1;
    }

    if (!parent->pointers)
    {
        perror("Invalid parent structure: pointers array is NULL.");
        return -1;
    }

    for (int index = 0; index <= parent->num_keys; index++)
    {
        if (parent->pointers[index] == left)
        {
            return index;
        }
    }

    return -1;
}

Node *insertKeyInNode(B_treeInfo *tree_manager, Node *parent, int left_index, Value *key, Node *right)
{
    int i = parent->num_keys;

    while (i >= left_index)
    {
        parent->pointers[i + 1] = parent->pointers[i];

        if (i > left_index)
        {
            parent->keys[i] = parent->keys[i - 1];
        }

        i--;
    }

    *(parent->pointers + left_index + 1) = right;
    *(parent->keys + left_index) = key;

    parent->num_keys += 1;

    return tree_manager->root;
}

Node *addNewRoot(B_treeInfo *tree_manager, Node *left, Value *key, Node *right)
{
    if (!tree_manager || !left || !key || !right)
    {
        perror("Invalid input: tree_manager, left, key, or right is NULL.");
        return NULL;
    }

    Node *root = allocateBTreeNode(tree_manager);
    if (!root)
    {
        perror("Failed to allocate memory for root node.");
        exit(RC_INSERT_ERROR);
    }

    *(root->keys) = key;
    *(root->pointers) = left;
    *(root->pointers + 1) = right;

    ++(root->num_keys);
    root->parent = NULL;

    left->parent = root;
    right->parent = root;

    return root;
}

Node *allocateBTreeNode(B_treeInfo *tree_manager)
{
    tree_manager->no_of_nodes++;

    int bTreeOrder = tree_manager->order;

    Node *new_node = malloc(sizeof(Node));
    new_node = new_node ? new_node : (perror("Failed to create node."), exit(RC_INSERT_ERROR), NULL);

    new_node->keys = malloc((bTreeOrder - 1) * sizeof(Value *));
    new_node->keys = new_node->keys ? new_node->keys : (perror("Failed to allocate memory for keys array."), free(new_node), exit(RC_INSERT_ERROR), NULL);

    new_node->pointers = malloc(bTreeOrder * sizeof(void *));
    new_node->pointers = new_node->pointers ? new_node->pointers : (perror("Failed to allocate memory for pointers array."), free(new_node->keys), free(new_node), exit(RC_INSERT_ERROR), NULL);

    *(&new_node->is_leaf) = false;
    *(&new_node->num_keys) = 0;
    *(&new_node->parent) = NULL;
    *(&new_node->next) = NULL;

    return new_node;
}

Node *allocateLeafNode(B_treeInfo *tree_manager)
{
    Node *leaf = allocateBTreeNode(tree_manager);

    *leaf = (Node){
        .keys = leaf->keys,
        .pointers = leaf->pointers,
        .is_leaf = true,
        .num_keys = 0,
        .parent = NULL,
        .next = NULL};

    return leaf;
}

Node *searchLeaf(Node *root, Value *key)
{
    Node *current = root;

    if (!current)
    {
        return NULL;
    }

    while (!current->is_leaf)
    {
        if (!current->keys || !current->pointers)
        {
            fprintf(stderr, "Invalid node structure: keys or pointers array is NULL.\n");
            return NULL;
        }

        int i = 0;

        while (i < current->num_keys)
        {
            if (!isKeyGreater(key, current->keys[i]) && !areKeysEqual(key, current->keys[i]))
            {
                break;
            }
            i++;
        }

        current = (Node *)current->pointers[i];
    }

    return current;
}

NodeData *searchRecord(Node *root, Value *key)
{

    Node *current = searchLeaf(root, key);

    if (!current)
    {
        return NULL;
    }

    if (!current->keys || !current->pointers)
    {
        fprintf(stderr, "Invalid leaf node structure: keys or pointers array is NULL.\n");
        return NULL;
    }

    for (int i = 0; i < current->num_keys; ++i)
    {
        if (areKeysEqual(current->keys[i], key))
        {
            return (NodeData *)current->pointers[i];
        }
    }

    return NULL;
}

int findSiblingIndex(Node *node)
{
    if (!node->parent)
    {
        fprintf(stderr, "Error: Node has no parent.\n");
        exit(RC_ERROR);
    }

    int i = 0;
    while (i <= node->parent->num_keys && node->parent->pointers[i] != node)
    {
        i++;
    }

    return (i <= node->parent->num_keys) ? i - 1 : (fprintf(stderr, "Error: Node not found in parent's pointers. Node address: %#lx\n", (unsigned long)node), exit(RC_ERROR), -1);

    fprintf(stderr, "Error: Node not found in parent's pointers. Node address: %#lx\n", (unsigned long)node);
    exit(RC_ERROR);
}

Node *removeEntry(B_treeInfo *tree_manager, Node *n, Value *key, Node *pointer)
{
    int i = 0;
    int bTreeOrder = tree_manager->order;

    for (; i < n->num_keys && !areKeysEqual(n->keys[i], key); i++)
        ;

    for (int j = i + 1; j < n->num_keys; j++)
    {
        n->keys[j - 1] = n->keys[j];
    }

    int num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
    i = 0;

    for (; i < num_pointers && n->pointers[i] != pointer; i++)
        ;

    for (int j = i + 1; j < num_pointers; j++)
    {
        n->pointers[j - 1] = n->pointers[j];
    }

    n->num_keys--;
    tree_manager->numEntries--;

    int start_index = n->is_leaf ? n->num_keys : n->num_keys + 1;
    for (int j = start_index; j < bTreeOrder - 1; j++)
    {
        n->pointers[j] = NULL;
    }

    return n;
}

Node *restructureRoot(Node *root)
{
    Node *new_root = NULL;

    while (root->num_keys == 0)
    {
        for (; !root->is_leaf;)
        {
            new_root = root->pointers[0];
            new_root->parent = NULL;
            break;
        }
        break;
    }

    for (; new_root == NULL;)
    {
        return root;
    }

    free(root->keys);
    free(root->pointers);
    free(root);

    return new_root;
}

Node *mergeNodes(B_treeInfo *tree_manager, Node *n, Node *neighbor, int neighbor_index, int k_prime)
{
    if (!tree_manager || !n || !neighbor)
    {
        perror("Invalid input: tree_manager, n, or neighbor is NULL.");
        return NULL;
    }

    int i, j, neighbor_insertion_index, n_end;
    Node *tmp;
    int bTreeOrder = tree_manager->order;

    if (bTreeOrder <= 0)
    {
        perror("Invalid tree order.");
        return NULL;
    }

    if (neighbor_index == -1)
    {
        tmp = n;
        n = neighbor;
        neighbor = tmp;
    }

    neighbor_insertion_index = neighbor->num_keys;

    if (!neighbor->keys || !neighbor->pointers)
    {
        perror("Invalid neighbor structure: keys or pointers array is NULL.");
        return NULL;
    }

    if (!n->is_leaf)
    {
        if (!n->keys || !n->pointers)
        {
            perror("Invalid node structure: keys or pointers array is NULL.");
            return NULL;
        }

        neighbor->keys[neighbor_insertion_index] = k_prime;
        neighbor->num_keys++;

        n_end = n->num_keys;

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++)
        {
            if (i >= bTreeOrder || j >= n->num_keys)
            {
                perror("Index out of bounds during key and pointer transfer.");
                return NULL;
            }

            neighbor->keys[i] = n->keys[j];
            neighbor->pointers[i] = n->pointers[j];
            neighbor->num_keys++;
            n->num_keys--;
        }

        neighbor->pointers[i] = n->pointers[j];

        for (i = 0; i < neighbor->num_keys + 1; i++)
        {
            tmp = (Node *)neighbor->pointers[i];
            if (!tmp)
            {
                perror("Invalid pointer in neighbor's pointers array.");
                return NULL;
            }
            tmp->parent = neighbor;
        }
    }
    else
    {
        for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++)
        {
            if (i >= bTreeOrder - 1 || j >= n->num_keys)
            {
                perror("Index out of bounds during leaf merge.");
                return NULL;
            }

            neighbor->keys[i] = n->keys[j];
            neighbor->pointers[i] = n->pointers[j];
            neighbor->num_keys++;
        }

        neighbor->pointers[bTreeOrder - 1] = n->pointers[bTreeOrder - 1];
    }

    tree_manager->root = removeKeyFromNode(tree_manager, n->parent, k_prime, n);

    if (n->keys)
        free(n->keys);
    if (n->pointers)
        free(n->pointers);
    free(n);

    return tree_manager->root;
}

Node *removeKeyFromNode(B_treeInfo *tree_manager, Node *n, Value *key, void *pointer)
{
    int bTreeOrder = tree_manager->order;
    int min_keys, neighbor_index, k_prime_index, k_prime, capacity;
    Node *neighbor;

    n = removeEntry(tree_manager, n, key, pointer);

    Node *result = (n == tree_manager->root) ? restructureRoot(tree_manager->root) : n;
    if (result != n)
    {
        return result;
    }

    min_keys = (n->is_leaf)
                   ? ((bTreeOrder - 1) / 2 + ((bTreeOrder - 1) % 2 != 0))
                   : ((bTreeOrder / 2) - 1 + (bTreeOrder % 2 != 0));

    if (n == tree_manager->root)
    {
        return restructureRoot(tree_manager->root);
    }

    neighbor_index = findSiblingIndex(n);
    k_prime_index = (neighbor_index == -1) ? 0 : neighbor_index;
    k_prime = n->parent->keys[k_prime_index];

    neighbor = (neighbor_index == -1)
                   ? n->parent->pointers[1]
                   : n->parent->pointers[neighbor_index];

    capacity = (n->is_leaf) ? bTreeOrder : bTreeOrder - 1;

    return (neighbor->num_keys + n->num_keys < capacity)
               ? mergeNodes(tree_manager, n, neighbor, neighbor_index, k_prime)
               : redistributeNodes(tree_manager->root, n, neighbor, neighbor_index, k_prime_index, k_prime);
}

Node *delete(B_treeInfo *tree_manager, Value *key)
{

    Node *record = tree_manager->root ? searchRecord(tree_manager->root, key) : NULL;
    NodeData *key_leaf = tree_manager->root ? searchLeaf(tree_manager->root, key) : NULL;

    tree_manager->root = (record && key_leaf) ? removeKeyFromNode(tree_manager, key_leaf, key, record) : tree_manager->root;

    record ? free(record) : (void)0;

    return tree_manager->root;
}

Node *redistributeNodes(Node *root, Node *n, Node *neighbor, int neighbor_index, int k_prime_index, int k_prime)
{
    int i;
    Node *tmp;

    if (neighbor_index != -1)
    {
        n->pointers[n->num_keys + 1] = n->is_leaf ? NULL : n->pointers[n->num_keys];
        i = n->num_keys;
        while (i > 0)
        {
            n->keys[i] = n->keys[i - 1];
            n->pointers[i] = n->pointers[i - 1];
            i--;
        }

        n->is_leaf
            ? (n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1],
               neighbor->pointers[neighbor->num_keys - 1] = NULL,
               n->keys[0] = neighbor->keys[neighbor->num_keys - 1],
               n->parent->keys[k_prime_index] = n->keys[0])
            : (n->pointers[0] = neighbor->pointers[neighbor->num_keys],
               tmp = (Node *)n->pointers[0],
               tmp->parent = n,
               neighbor->pointers[neighbor->num_keys] = NULL,
               n->keys[0] = k_prime,
               n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1]);
    }
    else
    {

        n->keys[n->num_keys] = n->is_leaf ? neighbor->keys[0] : k_prime;
        n->pointers[n->num_keys + (n->is_leaf ? 0 : 1)] = n->is_leaf ? neighbor->pointers[0] : neighbor->pointers[0];
        n->parent->keys[k_prime_index] = n->is_leaf ? neighbor->keys[1] : neighbor->keys[0];

        tmp = n->is_leaf ? NULL : (Node *)n->pointers[n->num_keys + 1];
        tmp ? (tmp->parent = n) : (void)0;

        i = 0;
        while (i < neighbor->num_keys - 1)
        {
            neighbor->keys[i] = neighbor->keys[i + 1];
            neighbor->pointers[i] = neighbor->pointers[i + 1];
            i++;
        }

        n->is_leaf ? (void)0 : (neighbor->pointers[i] = neighbor->pointers[i + 1]);
    }

    n->num_keys++;
    neighbor->num_keys--;

    return root;
}

void addToQueue(B_treeInfo *tree_manager, Node *new_node)
{
    Node *current = tree_manager->queue;

    new_node->next = NULL;

    tree_manager->queue = tree_manager->queue ? tree_manager->queue : new_node;

    while (current && current->next)
    {
        current = current->next;
    }

    current ? (current->next = new_node) : (void)0;
}

Node *removeFromQueue(B_treeInfo *tree_manager)
{
    Node *node_to_dequeue = tree_manager->queue;

    if (!tree_manager)
    {
        fprintf(stderr, "Error: tree_manager is NULL in removeFromQueue function.\n");
        return NULL;
    }

    if (!tree_manager->queue)
    {
        fprintf(stderr, "Error: Queue is empty in removeFromQueue function.\n");
        return NULL;
    }

    tree_manager->queue = node_to_dequeue ? node_to_dequeue->next : NULL;

    node_to_dequeue ? (node_to_dequeue->next = NULL) : (void)0;

    return node_to_dequeue;
}

int root_path(Node *root, Node *child)
{
    int length = 0;

    for (Node *current = child; current != root; current = current->parent)
    {
        length++;
    }

    return length;
}

bool isKeyLess(Value *key1, Value *key2)
{
    return (key1->dt == DT_INT) ? (key1->v.intV < key2->v.intV) : (key1->dt == DT_FLOAT) ? (key1->v.floatV < key2->v.floatV)
                                                              : (key1->dt == DT_STRING)  ? (strcmp(key1->v.stringV, key2->v.stringV) < 0)
                                                                                         : FALSE;
}

bool isKeyGreater(Value *key1, Value *key2)
{
    return (key1->dt == DT_INT) ? (key1->v.intV > key2->v.intV) : (key1->dt == DT_FLOAT) ? (key1->v.floatV > key2->v.floatV)
                                                              : (key1->dt == DT_STRING)  ? (strcmp(key1->v.stringV, key2->v.stringV) > 0)
                                                                                         : FALSE;
}

bool areKeysEqual(Value *key1, Value *key2)
{
    return (key1->dt == DT_INT) ? (key1->v.intV == key2->v.intV) : (key1->dt == DT_FLOAT) ? (key1->v.floatV == key2->v.floatV)
                                                               : (key1->dt == DT_STRING)  ? (strcmp(key1->v.stringV, key2->v.stringV) == 0)
                                                               : (key1->dt == DT_BOOL)    ? (key1->v.boolV == key2->v.boolV)
                                                                                          : FALSE;
}