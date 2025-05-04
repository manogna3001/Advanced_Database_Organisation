CS525 Advanced Database Organization
Assignment 4 - B+ tree

Group 27:
Team Members:
Choladevi Gheereddy
Tejaswini Bollepalli 
Manogna Vadlamudi 

Code Structure:

assign4
	btree_implement.c
	btree_implement.h
	btree_mgr.c
	btree_mgr.h
	buffer_mgr_stat.c
	buffer_mgr_stat.h
	buffer_mgr.c
	buffer_mgr.h
	dberror.c
	dberror.h
	dt.h
	expr.c
	expr.h
	Makefile
	Readme.txt
	record_mgr.c
	record_mgr.h
	rm_serializer.c
	storage_mgr.c
	storage_mgr.h
	tables.h
	test_assign4_1.c
	test_expr.c
	test_helper.h

Instructions to run makefile:
1. Move to assign4 folder where makefile exists.
2. Open assign4 folder in terminal and type 'make';
3. It will compile code and create .o and .exec files.
4. Type 'make run' to run test_assign4 executable.
6. Type 'make run1' to run text_expr.exe executable.
7. Type 'make clean' to delete all the previously created  .o .exe files.

Interface Implementation:

1.RC initIndexManager(void *mgmtData): The initIndexManager function sets up an index manager by calling initStorageManager() to handle storage needs. It takes one parameter, mgmtData, which points to any additional management data required for the setup. If everything initializes correctly, the function returns RC_OK.

2. RC shutdownIndexManager(): The shutdownIndexManager function shuts down the index manager by clearing out tree_manager, setting it to a `NULL` pointer to remove the B-tree manager instance. It doesn’t require any input parameters and returns RC_OK to confirm a successful shutdown.

3.RC createBtree(char *idxId, DataType keyType, int n): The createBtree function sets up a new B-tree with a unique identifier , a specified key type , and a node order . It first checks that n doesn’t exceed the maximum nodes allowed per page, then creates a B_treeInfo structure with the appropriate order and key type, and initializes a buffer pool. Afterward, it creates a file for the B-tree, writes an empty block to start, and closes the file. If everything goes smoothly, it returns RC_OK; otherwise, it returns an error code.

4.RC openBtree(BTreeHandle **tree, char *idxId): The openBtree function opens an existing B-tree using its identifier  and sets up a BTreeHandle to manage it, linking tree_manager as its data. It first confirms that tree and idxId are valid, then allocates memory for *tree, returning an error if it runs out of memory. Finally, it sets up a buffer pool for tree_manager with space for 1000 pages using a FIFO replacement strategy. If everything works as expected, it returns RC_OK; otherwise, it returns an error code.

5.RC closeBtree(BTreeHandle *tree): The closeBtree function safely closes an open B-tree specified by tree. It first checks that both tree and its management data are valid, then marks the buffer pool as “dirty” to save any pending changes. After shutting down the buffer pool, it frees the memory used by tree_manager and tree. If everything closes properly, it returns RC_OK; if any essential data is missing, it returns RC_ERROR.

6.RC deleteBtree(char *idxId): The deleteBtree function deletes a B-tree by calling destroyPageFile with the B-tree’s file identifier (idxId). It tries to remove the file associated with this B-tree and returns RC_OK if successful or RC_FILE_NOT_FOUND if the file couldn’t be found. The function takes one parameter, idxId, which is the unique identifier for the B-tree file.

7.RC getNumNodes(BTreeHandle *tree, int *result): The getNumNodes function retrieves the number of nodes in a B-tree specified by tree and stores it in result. It checks if tree, result, and tree->mgmtData are valid, then assigns no_of_nodes from tree_manager to *result. The function returns RC_OK if successful, or RC_ERROR if any pointers are NULL or no_of_nodes is invalid.

8.RC getNumEntries(BTreeHandle *tree, int *result): The getNumEntries function gets the total number of entries in a B-tree identified by tree and stores it in result. It first ensures that tree, result, and the management data are valid, then assigns the entry count from tree_manager to *result. If successful, it returns RC_OK; otherwise, it returns RC_ERROR if any pointers are NULL or the entry count is invalid.

9.RC getKeyType(BTreeHandle *tree, DataType *result): The getKeyType function retrieves the key type of a B-tree specified by the tree parameter and stores it in result. It first checks if tree, result, and tree->mgmtData are valid pointers, returning RC_ERROR if any are NULL. If valid, it casts tree->mgmtData to B_treeInfo and assigns its keyType to *result, returning RC_OK to indicate success.

10.extern RC findKey(BTreeHandle *tree, Value *key, RID *result): The findKey function looks for a specific key in a B-tree identified by tree and, if found, stores the result in result. It first checks that tree, result, and the management data are all valid, then uses searchRecord to find the key in the B-tree’s root. If it finds the record, it assigns the record ID (rid) to *result and returns RC_OK; if not, it returns RC_IM_KEY_NOT_FOUND.

11.RC insertKey(BTreeHandle *tree, Value *key, RID rid): The insertKey function adds a key and its record ID (RID) to a B-tree specified by tree. It first confirms that tree_manager is valid and retrieves the B-tree’s order to check capacity, ensuring the key isn’t already there. If the tree is empty, it sets up a root node; otherwise, it locates the correct leaf to insert the key. If the leaf is full, it splits the node to fit the new key. On success, it returns RC_OK; if there’s an issue, it returns RC_ERROR.

12.RC deleteKey(BTreeHandle *tree, Value *key): The deleteKey function removes a specified key from a B-tree identified by tree. It verifies that tree and tree->mgmtData are valid, then calls delete to remove the key, updating tree_manager->root to newRoot. The function returns RC_OK if successful or RC_IM_KEY_NOT_FOUND if the key was not found.

13.RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle):The openTreeScan function sets up a scan on a B-tree specified by tree, storing the scan’s details in handle. It ensures that tree, handle, and the management data are all valid, then allocates memory for scandata and handle. It positions scandata to start at the leftmost leaf node so that it can scan through the B-tree sequentially. If everything is set up successfully, it returns RC_OK; otherwise, it returns an error if memory allocation fails or there are no records to scan.

14.RC nextEntry(BT_ScanHandle *handle, RID *result): The nextEntry function retrieves the next record ID (RID) from a B-tree scan specified by handle, storing it in result. It accesses scandata to check if there are more entries in the current node; if so, it updates keyIndex and returns the RID. If no more entries are available, it moves to the next leaf node or returns RC_IM_NO_MORE_ENTRIES if the scan is complete.

15.extern RC closeTreeScan(BT_ScanHandle *handle): The closeTreeScan function closes an ongoing scan on a B-tree by releasing any resources tied to it. It first checks if handle is valid, clears handle->mgmtData by setting it to NULL, and then frees up shandle itself. The function returns `RC_OK` if everything is closed properly, and it logs a warning if handle or mgmtData was already NULL.

16.extern char *printTree(BTreeHandle *tree): The printTree function displays the layout and contents of a B-tree, level by level, starting from the root. It sets up a queue to manage nodes and prints each node’s keys along with their associated record IDs , adapting the display based on the type of key (integer, float, string, or boolean). Helper functions like addToQueue and removeFromQueue help with traversing the tree. Once finished, the function returns NULL.

