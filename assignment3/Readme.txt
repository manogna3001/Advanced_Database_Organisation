CS525 Advanced Database Organization
Assignment 3 - Record Manager

Group 27:
Team Members:
Choladevi Gheereddy
Tejaswini Bollepalli 
Manogna Vadlamudi 

Code Structure:

assign3
	buffer_mgr_stat.c
	buffer_mgr_stat.h
	buffer_mgr.c
	buffer_mgr.h
	dberror.c
	dberror.h
	dt.h
	expr.c
	expr.h
	makefile
	Readme.txt
	record_mgr.c
	record_mgr.h
	rm_serializer.c
	storage_mgr.c
	storage_mgr.h
	tables.h
	test_assign3_1.c
	test_expr.c
	test_helper.h

Instructions to run makefile:
1. Move to assign3 folder where makefile exists.
2. Open assign3 folder in terminal and type 'make';
3. It will compile code and create .o and .exec files.
4. Type 'make run' to run test_assign3 executable.
5. Type 'make test_expr' to create text_expr.o and test_expr.exe file.
6. Type 'make run1' to run text_expr.exe executable.
7. Type 'make clean' to delete all the previously created  .o .exe files.

Interface Implementation:

1. extern RC initRecordManager(void *mgmtData):The initRecordManager function takes void *mgmtData as a parameter, initializes the storage manager by calling initStorageManager, and checks if mgmtData is not NULL. If mgmtData is non-NULL, it returns an error code (RC_ERROR); otherwise, it returns success (RC_OK). This ensures proper initialization without extra data.

2. extern RC shutdownRecordManager(): The shutdownRecordManager function checks if the global recordManager is NULL and, if so, returns success (RC_OK). If recordManager is not NULL, it frees the allocated memory, sets recordManager to NULL, and then returns success (RC_OK). This ensures proper cleanup of resources without input parameters.

3. extern RC createTable(char *name, Schema *schema):The createTable function takes char *name (table name) and Schema *schema (table schema) as parameters, allocates memory for the recordManager, and initializes the buffer pool. It prepares the first page with metadata (tuple count, first page, schema attributes) and performs file operations to create, open, write to, and close the page file. The function returns success (RC_OK) or an error code based on the outcome of each operation.

4. extern RC openTable(RM_TableData *rel, char *name): The openTable function takes RM_TableData *rel (table data) and char *name (table name) as parameters, associates the table's management data (mgmtData) with the recordManager, and pins the first page to retrieve metadata like tuple count and schema attributes. It allocates memory for the schema, copies attribute names, data types, and lengths from the page, and sets this schema in the table data (rel->schema). The function unpins and forces changes to the page, returning success (RC_OK) or an error code based on the operation results.

5. extern RC closeTable(RM_TableData *rel): The closeTable function takes RM_TableData *rel as a parameter, representing the table to close. It checks if rel and its management data (mgmtData) are valid, and if so, it shuts down the associated buffer pool. After releasing resources, it returns success (RC_OK).

6. extern RC deleteTable(char *name): The deleteTable function takes char *name as a parameter, representing the name of the table to delete. It checks if the name is NULL, returning an error (RC_ERROR) if so, and then attempts to delete the page file using destroyPageFile(name). If the deletion is successful, it returns success (RC_OK), otherwise, it returns the error status.

7. extern int getNumTuples(RM_TableData *rel):The getNumTuples function takes RM_TableData *rel as a parameter, representing the table, and checks if rel or its management data (mgmtData) is NULL, returning -1 for an error. It retrieves the number of tuples from the RecordManager and returns the tuple count, or -1 if the count is invalid (less than 0).

8. extern RC insertRecord(RM_TableData *rel, Record *record):The insertRecord function takes RM_TableData *rel (table data) and Record *record as parameters, assigns the record's page and slot, and searches for a free slot in the current page. It pins pages, finds an available slot, marks the slot as used, inserts the record data into the slot, and unpins the page. The function updates the tuple count and returns success (RC_OK) or an error code depending on the outcome of each operation.

9. extern RC deleteRecord(RM_TableData *rel, RID id):The deleteRecord function takes RM_TableData *rel (table data) and RID id (record identifier) as parameters. It pins the specified page, finds the record at the given slot, and marks it as deleted by setting the slot's data to '-'. After marking the page as dirty, it unpins the page and returns success (RC_OK) or an error code depending on the result of each operation.

10. extern RC updateRecord(RM_TableData *rel, Record *record): The updateRecord function takes RM_TableData *rel (table data) and Record *record as parameters. It pins the page where the record is located, updates the record's data in the specified slot, marks the page as dirty, and then unpins the page. The function returns success (RC_OK) or an error code based on the result of the operations.

11. extern RC getRecord(RM_TableData *rel, RID id, Record *record): The getRecord function takes RM_TableData *rel (table data), `RID id` (record identifier), and `Record *record` as parameters. It pins the page containing the record, checks if the record exists in the specified slot, and if found, copies the record data into `record`. After retrieving the data, it unpins the page and returns success (`RC_OK`) or an error code if any step fails.

12. extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond): The startScan function takes RM_TableData *rel (table), RM_ScanHandle *scan (scan handle), and Expr *cond (scan condition) as parameters. It opens the table, allocates memory for the scan manager, and initializes the scan with the given condition (cond). It assigns the scan manager to scan->mgmtData and returns success (RC_OK) or an error code if any operation fails.

13. extern RC next (RM_ScanHandle *scan, Record *record): The `next` function takes `RM_ScanHandle *scan` (scan handle) and `Record *record` as parameters. It iterates through the table's records by scanning each page and slot, evaluating the scan condition (`scanManager->expr`) on each record. If a matching record is found, it returns success (`RC_OK`), otherwise, it continues scanning until all records are processed, returning `RC_RM_NO_MORE_TUPLES` when no more records are found.

14. extern RC closeScan(RM_ScanHandle *scan): The closeScan function takes RM_ScanHandle *scan as a parameter and checks if scan and its management data (mgmtData) are valid. It unpins the page if a scan is in progress, resets the scan manager's state, frees the allocated memory for scan->mgmtData, and sets it to NULL. The function then returns success (RC_OK).

15. extern int getRecordSize(Schema *schema):The `getRecordSize` function takes `Schema *schema` as a parameter and calculates the total size of a record based on the schema's attributes. It iterates through each attribute, adding the appropriate size based on its data type (string, int, float, or bool). The function returns the total record size plus one byte for record management.

16. extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys): The `createSchema` function takes parameters including `numAttr` (number of attributes), `attrNames` (names of the attributes), `dataTypes` (data types of each attribute), `typeLength` (length of each attribute type), `keySize` (number of key attributes), and `keys` (key attribute indices). It allocates memory for the schema and its components, then copies the attribute and key information into the schema structure. If memory allocation fails, it cleans up and returns `NULL`; otherwise, it returns the newly created `Schema`.

17. extern RC freeSchema(Schema *schema): The freeSchema function takes Schema *schema as a parameter and checks if it's not NULL. It frees the dynamically allocated memory for the schema's components: attrNames, dataTypes, typeLength, and keyAttrs. After freeing these components, it frees the schema itself and returns success (RC_OK).

18. extern RC createRecord(Record **record, Schema *schema):The createRecord function takes Record **record (a pointer to store the newly created record) and Schema *schema (describing the record's structure) as parameters. It first checks if both parameters are valid, then allocates memory for a new Record and its data based on the calculated record size from the schema. If memory allocation is successful, it initializes the record's page and slot IDs to -1, marks the record as unused (by setting the first byte of data to '-'), and assigns the Record pointer to the provided record. It returns RC_OK on success or RC_ERROR if any step fails.

19. extern RC freeRecord(Record *record): The freeRecord function takes Record *record as a parameter and checks if the record is NULL. If it's not NULL, it frees the memory allocated for the record->data and the record itself. Finally, it returns success (RC_OK) or an error if the record is NULL.

20. RC attrDtOffset(Schema *schema, int attrNum, int *result): The attrDtOffset function takes Schema *schema (record schema), int attrNum (attribute index), and int *result (pointer to store the offset) as parameters. It calculates the byte offset for the specified attribute (attrNum) by summing the sizes of preceding attributes based on their data types. The calculated offset is stored in *result, and the function returns success (RC_OK).

21. extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value):The `getAttr` function takes `Record *record`, `Schema *schema`, `int attrNum` (attribute index), and `Value **value` (output attribute value) as parameters. It calculates the offset for the attribute using `attrDtOffset`, retrieves the attribute's value based on its data type (string, int, float, or bool), and stores it in a dynamically allocated `Value` structure. If successful, it assigns the `Value` to `*value` and returns success (`RC_OK`); otherwise, it returns an error.
 
22. extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value):The `setAttr` function takes `Record *record`, `Schema *schema`, `int attrNum` (attribute index), and `Value *value` (new value to set) as parameters. It calculates the offset of the attribute within the record and updates the attribute's value at the calculated position based on its data type (string, int, float, or bool). The function returns success (`RC_OK`) or an error code if any operation fails.
