#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

// record manager structure
typedef struct RecordManager
{
    BM_PageHandle pHandle;
    RID r_ID;
    int first_page;
    int num_scan;
    Expr *expr;
    int num_tup;
    BM_BufferPool buffer_pool;
} RecordManager;

const int max_pages = 100;
RecordManager *recordManager;
const int attr_len = 15;

// Initialize record manager
extern RC initRecordManager(void *mgmtData)
{
    initStorageManager();

    if (mgmtData != NULL)
    {
        return RC_ERROR;
    }
    return RC_OK;
}

// function to shutdown record manager
extern RC shutdownRecordManager()
{
    if (recordManager == NULL)
    {
        return RC_OK;
    }
    else
    {
        free(recordManager);
        recordManager = NULL;
    }
    return RC_OK;
}

// function to create table
extern RC createTable(char *name, Schema *schema)
{
    recordManager = (RecordManager *)malloc(sizeof(RecordManager));

    if (recordManager == NULL)
        return RC_ERROR;

    RC status = initBufferPool(&recordManager->buffer_pool, name, max_pages, RS_LRU, NULL);
    if (status != RC_OK)
        return status;

    char data[PAGE_SIZE];
    char *pHandle = data;

    int tupleCount = 0, firstPage = 1;
    memcpy(pHandle, &tupleCount, sizeof(int));
    pHandle += sizeof(int);

    memcpy(pHandle, &firstPage, sizeof(int));
    pHandle += sizeof(int);

    int numAttr = schema->numAttr;
    int keySize = schema->keySize;
    memcpy(pHandle, &numAttr, sizeof(int));
    pHandle += sizeof(int);

    memcpy(pHandle, &keySize, sizeof(int));
    pHandle += sizeof(int);

    for (int k = 0; k < schema->numAttr; k++)
    {
        strncpy(pHandle, schema->attrNames[k], attr_len);
        pHandle += attr_len;

        int dataType = (int)schema->dataTypes[k];
        int typeLength = (int)schema->typeLength[k];
        memcpy(pHandle, &dataType, sizeof(int));
        pHandle += sizeof(int);

        memcpy(pHandle, &typeLength, sizeof(int));
        pHandle += sizeof(int);
    }

    SM_FileHandle fileHandle;

    status = createPageFile(name);
    if (status != RC_OK)
        return status;

    status = openPageFile(name, &fileHandle);
    if (status != RC_OK)
        return status;

    status = writeBlock(0, &fileHandle, data);
    if (status != RC_OK)
        return status;

    status = closePageFile(&fileHandle);
    if (status != RC_OK)
        return status;

    return RC_OK;
}

// function to open table
extern RC openTable(RM_TableData *rel, char *name)
{
    SM_PageHandle pHandle;
    int attributeCount, k;

    if (recordManager == NULL)
    {
        return RC_ERROR;
    }

    if (name == NULL)
    {
        return RC_ERROR;
    }

    rel->mgmtData = recordManager;
    rel->name = name;

    RC status = pinPage(&recordManager->buffer_pool, &recordManager->pHandle, 0);
    if (status != RC_OK)
    {
        return status;
    }

    pHandle = (char *)recordManager->pHandle.data;
    if (pHandle == NULL)
    {
        return RC_ERROR;
    }

    recordManager->num_tup = *(int *)pHandle;
    pHandle += sizeof(int);

    recordManager->first_page = *(int *)pHandle;
    pHandle += sizeof(int);

    attributeCount = *(int *)pHandle;
    pHandle += sizeof(int);

    if (attributeCount <= 0)
    {
        return RC_ERROR;
    }

    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL)
    {
        return RC_ERROR;
    }

    schema->numAttr = attributeCount;
    schema->attrNames = (char **)malloc(sizeof(char *) * attributeCount);
    schema->dataTypes = (DataType *)malloc(sizeof(DataType) * attributeCount);
    schema->typeLength = (int *)malloc(sizeof(int) * attributeCount);

    if (schema->attrNames == NULL || schema->dataTypes == NULL || schema->typeLength == NULL)
    {
        return RC_ERROR;
    }

    for (k = 0; k < attributeCount; k++)
    {
        schema->attrNames[k] = (char *)malloc(attr_len);
        if (schema->attrNames[k] == NULL)
        {
            return RC_ERROR;
        }
    }

    for (k = 0; k < schema->numAttr; k++)
    {
        memcpy(schema->attrNames[k], pHandle, attr_len);
        pHandle += attr_len;

        memcpy(&schema->dataTypes[k], pHandle, sizeof(int));
        pHandle += sizeof(int);

        memcpy(&schema->typeLength[k], pHandle, sizeof(int));
        pHandle += sizeof(int);
    }

    rel->schema = schema;

    status = unpinPage(&recordManager->buffer_pool, &recordManager->pHandle);
    if (status != RC_OK)
    {
        return status;
    }

    status = forcePage(&recordManager->buffer_pool, &recordManager->pHandle);
    if (status != RC_OK)
    {
        return status;
    }
    return RC_OK;
}

// function to close table
extern RC closeTable(RM_TableData *rel)
{
    if (rel && rel->mgmtData)
    {
        shutdownBufferPool(&((RecordManager *)rel->mgmtData)->buffer_pool);
    }

    return RC_OK;
}

// functio to delete table
extern RC deleteTable(char *name)
{
    if (name == NULL)
    {
        return RC_ERROR;
    }

    RC status = destroyPageFile(name);
    if (status != RC_OK)
    {
        return status;
    }
    return RC_OK;
}

// fucntion to get number of tuples
extern int getNumTuples(RM_TableData *rel)
{
    if (rel == NULL || rel->mgmtData == NULL)
    {
        return -1;
    }

    RecordManager *recordManager = (RecordManager *)rel->mgmtData;

    if (recordManager->num_tup < 0)
    {
        return -1;
    }

    return recordManager->num_tup;
}

// function to insert record
extern RC insertRecord(RM_TableData *rel, Record *record)
{
    RecordManager *recordManager = (RecordManager *)rel->mgmtData;

    RID *r_ID = &(record->id);
    char *slotPointer = NULL;
    int recordSize = getRecordSize(rel->schema);
    char *data = NULL;

    r_ID->page = recordManager->first_page;

    RC pinStatus = pinPage(&recordManager->buffer_pool, &recordManager->pHandle, r_ID->page);
    if (pinStatus != RC_OK)
    {
        return pinStatus;
    }

    while (1)
    {
        data = (char *)recordManager->pHandle.data;

        int slot_cnt = PAGE_SIZE / recordSize;

        for (int i = 0; i < slot_cnt; i++)
        {
            slotPointer = &data[i * recordSize];

            if ((*slotPointer & 0xFF) != '+')
            {
                r_ID->slot = i;
                goto foundFreeSlot;
            }
        }

        RC unpinStatus = unpinPage(&recordManager->buffer_pool, &recordManager->pHandle);
        if (unpinStatus != RC_OK)
        {
            return unpinStatus;
        }

        r_ID->page++;

        pinStatus = pinPage(&recordManager->buffer_pool, &recordManager->pHandle, r_ID->page);
        if (pinStatus != RC_OK)
        {
            return pinStatus;
        }
    }

foundFreeSlot:

    *slotPointer = '+';

    RC markStatus = markDirty(&recordManager->buffer_pool, &recordManager->pHandle);
    if (markStatus != RC_OK)
    {
        return markStatus;
    }

    memcpy(slotPointer + 1, record->data + 1, recordSize - 1);
    RC unpinStatus = unpinPage(&recordManager->buffer_pool, &recordManager->pHandle);
    if (unpinStatus != RC_OK)
    {
        return unpinStatus;
    }

    recordManager->num_tup++;
    pinStatus = pinPage(&recordManager->buffer_pool, &recordManager->pHandle, 0);
    if (pinStatus != RC_OK)
    {
        return pinStatus;
    }

    return RC_OK;
}

// function to delete record
extern RC deleteRecord(RM_TableData *rel, RID id)
{
    int recordSize = getRecordSize(rel->schema);
    if (rel == NULL)
    {
        return RC_ERROR;
    }

    RecordManager *recordManager = rel->mgmtData;
    if (recordManager == NULL)
    {
        return RC_ERROR;
    }

    RC status = pinPage(&recordManager->buffer_pool, &recordManager->pHandle, id.page);
    if (status != RC_OK)
    {
        return status;
    }

    if (recordManager->first_page > id.page)
    {
        recordManager->first_page = id.page;
    }

    char *data = recordManager->pHandle.data;

    data += (id.slot * recordSize);
    *data = '-';
    status = markDirty(&recordManager->buffer_pool, &recordManager->pHandle);
    if (status != RC_OK)
    {
        unpinPage(&recordManager->buffer_pool, &recordManager->pHandle);
        return status;
    }

    status = unpinPage(&recordManager->buffer_pool, &recordManager->pHandle);
    if (status != RC_OK)
    {
        return status;
    }
    return RC_OK;
}

// function to update record
extern RC updateRecord(RM_TableData *rel, Record *record)
{
    char *data;
    RID id = record->id;
    int recordSize = getRecordSize(rel->schema);

    if (rel == NULL || record == NULL)
    {
        return RC_ERROR;
    }

    RecordManager *recordManager = rel->mgmtData;
    if (recordManager == NULL)
    {
        return RC_ERROR;
    }

    RC status = pinPage(&recordManager->buffer_pool, &recordManager->pHandle, record->id.page);
    if (status != RC_OK)
    {
        return status;
    }

    data = recordManager->pHandle.data;
    data += (id.slot * recordSize);
    *data = '+';

    for (int i = 1; i < recordSize; i++)
    {
        data[i] = record->data[i];
    }

    status = markDirty(&recordManager->buffer_pool, &recordManager->pHandle);
    if (status != RC_OK)
    {
        unpinPage(&recordManager->buffer_pool, &recordManager->pHandle);
        return status;
    }

    status = unpinPage(&recordManager->buffer_pool, &recordManager->pHandle);
    if (status != RC_OK)
    {
        return status;
    }

    return RC_OK;
}

// function to get record
extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    int recordSize = getRecordSize(rel->schema);
    if (rel == NULL || record == NULL)
    {
        return RC_ERROR;
    }

    RecordManager *recordManager = rel->mgmtData;
    if (recordManager == NULL)
    {
        return RC_ERROR;
    }

    RC status = pinPage(&recordManager->buffer_pool, &recordManager->pHandle, id.page);
    if (status != RC_OK)
    {
        return status;
    }

    char *dataRef = recordManager->pHandle.data;
    dataRef = dataRef + (id.slot * recordSize);

    if (*dataRef != '+')
    {
        unpinPage(&recordManager->buffer_pool, &recordManager->pHandle);
        return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
    }

    record->id = id;
    char *data = record->data;
    memcpy(++data, dataRef + 1, recordSize - 1);

    status = unpinPage(&recordManager->buffer_pool, &recordManager->pHandle);
    if (status != RC_OK)
    {
        return status;
    }

    return RC_OK;
}

// function to start scanning a table using a condition
extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    if (rel == NULL || scan == NULL)
    {
        return RC_ERROR;
    }

    if (cond == NULL)
    {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    RC status = openTable(rel, "ScanTable");
    if (status != RC_OK)
    {
        return status;
    }

    RecordManager *scn_mgr = (RecordManager *)malloc(sizeof(RecordManager));
    if (scn_mgr == NULL)
    {
        return RC_ERROR;
    }

    *scn_mgr = (RecordManager){
        .r_ID = {.page = 1, .slot = 0},
        .num_scan = 0,
        .expr = cond};

    scan->mgmtData = scn_mgr;
    scan->rel = rel;

    RecordManager *tbl_mgr = rel->mgmtData;
    if (tbl_mgr == NULL)
    {
        free(scn_mgr);
        return RC_ERROR;
    }

    tbl_mgr->num_tup = attr_len;
    return RC_OK;
}

// function to retrieve the next record in a scan that matches the condition
extern RC next(RM_ScanHandle *scan, Record *record)
{

    RecordManager *scn_mgr = scan ? (RecordManager *)scan->mgmtData : NULL;
    RecordManager *tbl_mgr = scan && scan->rel ? (RecordManager *)scan->rel->mgmtData : NULL;

    Schema *schema = scan && scan->rel ? scan->rel->schema : NULL;

    Value *result = (scn_mgr->expr != NULL) ? malloc(sizeof(Value)) : NULL;
    if (result == NULL)
    {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    char *data = NULL;

    int recordSize = getRecordSize(schema);

    int slot_cnt = PAGE_SIZE / recordSize;

    int num_scan = scn_mgr->num_scan;
    int num_tup = tbl_mgr->num_tup;

    for (num_scan = scn_mgr->num_scan; num_scan <= num_tup; ++num_scan)
    {
        scn_mgr->r_ID.page = (num_scan == 0) ? 1 : scn_mgr->r_ID.page;
        scn_mgr->r_ID.slot = (num_scan == 0) ? 0 : scn_mgr->r_ID.slot + 1;

        scn_mgr->r_ID.page += (scn_mgr->r_ID.slot >= slot_cnt);
        scn_mgr->r_ID.slot %= slot_cnt;

        pinPage(&tbl_mgr->buffer_pool, &scn_mgr->pHandle, scn_mgr->r_ID.page);

        data = scn_mgr->pHandle.data + (scn_mgr->r_ID.slot * recordSize);

        record->id.page = scn_mgr->r_ID.page;
        record->id.slot = scn_mgr->r_ID.slot;

        record->data[0] = '-';

        memcpy(record->data + 1, data + 1, recordSize - 1);

        ++scn_mgr->num_scan;

        evalExpr(record, schema, scn_mgr->expr, &result);

        if (result->v.boolV)
        {
            unpinPage(&tbl_mgr->buffer_pool, &scn_mgr->pHandle);
            return RC_OK;
        }

        unpinPage(&tbl_mgr->buffer_pool, &scn_mgr->pHandle);
    }

    return RC_RM_NO_MORE_TUPLES;

    if (unpinPage(&tbl_mgr->buffer_pool, &scn_mgr->pHandle) != RC_OK)
    {
        return RC_UNPIN_FAILED;
    }

    scn_mgr->r_ID = (RID){.page = 1, .slot = 0};
    scn_mgr->num_scan = 0;

    return RC_RM_NO_MORE_TUPLES;
}

// function to close a scan operation and free associated resources
extern RC closeScan(RM_ScanHandle *scan)
{
    if (!scan || !scan->mgmtData)
    {
        return RC_ERROR;
    }

    RecordManager *scn_mgr = (RecordManager *)scan->mgmtData;
    RecordManager *recordManager = (RecordManager *)scan->rel->mgmtData;

    if (scn_mgr->num_scan > 0)
    {
        unpinPage(&recordManager->buffer_pool, &scn_mgr->pHandle);
    }

    *scn_mgr = (RecordManager){0};

    free(scan->mgmtData);
    scan->mgmtData = NULL;

    return RC_OK;
}

// function to calculate the size of a record based on the schema
extern int getRecordSize(Schema *schema)
{
    int size = 0;

    for (int i = 0; i < schema->numAttr; i++)
    {
        size += (schema->dataTypes[i] == DT_STRING) ? schema->typeLength[i] : (schema->dataTypes[i] == DT_INT) ? sizeof(int)
                                                                          : (schema->dataTypes[i] == DT_FLOAT) ? sizeof(float)
                                                                          : (schema->dataTypes[i] == DT_BOOL)  ? sizeof(bool)
                                                                                                               : 0;
    }

    return size + 1;
}

// function to create a new schema with the provided attributes and keys
extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL)
    {
        return NULL;
    }

    schema->attrNames = (char **)malloc(numAttr * sizeof(char *));
    schema->dataTypes = (DataType *)malloc(numAttr * sizeof(DataType));
    schema->typeLength = (int *)malloc(numAttr * sizeof(int));
    schema->keyAttrs = (int *)malloc(keySize * sizeof(int));

    if (!schema->attrNames || !schema->dataTypes || !schema->typeLength || !schema->keyAttrs)
    {
        free(schema->attrNames);
        free(schema->dataTypes);
        free(schema->typeLength);
        free(schema->keyAttrs);
        free(schema);
        return NULL;
    }

    for (int i = 0; i < numAttr; i++)
    {
        schema->attrNames[i] = attrNames[i];
        schema->dataTypes[i] = dataTypes[i];
        schema->typeLength[i] = typeLength[i];
    }

    for (int i = 0; i < keySize; i++)
    {
        schema->keyAttrs[i] = keys[i];
    }

    schema->numAttr = numAttr;
    schema->keySize = keySize;

    return schema;
}

// function to free the memory allocated for a schema
extern RC freeSchema(Schema *schema)
{
    if (schema == NULL)
    {
        return RC_ERROR;
    }

    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);

    free(schema);

    return RC_OK;
}

// Function to create a new record based on the given schema
extern RC createRecord(Record **record, Schema *schema)
{
    if (schema == NULL || record == NULL)
    {
        return RC_ERROR;
    }

    Record *newRecord = (Record *)malloc(sizeof(Record));
    if (newRecord == NULL)
    {
        return RC_ERROR;
    }

    int recordSize = getRecordSize(schema);

    newRecord->data = (char *)malloc(recordSize);
    if (newRecord->data == NULL)
    {
        free(newRecord);
        return RC_ERROR;
    }

    newRecord->id.page = -1;
    newRecord->id.slot = -1;

    newRecord->data[0] = '-';
    newRecord->data[1] = '\0';

    *record = newRecord;

    return RC_OK;
}

// function to free the memory allocated for a record
extern RC freeRecord(Record *record)
{
    if (record == NULL)
    {
        return RC_ERROR;
    }

    free(record->data);

    free(record);

    return RC_OK;
}

// function to calculate the byte offset of a specific attribute in a record based on the schema
RC attrDtOffset(Schema *schema, int attrNum, int *result)
{
    int offset = 1;

    for (int i = 0; i < attrNum; i++)
    {
        offset += (schema->dataTypes[i] == DT_STRING) ? schema->typeLength[i] : (schema->dataTypes[i] == DT_INT) ? sizeof(int)
                                                                            : (schema->dataTypes[i] == DT_FLOAT) ? sizeof(float)
                                                                            : (schema->dataTypes[i] == DT_BOOL)  ? sizeof(bool)
                                                                                                                 : 0;
    }

    *result = offset;

    return RC_OK;
}

// function to retrieve the value of an attribute from a record based on its schema
extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    int offset = 0;
    if (attrDtOffset(schema, attrNum, &offset) != RC_OK)
    {
        return RC_ERROR;
    }

    Value *attribute = malloc(sizeof(Value));
    if (attribute == NULL)
    {
        return RC_ERROR;
    }

    char *dataRef = record->data + offset;

    schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];

    if (schema->dataTypes[attrNum] == DT_STRING)
    {
        int length = schema->typeLength[attrNum];
        attribute->v.stringV = (char *)malloc(length + 1);
        if (attribute->v.stringV != NULL)
        {
            strncpy(attribute->v.stringV, dataRef, length);
            attribute->v.stringV[length] = '\0';
        }
        attribute->dt = DT_STRING;
    }
    else if (schema->dataTypes[attrNum] == DT_INT)
    {
        int value = 0;
        memcpy(&value, dataRef, sizeof(int));
        attribute->v.intV = value;
        attribute->dt = DT_INT;
    }
    else if (schema->dataTypes[attrNum] == DT_FLOAT)
    {
        float value;
        memcpy(&value, dataRef, sizeof(float));
        attribute->v.floatV = value;
        attribute->dt = DT_FLOAT;
    }
    else if (schema->dataTypes[attrNum] == DT_BOOL)
    {
        bool value;
        memcpy(&value, dataRef, sizeof(bool));
        attribute->v.boolV = value;
        attribute->dt = DT_BOOL;
    }
    else
    {
        printf("Serializer is not available for the given data structure.\n");
        free(attribute);
        return RC_ERROR;
    }

    *value = attribute;
    return RC_OK;
}

// Function to set the value of an attribute in a record based on its schema
extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    int offset = 0;

    if (attrDtOffset(schema, attrNum, &offset) != RC_OK)
    {
        return RC_ERROR;
    }

    char *dataRef = record->data + offset;

    if (schema->dataTypes[attrNum] == DT_STRING)
    {
        int length = schema->typeLength[attrNum];
        strncpy(dataRef, value->v.stringV, length);
        if (strlen(value->v.stringV) < length)
        {
            dataRef[strlen(value->v.stringV)] = '\0';
        }
    }
    else if (schema->dataTypes[attrNum] == DT_INT)
    {
        memcpy(dataRef, &value->v.intV, sizeof(int));
    }
    else if (schema->dataTypes[attrNum] == DT_FLOAT)
    {
        memcpy(dataRef, &value->v.floatV, sizeof(float));
    }
    else if (schema->dataTypes[attrNum] == DT_BOOL)
    {
        memcpy(dataRef, &value->v.boolV, sizeof(bool));
    }
    else
    {
        return RC_ERROR;
    }

    return RC_OK;
}