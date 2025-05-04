#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "storage_mgr.h"

FILE *f_ptr;

extern void initStorageManager(void)
{
    printf("Initializing the storagae manager\n");
}

// Function to create page
extern RC createPageFile(char *fileName)
{
    f_ptr = fopen(fileName, "w+");
    if (f_ptr == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }
    else
    {
        SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char)); // check

        // Writing the empty page to the file.
        if (fwrite(emptyPage, sizeof(char), PAGE_SIZE, f_ptr) < PAGE_SIZE)
        {
            printf("Write failed \n");
        }
        else
        {
            printf("Write succeeded \n");
        }

        fclose(f_ptr);
        free(emptyPage);
        return RC_OK;
    }
}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    f_ptr = fopen(fileName, "r");
    if (f_ptr == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }
    else
    {
        fHandle->fileName = fileName;
        fHandle->curPagePos = 0;
        fHandle->mgmtInfo = f_ptr;
        struct stat fileInfo;
        if (fstat(fileno(f_ptr), &fileInfo) < 0)
        { // check
            return RC_ERROR;
        }
        fHandle->totalNumPages = fileInfo.st_size / PAGE_SIZE;
        fclose(f_ptr);
        return RC_OK;
    }
}

// function to close page file.
extern RC closePageFile(SM_FileHandle *fHandle)
{
    // close file and check for error.
    if (f_ptr != NULL)
        f_ptr = NULL;
    
    
    return RC_OK;

    // if (fclose(fHandle->mgmtInfo) == 0) {
    //     return RC_OK;
    // } else {
    //     return RC_FILE_NOT_FOUND;
    // }
}

// function to destroy page file
extern RC destroyPageFile(char *fileName)
{
    f_ptr = fopen(fileName, "r");
    if (f_ptr == NULL)
        return RC_FILE_NOT_FOUND;
    remove(fileName);
    return RC_OK;
}

// function to read block
extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (pageNum > fHandle->totalNumPages || pageNum < 0)
        return RC_READ_NON_EXISTING_PAGE;

    f_ptr = fopen(fHandle->fileName, "r");

    if (f_ptr == NULL)
        return RC_FILE_NOT_FOUND;

    if (fseek(f_ptr, (pageNum * PAGE_SIZE), SEEK_SET) == 0)
    {
        if (fread(memPage, sizeof(char), PAGE_SIZE, f_ptr) < PAGE_SIZE)
            return RC_ERROR;
    }
    else
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    fHandle->curPagePos = ftell(f_ptr); // check
    fclose(f_ptr);
    return RC_OK;
}

// function to get block position
extern int getBlockPos(SM_FileHandle *fHandle)
{
    return fHandle->curPagePos;
}

// function to read first block
extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(0, fHandle, memPage);
}

// function to read previous block
extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // instead of re-writing function from scratch
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

// function to read current block
extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // instead of re-writing function from scratch
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

// function to read next block
extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle->curPagePos == PAGE_SIZE)
    {
        printf("\n Last block: Next block not present.");
        return RC_READ_NON_EXISTING_PAGE;
    }
    else
    {
        return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
    }
}

// function to read last block
extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // instead of re-writing function from scratch
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

/////////////////////////////write //////////////////////

extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (pageNum > fHandle->totalNumPages || pageNum < 0)
        return RC_WRITE_FAILED;

    f_ptr = fopen(fHandle->fileName, "r+");
    if (f_ptr == NULL)
        return RC_FILE_NOT_FOUND;

    if (pageNum == 0)
    {
        fseek(f_ptr, pageNum * PAGE_SIZE, SEEK_SET);
        int i;
        for (i = 0; i < PAGE_SIZE; i++)
        {
            if (feof(f_ptr))
                appendEmptyBlock(fHandle);
            fputc(memPage[i], f_ptr);
        }
        fHandle->curPagePos = ftell(f_ptr);
        fclose(f_ptr);
    }
    else
    {
        fHandle->curPagePos = pageNum * PAGE_SIZE;
        fclose(f_ptr);
        writeCurrentBlock(fHandle, memPage);
    }
    return RC_OK;
}

extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    f_ptr = fopen(fHandle->fileName, "r+");

    if (f_ptr == NULL)
        return RC_FILE_NOT_FOUND;

    appendEmptyBlock(fHandle);
    fseek(f_ptr, fHandle->curPagePos, SEEK_SET);
    fwrite(memPage, sizeof(char), strlen(memPage), f_ptr);
    fHandle->curPagePos = ftell(f_ptr);
    fclose(f_ptr);
    return RC_OK;
}

extern RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));

    if (fseek(f_ptr, 0, SEEK_END) == 0)
    {
        // Writing an empty page to the file
        fwrite(emptyBlock, sizeof(char), PAGE_SIZE, f_ptr);
    }
    else
    {
        free(emptyBlock);
        return RC_WRITE_FAILED;
    }

    free(emptyBlock);
    fHandle->totalNumPages++;
    return RC_OK;
}

extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    if (fopen(fHandle->fileName, "a") == NULL)
        return RC_FILE_NOT_FOUND;

    while (numberOfPages > fHandle->totalNumPages)
        appendEmptyBlock(fHandle);

    fclose(f_ptr);
    return RC_OK;
}