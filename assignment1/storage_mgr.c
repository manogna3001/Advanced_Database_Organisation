#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "storage_mgr.h"
#include "dberror.h"

FILE *f_ptr;


void initStorageManager(void)
{
    //verifying that storage manager is initialized.
    printf("initializing the storagae manager\n");
}

// function to create page file
RC createPageFile(char *fileName)
{
    //created and opened in write and read mode.
    FILE *f_ptr = fopen(fileName, "w+"); 
    if (f_ptr == NULL)
    {
        return RC_FILE_NOT_FOUND; 
    }

    char *mblock = malloc(PAGE_SIZE); 
    if (mblock == NULL)
    {
        fclose(f_ptr);
        return RC_FILE_NOT_FOUND; 
    }
    //Fill's the allocated memory with '\0' bytes.
    for (int i = 0; i < PAGE_SIZE; i++)
    {
        mblock[i] = '\0';
    }
    //writes memory  block to file.
    size_t written = fwrite(mblock, sizeof(char), PAGE_SIZE, f_ptr);
    if (written < PAGE_SIZE)
    {
        fclose(f_ptr);
        return RC_WRITE_FAILED;
    }

    free(mblock);     
    fclose(f_ptr); 

    return RC_OK;
}

// function to open page file.
RC openPageFile(char *fileName, SM_FileHandle *fHandle) 
{

    // checks is file is present.
    if (access(fileName, F_OK) != 0) {
        return RC_FILE_NOT_FOUND;
    }

   
    FILE *f_ptr = fopen(fileName, "r+");
    
    // checks if opening of file is successfull.
    if (f_ptr == NULL) {
        return RC_FILE_NOT_FOUND; 
    }

    // storing information in fHandle.
    fHandle->fileName = strdup(fileName);  
    fHandle->curPagePos = 0;               
    fHandle->mgmtInfo = f_ptr;          

    fseek(f_ptr, 0, SEEK_END);         
    long fileSize = ftell(f_ptr);     

    fHandle->totalNumPages = fileSize / PAGE_SIZE;

    fseek(f_ptr, 0, SEEK_SET);

    return RC_OK;
}

// function to close page file.
RC closePageFile(SM_FileHandle *fHandle) {
    // close file and check for error.
    if (fclose(fHandle->mgmtInfo) == 0) {
        return RC_OK;  
    } else {
        return RC_FILE_NOT_FOUND;  
    }
}

// function to destroy page file.
RC destroyPageFile(char *fileName) {
    // delete file and check for error.
    if (remove(fileName) == 0) {
        return RC_OK;  
    } else {
        
        return RC_FILE_NOT_FOUND;  
    }
}

// function to read block.
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // check if page is valid.
    if (pageNum >= fHandle->totalNumPages || pageNum < 0) {
        return RC_READ_NON_EXISTING_PAGE;  
    }

    // move file pointer to correct position.
    if (fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET) != 0) {
        return RC_READ_NON_EXISTING_PAGE;  
    }

    if (fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo) < PAGE_SIZE) {
        return RC_READ_NON_EXISTING_PAGE;  
    }

    fHandle->curPagePos = pageNum;

    return RC_OK;  
}

// fuction to get block position.
int getBlockPos(SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

// function to read first block.
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
   // checking if fHandle and memPge is invalid.
    if (fHandle == NULL || memPage == NULL) {
        return RC_FILE_NOT_FOUND; 
    }

    FILE *f_ptr = fopen(fHandle->fileName, "rb");
    if (f_ptr == NULL) {
        return RC_FILE_NOT_FOUND;  
    }


    // checking if read is successfull.
    if (fread(memPage, sizeof(char), PAGE_SIZE, f_ptr) < PAGE_SIZE) {
        if (feof(f_ptr)) {
            fclose(f_ptr);  
            return 4;  
        } else if (ferror(f_ptr)) {
            fclose(f_ptr);  
            return RC_READ_NON_EXISTING_PAGE;  
        }
    }

    fclose(f_ptr);

    fHandle->curPagePos = 0;

    return RC_OK;
}

// function to read previous block.
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    //check if valid.
    if (fHandle == NULL || memPage == NULL) {
        return RC_FILE_NOT_FOUND;  
    }

    int currentBlock = getBlockPos(fHandle);
    int previousBlock = currentBlock - 1;

    // checking if previous block is valid
    if (previousBlock < 0) {
        return RC_READ_NON_EXISTING_PAGE;  
    }

    long offset = previousBlock * PAGE_SIZE;

    FILE *f_ptr = fopen(fHandle->fileName, "rb");
    if (f_ptr == NULL) {
        return RC_FILE_NOT_FOUND;  
    }
    // seeks previous position of block.
    if (fseek(f_ptr, offset, SEEK_SET) != 0) {
        fclose(f_ptr);
        return RC_READ_NON_EXISTING_PAGE; 
    }

    if (fread(memPage, sizeof(char), PAGE_SIZE, f_ptr) < PAGE_SIZE) {
        if (feof(f_ptr)) {
            fclose(f_ptr);
            return RC_READ_NON_EXISTING_PAGE;  
        } else if (ferror(f_ptr)) {
            fclose(f_ptr);
            return RC_READ_NON_EXISTING_PAGE;  
        }
    }

    fclose(f_ptr);
    fHandle->curPagePos = previousBlock;
    return RC_OK;
}

// function to read current block.
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if (fHandle == NULL || memPage == NULL) {
        return RC_FILE_NOT_FOUND; 
    }

    int currentBlock = getBlockPos(fHandle);

    long offset = currentBlock * PAGE_SIZE;

    FILE *f_ptr = fopen(fHandle->fileName, "rb");
    if (f_ptr == NULL) {
        return RC_FILE_NOT_FOUND;  
    }

    // seek current position of block.
    if (fseek(f_ptr, offset, SEEK_SET) != 0) {
        fclose(f_ptr);
        return RC_READ_NON_EXISTING_PAGE; 
    }
    
    if (fread(memPage, sizeof(char), PAGE_SIZE, f_ptr) < PAGE_SIZE) {
        if (feof(f_ptr)) {
            fclose(f_ptr);
            return RC_READ_NON_EXISTING_PAGE;
        } else if (ferror(f_ptr)) {
            fclose(f_ptr);
            return RC_READ_NON_EXISTING_PAGE;
        }
    }

    fclose(f_ptr);
    return RC_OK;
}

// function to read last block.
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || memPage == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;  
    }

    if (fHandle->totalNumPages <= 0) {
        return RC_READ_NON_EXISTING_PAGE; 
    }

    if (fseek(fHandle->mgmtInfo, -PAGE_SIZE, SEEK_END) != 0) {
        return RC_READ_NON_EXISTING_PAGE;  
    }

    if (fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo) != PAGE_SIZE) {
        return RC_READ_NON_EXISTING_PAGE; 
    }

    fHandle->curPagePos = fHandle->totalNumPages - 1;

    return RC_OK;
}

// function to write block.
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || memPage == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;  
    }

    // checking if page number is within the file page numbers.
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;  
    }

    long byte_offset = (long)pageNum * PAGE_SIZE;

    if (fseek(fHandle->mgmtInfo, byte_offset, SEEK_SET) != 0) {
        return RC_WRITE_FAILED;  
    }

    if (fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo) != PAGE_SIZE) {
        return RC_WRITE_FAILED; 
    }

    fHandle->curPagePos = pageNum;

    return RC_OK;  
}

// function to write current block.
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || memPage == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;  
    }

    //checking if page is valid.
    if (fHandle->curPagePos < 0 || fHandle->curPagePos >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;  
    }

    long byte_offset = (long)fHandle->curPagePos * PAGE_SIZE;

    if (fseek(fHandle->mgmtInfo, byte_offset, SEEK_SET) != 0) {
        return RC_WRITE_FAILED;  
    }

    if (fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo) != PAGE_SIZE) {
        return RC_WRITE_FAILED; 
    }

    return RC_OK;  
}

// function to append empty.
RC appendEmptyBlock(SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT; 
    }

    if (fseek(fHandle->mgmtInfo, 0, SEEK_END) != 0) {
        return RC_WRITE_FAILED; 
    }

    char empty_block[PAGE_SIZE] = {0};  
    if (fwrite(empty_block, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo) != PAGE_SIZE) {
        return RC_WRITE_FAILED; 
    }

    fHandle->totalNumPages++;

    return RC_OK;  
}

// function to ensure the capacity of file is numberOfPages.
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // adding empty pages based on condition.
    while (fHandle->totalNumPages < numberOfPages) {
        if (appendEmptyBlock(fHandle) != RC_OK) {
            return RC_WRITE_FAILED;
        }
    }

    return RC_OK;
}
