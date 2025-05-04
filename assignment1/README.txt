CS525 Advanced Database Organization
Assignment 1-Storage Manager

Group:27
Team Members:
Choladevi Gheereddy (A20544476)
Tejaswini Bollepalli (A20562616)
Manogna Vadlamudi (A20551908)

Code Structure:
assign1
* dberror.c
* dberror.h
* Makefile
* README.txt
* storage_mgr.c
* storage_mgr.h
* test_assign1_1.c
* test_helper.h

Interface Implementation:

1.initStorageManager (void) : It has no active functionality. It performs no operations in this implementation.

2.createPageFile (char *fileName): The createPageFile() function akes a fHandle as a parameter and creates a new file specified by fileName in write/read mode (w+), allocates a memory block of size PAGE_SIZE, fills it with zero bytes ('\0'), and writes it to the file. If the file cannot be created or memory cannot be allocated, it returns RC_FILE_NOT_FOUND. If writing the block to the file fails, it returns RC_WRITE_FAILED. On success, the memory is freed, the file is closed, and RC_OK is returned.

3.openPageFile (char *fileName, SM_FileHandle *fHandle): The openPageFile function takes fileName and fHandle as parameters, attempting to open the file in read/write mode. If successful it calculates the total number of pages by determining the file size and dividing by PAGE_SIZE, then updates the fHandle with the file name, total number of pages, current page position, and file pointer. If the file doesn't exist or can't be opened, it returns RC_FILE_NOT_FOUND else returns RC_OK.

4.closePageFile (SM_FileHandle *fHandle):The closePageFile function takes a fHandle as a parameter and attempts to close the file associated with it using the fclose function. If the file can't be closed it will return RC_FILE_NOT_FOUND; or else it sets fHandle->mgmtInfo to NULL and returns RC_OK.

5.destroyPageFile (char *fileName): The destroyPageFile function takes a fileName as a parameter and attempts to delete the file using the remove function. If the file is successfully deleted, it returns RC_OK or else it returns RC_FILE_NOT_FOUND.

6.readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage): The readBlock function takes a pageNum, fHandle, memPage as parameters.  It first checks if the page number is valid, then moves the file pointer to the correct position using fseek(). If reading the block is successful, it updates the current page position and returns RC_OK, otherwise it returns RC_READ_NON_EXISTING_PAGE.

7.getBlockPos (SM_FileHandle *fHandle): The getBlockPos function takes a file handle fHandle as a parameter and returns the current page position (curPagePos) stored in the file handle.

8.readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage): The readFirstBlock function takes a fHandle and memPage to read the first block of a file. It first checks if fHandle or memPage are valid and opens the file in read-binary mode returning RC_FILE_NOT_FOUND if the file can't be opened. It then reads the first block into memPage closes the file updates the current page position to 0 and returns RC_OK on success or an error code if reading fails.

9.readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage):The readPreviousBlock function takes a fHandle and memPage to read the previous block of the file. It first checks if fHandle and memPage are valid then calculates the previous block position. If the previous block exists the function seeks to its byte offset reads it into memPage updates the current page position and returns RC_OK or returns RC_READ_NON_EXISTING_PAGE if the operation fails.

10.readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage):The readCurrentBlock function takes a fHandle and memPage to read the current block from the file. It first checks if the file handle and memory page are valid and opens the file in read mode returning RC_FILE_NOT_FOUND if there's an issue. It calculates the byte offset for the current block seeks to that position reads the block into memPage and returns RC_OK on success or RC_READ_NON_EXISTING_PAGE if the operation fails.

11.readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage):The readNextBlock function checks if fHandle is NULL if is then it returns RC_FILE_NOT_FOUND indicating the file handle is invalid. Then it verifies if the succeeding block (presentPage + 1) is within the file's total number of pages if not then it returns RC_READ_NON_EXISTING_PAGE. If the block exists and the read is successful it updates the current page position and returns RC_OK; if the read fails, it returns RC_READ_NON_EXISTING_PAGE.

12.readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage):The readLastBlock function takes a fHandle and memPage to read the last block of the file. It first checks if fHandle and memPage are valid and ensures the file contains at least one page returning errors if either condition fails. It then seeks to the start of the last block reads it into memPage updates the current page position and returns RC_OK on success or RC_READ_NON_EXISTING_PAGE if the operation fails.

13.writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage):The writeBlock function takes a pageNum a fHandle and memPage to write data to a specific page in the file. It first checks if the file handle and memory page are valid returning RC_FILE_HANDLE_NOT_INIT if not and ensures the pageNum is within bounds. It then calculates the byte offset seeks to the correct position, writes the page, updates the current page position and returns RC_OK on success or RC_WRITE_FAILED if the operation fails.

14.writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage):The writeCurrentBlock function takes a fHandle and memPage to write data to the current page in the file. It first checks if fHandle or memPage is invalid returning RC_FILE_HANDLE_NOT_INIT if so and ensures the current page position is valid. It then calculates the byte offset seeks to the position and writes the page to the file returning RC_OK on success or RC_WRITE_FAILED if the write operation fails.

15.appendEmptyBlock(SM_FileHandle *fHandle):The appendEmptyBlock function takes a fHandle as a parameter and appends an empty block of size PAGE_SIZE to the file. It first checks if fHandle is valid returning RC_FILE_HANDLE_NOT_INIT if not then moves the file pointer to the end. If successful it writes a zero-initialized block to the file updates the total number of pages and returns RC_OK; orelse it returns RC_WRITE_FAILED.

16.ensureCapacity(int numberOfPages, SM_FileHandle *fHandle):The ensureCapacity function takes a target numberOfPages and fHandle to ensure the file has enough pages. It first checks if fHandle is valid, returning RC_FILE_HANDLE_NOT_INIT if it's not. If the file has fewer pages than required it appends empty blocks using appendEmptyBlock until the required number is reached, then returns RC_OK.