CS525 Advanced Database Organization
Assignment 2 - Buffer Manager

Group 27:
Team Members:
Choladevi Gheereddy
Tejaswini Bollepalli 
Manogna Vadlamudi 

Code Structure:
assign2
* README.txt
* Makefile
* buffer_mgr.c
* buffer_mgr.h
* buffer_mgr_stat.c
* buffer_mgr_stat.h
* dberror.c
* dberror.h
* dt.h
* storage_mgr.c
* storage_mgr.h
* test_assign2_1.c
* test_assign2_2.c
* test_helper.h

Interface Implementation:

FIFOReplacement(BM_BufferPool *const bm, PageFrame *pageFrame): The FIFOReplacement function replaces the first unpinned frame in the buffer with a new page frame (pageFrame). If the frame is dirty, it writes it back to disk before replacement. It then updates the frame in the buffer and increments the index.

LRUReplacement(BM_BufferPool *const bm, PageFrame *pageFrame) : The LRUReplacement function replaces the least recently used page in the buffer pool. It finds the page with the lowest lruCounter and fixCount of 0, writes it to disk if it's dirty, and replaces it with the new pageFrame. Parameters are the buffer pool (bm) and the new page frame (pageFrame).

LRU_KReplacement(BM_BufferPool *const bm, PageFrame *pageFrame) : The LRU_KReplacement function replaces a page in the buffer pool using the LRU-K algorithm. It finds the unpinned page with the oldest 2nd most recent access time (referenceTimes[1]) and replaces it, writing to disk if dirty. Parameters are the buffer pool (bm) and the new page frame (pageFrame).

1. initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy,void *stratData): This function initBufferPool initializes a buffer pool for a page file pageFileName with numPages using the specified strategy. It sets up the bm structure with the page file name, number of pages, and replacement strategy, and allocates memory for numPages PageFrame structures in mgmtData, initializing each frame's properties. Global variables such as writeCount, clockIdx, and lfuPointer are also reset.

2. shutdownBufferPool(BM_BufferPool *const bm): The shutdownBufferPool function deallocates the buffer pool bm after ensuring that all pages are written back to disk by calling forceFlushPool. It checks if any pages have a non-zero fixCount (indicating they are still in use) and returns RC_PINNED_PAGES_IN_BUFFER if so. If all pages are unpinned, it frees the allocated memory for the frames and sets bm->mgmtData to NULL.

3. forceFlushPool(BM_BufferPool *const bm): The forceFlushPool function writes all dirty pages in the buffer pool bm back to the disk if their fixCount is zero (not in use). It iterates over each PageFrame in mgmtData, opens the page file bm->pageFile, and uses writeBlock to save the page contents, then marks the page as clean (dirty = 0) and increments writeCount. The function returns RC_OK after flushing all eligible pages.

4. markDirty(BM_BufferPool *const bm, BM_PageHandle *const page): The markDirty function marks a specific page in the buffer pool bm as dirty, indicating it has been modified. It iterates through the PageFrame structures in mgmtData to find the frame that matches the page number of page->pageNum and sets its dirty flag to 1. If the page is found and marked, it returns RC_OK; otherwise, it returns RC_ERROR.

5. unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page): The unpinPage function decreases the fixCount of a specific page in the buffer pool bm, indicating it is no longer being accessed. It searches through the PageFrame structures in mgmtData to find the frame with page->pageNum and decrements its fixCount. If the page is found, it returns RC_OK; otherwise, it returns RC_ERROR.

6. forcePage(BM_BufferPool *const bm, BM_PageHandle *const page): The forcePage function writes the contents of a specific page in the buffer pool bm to disk. It searches the PageFrame structures in mgmtData to find the frame with page->pageNum, opens the page file bm->pageFile, and calls writeBlock to write the page's content. After writing, it marks the page as clean (dirty = 0) and increments writeCount. If successful, it returns RC_OK; otherwise, it returns RC_ERROR.

7. pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum): The pinPage function loads a specific page pageNum into the buffer pool bm and pins it for use. If the buffer is empty, it reads the requested page from the file and initializes the first PageFrame. If the requested page is already in the buffer, it increments its fixCount and updates its LRU information if necessary. If there are empty frames, it reads the page into the first available frame. If the buffer is full, it uses the specified replacement strategy (e.g., FIFO, LRU, LRU_K) to replace an existing page with the new one and then pins it. The page data and number are updated in the page parameter.

8.PageNumber *getFrameContents(BM_BufferPool *const bm): The getFrameContents function returns an array of PageNumber values representing the pages currently held in the buffer pool bm. It allocates memory for the array contents, iterates over each PageFrame in mgmtData, and stores the pageNum of each frame in the array. If a frame is empty (pageNum is -1), it stores NO_PAGE instead.

9.bool *getDirtyFlags(BM_BufferPool *const bm): The getDirtyFlags function returns an array of boolean values indicating the dirty status of each page in the buffer pool bm. It allocates memory for the flags array and iterates over each PageFrame in mgmtData, setting each flag to true if the frame's dirty attribute is 1, otherwise setting it to false. The array is then returned to indicate which pages have been modified.

10.int *getFixCounts(BM_BufferPool *const bm): The getFixCounts function returns an array of integers representing the fixCount of each page in the buffer pool bm. It allocates memory for the counts array and iterates through each PageFrame in mgmtData, assigning the frame's fixCount to the corresponding position in the array. If the fixCount is -1, it sets the value to 0 in the array.

11.int getNumReadIO(BM_BufferPool *const bm): The getNumReadIO function returns the total number of pages read from the disk into the buffer pool bm. It calculates this by returning the value of nextPageIdx + 1, where nextPageIdx tracks the number of pages read so far. This value indicates the total number of read I/O operations performed.

12.int getNumWriteIO(BM_BufferPool *const bm): The getNumWriteIO function returns the total number of write operations performed by the buffer pool bm. It simply returns the value of the writeCount variable, which keeps track of the number of pages that have been written back to disk.
