#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>
#include <limits.h>

typedef struct PageFrame {
    SM_PageHandle content;
    PageNumber pageNum;
    int dirty;
    int fixCount;
    int lruCounter;
    int referenceTimes[2];

} PageFrame;

int maxBufferPages = 0;
int nextPageIdx = 0;
int writeCount = 0;
int hitCounter = 0;

// FIFO replacement strategy.
void FIFOReplacement(BM_BufferPool *const bm, PageFrame *pageFrame) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    int idx, firstPageIdx;
    firstPageIdx = nextPageIdx % maxBufferPages;

    for (idx = 0; idx < maxBufferPages; idx++) {
        if (frames[firstPageIdx].fixCount == 0) {
            if (frames[firstPageIdx].dirty == 1) {
                SM_FileHandle fileHandle;
                openPageFile(bm->pageFile, &fileHandle);
                writeBlock(frames[firstPageIdx].pageNum, &fileHandle, frames[firstPageIdx].content);
                writeCount++;
            }

            frames[firstPageIdx] = *pageFrame;
            break;
        }
        firstPageIdx = (firstPageIdx + 1) % maxBufferPages;
    }
}

// LRU (Least Recently Used)
void LRUReplacement(BM_BufferPool *const bm, PageFrame *pageFrame) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    int idx, leastRecentlyUsedIdx, minLRUCount;
    for (idx = 0; idx < maxBufferPages; idx++) {
        if (frames[idx].fixCount == 0) {
            leastRecentlyUsedIdx = idx;
            minLRUCount = frames[idx].lruCounter;
            break;
        }
    }

    for (idx = 0; idx < maxBufferPages; idx++) {
        if (frames[idx].lruCounter < minLRUCount) {
            leastRecentlyUsedIdx = idx;
            minLRUCount = frames[idx].lruCounter;
        }
    }

    if (frames[leastRecentlyUsedIdx].dirty == 1) {
        SM_FileHandle fileHandle;
        openPageFile(bm->pageFile, &fileHandle);
        writeBlock(frames[leastRecentlyUsedIdx].pageNum, &fileHandle, frames[leastRecentlyUsedIdx].content);
        writeCount++;
    }

    frames[leastRecentlyUsedIdx] = *pageFrame;
}

// LRU_K (Least Recently Used- K)
void LRU_KReplacement(BM_BufferPool *const bm, PageFrame *pageFrame) {
    printf("calling LRU K---------------->");
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    int oldestKthIndex = -1; 
    int oldestKthRefTime = INT_MAX; 
    for (int i = 0; i < maxBufferPages; i++) {
        if (frames[i].fixCount == 0) {
            if (frames[i].referenceTimes[1] < oldestKthRefTime) {
                oldestKthRefTime = frames[i].referenceTimes[1];
                oldestKthIndex = i;
            }
        }
    }

    if (oldestKthIndex == -1) {
        printf("All pages are currently pinned, cannot replace any page.\n");
        return;
    }

    if (frames[oldestKthIndex].dirty == 1) {
        SM_FileHandle fileHandle;
        openPageFile(bm->pageFile, &fileHandle);
        writeBlock(frames[oldestKthIndex].pageNum, &fileHandle, frames[oldestKthIndex].content);
        writeCount++;
    }

    frames[oldestKthIndex] = *pageFrame;
}


// Buffer pool initialization.
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
                  const int numPages, ReplacementStrategy strategy, 
                  void *stratData) {
    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;

    PageFrame *frames = (PageFrame *)malloc(sizeof(PageFrame) * numPages);
    maxBufferPages = numPages;

    for (int idx = 0; idx < maxBufferPages; idx++) {
        PageFrame *currentFrame = &frames[idx];  

        currentFrame->content = NULL;
        currentFrame->pageNum = -1;
        currentFrame->dirty = 0;
        currentFrame->fixCount = 0;
        currentFrame->lruCounter = 0;
        currentFrame->referenceTimes[0] = -1;
        currentFrame->referenceTimes[1] = -1;
    }

    bm->mgmtData = frames;

    writeCount = 0;

    return RC_OK;
}

// function to shutdownbufferpool
RC shutdownBufferPool(BM_BufferPool *const bm) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    RC status = forceFlushPool(bm);
    if (status != RC_OK) {
        return status; 
    }

    // Checking if any pages are still pinned
    int idx = 0;
    while (idx < maxBufferPages) {
        if (frames[idx].fixCount > 0) {
            return RC_PINNED_PAGES_IN_BUFFER;
        }
        idx++;
    }
    if (frames != NULL) {
        free(frames);
        frames = NULL; 
    }
    bm->mgmtData = NULL;

    return RC_OK;
}



// function for forceflsuhfpool
RC forceFlushPool(BM_BufferPool *const bm) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    // Iterating all buffer pages
    for (int idx = 0; idx < maxBufferPages; idx++) {
        PageFrame *currentFrame = &frames[idx]; 

        // Checking if page is not pinned and is dirty
        if (currentFrame->fixCount == 0 && currentFrame->dirty == 1) {
            SM_FileHandle fileHandle;

            if (openPageFile(bm->pageFile, &fileHandle) == RC_OK) {
                // Writing content to specified block
                if (writeBlock(currentFrame->pageNum, &fileHandle, currentFrame->content) == RC_OK) {
                    currentFrame->dirty = 0;
                    writeCount++;
                }
                closePageFile(&fileHandle); 
            }
        }
    }

    return RC_OK;
}


// function to mark page dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;
    PageFrame *currentFrame = NULL;

    // Iterating through buffer pool pages to check if there is matching page
    for (int idx = 0; idx < maxBufferPages; idx++) {
        currentFrame = &frames[idx];

        if (currentFrame->pageNum == page->pageNum) {
            currentFrame->dirty = 1;
            return RC_OK; 
        }
    }

    return RC_ERROR;
}


// function to unpin page
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;
    
    // Iterating through buffer pool pages to check if there is matching page
    for (int idx = 0; idx < maxBufferPages; idx++) {
        PageFrame *currentFrame = &frames[idx]; 

        if (currentFrame->pageNum == page->pageNum) {
            if (currentFrame->fixCount > 0) {
                currentFrame->fixCount--;
            }
            return RC_OK; 
        }
    }

    return RC_ERROR;
}


// function to force write the page
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    // Iterating through buffer pool pages to check if there is matching page
    for (int idx = 0; idx < maxBufferPages; idx++) {
        PageFrame *currentFrame = &frames[idx]; 

        if (currentFrame->pageNum == page->pageNum) {
            SM_FileHandle fileHandle;

            if (openPageFile(bm->pageFile, &fileHandle) != RC_OK) {
                return RC_FILE_NOT_FOUND; 
            }

            // Writing  page's content to file
            if (writeBlock(currentFrame->pageNum, &fileHandle, currentFrame->content) != RC_OK) {
                closePageFile(&fileHandle); 
                return RC_WRITE_FAILED;     
            }

            
            currentFrame->dirty = 0;
            writeCount++;

            
            closePageFile(&fileHandle);

            return RC_OK; 
        }
    }

    return RC_ERROR;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, 
           const PageNumber pageNum) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    if (frames[0].pageNum == -1) {
        SM_FileHandle fileHandle;
        openPageFile(bm->pageFile, &fileHandle);
        frames[0].content = (SM_PageHandle)malloc(PAGE_SIZE);
        ensureCapacity(pageNum, &fileHandle);
        readBlock(pageNum, &fileHandle, frames[0].content);
        frames[0].pageNum = pageNum;
        frames[0].fixCount++;
        nextPageIdx = hitCounter = 0;
        frames[0].lruCounter = hitCounter;
        page->pageNum = pageNum;
        page->data = frames[0].content;
        return RC_OK;
    } else {
        bool isFull = true;

        for (int idx = 0; idx < maxBufferPages; idx++) {
            if (frames[idx].pageNum == pageNum) {
                frames[idx].fixCount++;
                isFull = false;
                hitCounter++;
                if (bm->strategy == RS_LRU)
                    frames[idx].lruCounter = hitCounter;
                page->pageNum = pageNum;
                page->data = frames[idx].content;
                return RC_OK;
            } else if (frames[idx].pageNum == -1) {
                SM_FileHandle fileHandle;
                openPageFile(bm->pageFile, &fileHandle);
                frames[idx].content = (SM_PageHandle)malloc(PAGE_SIZE);
                readBlock(pageNum, &fileHandle, frames[idx].content);
                frames[idx].pageNum = pageNum;
                frames[idx].fixCount = 1;
                nextPageIdx++;
                hitCounter++;
                if (bm->strategy == RS_LRU)
                    frames[idx].lruCounter = hitCounter;
                page->pageNum = pageNum;
                page->data = frames[idx].content;
                isFull = false;
                return RC_OK;
            }
        }

        if (isFull) {
            PageFrame *newPage = (PageFrame *)malloc(sizeof(PageFrame));
            SM_FileHandle fileHandle;
            openPageFile(bm->pageFile, &fileHandle);
            newPage->content = (SM_PageHandle)malloc(PAGE_SIZE);
            readBlock(pageNum, &fileHandle, newPage->content);
            newPage->pageNum = pageNum;
            newPage->dirty = 0;
            newPage->fixCount = 1;
            nextPageIdx++;
            hitCounter++;
            if (bm->strategy == RS_LRU)
                newPage->lruCounter = hitCounter;
            page->pageNum = pageNum;
            page->data = newPage->content;

            switch (bm->strategy) {
                case RS_FIFO:
                    FIFOReplacement(bm, newPage);
                    break;
                case RS_LRU:
                    LRUReplacement(bm, newPage);
                    break;
                case RS_LRU_K:
                    LRU_KReplacement(bm, newPage);
                    break;
                default:
                    printf("Replacement strategy not implemented.\n");
                    break;
            }
        }
        return RC_OK;
    }
}

// function to retrieve the frame contents
PageNumber *getFrameContents(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) {
        return NULL; 
    }

    // Allocating memory to store the frame contents
    PageNumber *contents = (PageNumber *)malloc(sizeof(PageNumber) * maxBufferPages);
    if (contents == NULL) {
        return NULL; 
    }

    PageFrame *frames = (PageFrame *)bm->mgmtData;

    for (int idx = 0; idx < maxBufferPages; idx++) {
        if (frames[idx].pageNum != -1) {
            contents[idx] = frames[idx].pageNum;
        } else {
            contents[idx] = NO_PAGE; 
        }
    }

    return contents; 
}

// function to retrieve the dirty flags
bool *getDirtyFlags(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) {
        return NULL; 
    }

    bool *flags = (bool *)malloc(sizeof(bool) * maxBufferPages);
    if (flags == NULL) {
        return NULL; 
    }

    PageFrame *frames = (PageFrame *)bm->mgmtData;
    for (int idx = 0; idx < maxBufferPages; idx++) {
        flags[idx] = (frames[idx].dirty != 0); // Sets true if dirty, otherwise false
    }

    return flags; 
}

// function to get fixcounts from bufferpool
int *getFixCounts(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) {
        return NULL; 
    }

    // Allocate memory for the fix counts array
    int *counts = (int *)malloc(sizeof(int) * maxBufferPages);
    if (counts == NULL) {
        return NULL; 
    }

    PageFrame *frames = (PageFrame *)bm->mgmtData;

    for (int idx = 0; idx < maxBufferPages; idx++) {
        counts[idx] = (frames[idx].fixCount >= 0) ? frames[idx].fixCount : 0;
    }

    return counts; 
}


int getNumReadIO(BM_BufferPool *const bm) {
    if (bm == NULL) {
        return -1; 
    }

    return (nextPageIdx >= 0) ? nextPageIdx + 1 : 0;
}

int getNumWriteIO(BM_BufferPool *const bm) {
    if (bm == NULL) {
        return -1; 
    }
    return (writeCount >= 0) ? writeCount : 0;
}

