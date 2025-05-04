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
    int lfuCounter;
    int referenceTimes[2];

} PageFrame;

int maxBufferPages = 0;
int nextPageIdx = 0;
int writeCount = 0;
int hitCounter = 0;
int clockIdx = 0;
int lfuPointer = 0;

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

    int oldestKthIndex = -1; // Index of the frame with the oldest K-th reference time
    int oldestKthRefTime = INT_MAX; // Stores the oldest K-th reference time

    // Find the frame with the oldest 2nd most recent access time
    for (int i = 0; i < maxBufferPages; i++) {
        if (frames[i].fixCount == 0) { // Consider only unpinned pages
            if (frames[i].referenceTimes[1] < oldestKthRefTime) {
                oldestKthRefTime = frames[i].referenceTimes[1];
                oldestKthIndex = i;
            }
        }
    }

    // If no frame is found for replacement (all are pinned), return without replacement
    if (oldestKthIndex == -1) {
        printf("All pages are currently pinned, cannot replace any page.\n");
        return;
    }

    // If the frame to be replaced is dirty, write it to disk before replacing it
    if (frames[oldestKthIndex].dirty == 1) {
        SM_FileHandle fileHandle;
        openPageFile(bm->pageFile, &fileHandle);
        writeBlock(frames[oldestKthIndex].pageNum, &fileHandle, frames[oldestKthIndex].content);
        writeCount++;
    }

    // Replace the frame with the new page
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
        frames[idx].content = NULL;
        frames[idx].pageNum = -1;
        frames[idx].dirty = 0;
        frames[idx].fixCount = 0;
        frames[idx].lruCounter = 0;
        frames[idx].lfuCounter = 0;
        frames[idx].referenceTimes[0] = -1;
        frames[idx].referenceTimes[1] = -1;
    }

    bm->mgmtData = frames;
    writeCount = clockIdx = lfuPointer = 0;

    return RC_OK;
}

// function to shutdownbufferpool
RC shutdownBufferPool(BM_BufferPool *const bm) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;
    forceFlushPool(bm);

    for (int idx = 0; idx < maxBufferPages; idx++) {
        if (frames[idx].fixCount != 0) {
            return RC_PINNED_PAGES_IN_BUFFER;
        }
    }

    free(frames);
    bm->mgmtData = NULL;

    return RC_OK;
}

// function for forceflsuhfpool
RC forceFlushPool(BM_BufferPool *const bm) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    for (int idx = 0; idx < maxBufferPages; idx++) {
        if (frames[idx].fixCount == 0 && frames[idx].dirty == 1) {
            SM_FileHandle fileHandle;
            openPageFile(bm->pageFile, &fileHandle);
            writeBlock(frames[idx].pageNum, &fileHandle, frames[idx].content);
            frames[idx].dirty = 0;
            writeCount++;
        }
    }

    return RC_OK;
}

// function to mark page dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    for (int idx = 0; idx < maxBufferPages; idx++) {
        if (frames[idx].pageNum == page->pageNum) {
            frames[idx].dirty = 1;
            return RC_OK;
        }
    }

    return RC_ERROR;
}

// function to unpin page
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    for (int idx = 0; idx < maxBufferPages; idx++) {
        if (frames[idx].pageNum == page->pageNum) {
            frames[idx].fixCount--;
            return RC_OK;
        }
    }

    return RC_ERROR;
}

// function to force write the page
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    for (int idx = 0; idx < maxBufferPages; idx++) {
        if (frames[idx].pageNum == page->pageNum) {
            SM_FileHandle fileHandle;
            openPageFile(bm->pageFile, &fileHandle);
            writeBlock(frames[idx].pageNum, &fileHandle, frames[idx].content);
            frames[idx].dirty = 0;
            writeCount++;
            return RC_OK;
        }
    }

    return RC_ERROR;
}

// function to pin page to buffer
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
                clockIdx++;
                return RC_OK;
            } else if (frames[idx].pageNum == -1) {
                SM_FileHandle fileHandle;
                openPageFile(bm->pageFile, &fileHandle);
                frames[idx].content = (SM_PageHandle)malloc(PAGE_SIZE);
                readBlock(pageNum, &fileHandle, frames[idx].content);
                frames[idx].pageNum = pageNum;
                frames[idx].fixCount = 1;
                frames[idx].lfuCounter = 0;
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
            newPage->lfuCounter = 0;
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
    PageNumber *contents = (PageNumber *)malloc(sizeof(PageNumber) * maxBufferPages);
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    for (int idx = 0; idx < maxBufferPages; idx++) {
        contents[idx] = (frames[idx].pageNum != -1) ? frames[idx].pageNum : NO_PAGE;
    }

    return contents;
}

// function to retrieve the dirty flags
bool *getDirtyFlags(BM_BufferPool *const bm) {
    bool *flags = (bool *)malloc(sizeof(bool) * maxBufferPages);
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    for (int idx = 0; idx < maxBufferPages; idx++) {
        flags[idx] = (frames[idx].dirty == 1) ? true : false;
    }

    return flags;
}

// function to get fixcounts from bufferpool
int *getFixCounts(BM_BufferPool *const bm) {
    int *counts = (int *)malloc(sizeof(int) * maxBufferPages);
    PageFrame *frames = (PageFrame *)bm->mgmtData;

    for (int idx = 0; idx < maxBufferPages; idx++) {
        counts[idx] = (frames[idx].fixCount != -1) ? frames[idx].fixCount : 0;
    }

    return counts;
}

// function to get no of read IO
int getNumReadIO(BM_BufferPool *const bm) {
    return nextPageIdx + 1;
}

// function to get no of write IO
int getNumWriteIO(BM_BufferPool *const bm) {
    return writeCount;
}