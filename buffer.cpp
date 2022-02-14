/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId& frame) {}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  FrameId frameNumber;
  try {
    // attempt to look up page in hashtable
    hashTable.lookup(file, pageNo, frameNumber);

    // can only unpin a page that has been pinned
    if (bufDescTable[frameNumber].pinCnt > 0) {
      bufDescTable[frameNumber].pinCnt--;
      // sigal that the page has been modified
      if (dirty == true) {
        bufDescTable[frameNumber].dirty = true;
      }

    } else if (bufDescTable[frameNumber].pinCnt == 0) {
      throw PagePinnedException(file.filename_, pageNo, frameNumber);
    }

  } catch(HashNotFoundException& e){
  }
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
  FrameId frameNumber;
  // obtain buffer pool
  allocBuf(frameNumber);
  // allocate new page for file and setup the page number
  bufPool[frameNumber] = file.allocatePage();
  pageNo = page->page_number();

  // insert entry into has
  hashTable.insert(file, pageNo, frameNumber);
  // set frame up properly
  bufDescTable[frameNumber].Set(file, pageNo);

  // return page to the caller
  page = &bufPool[frameNumber];
}

void BufMgr::flushFile(File& file) {}

void BufMgr::disposePage(File& file, const PageId PageNo) {
  FrameId frameNumber;
  try {
    file.deletePage(PageNo);

    // delete from hashtable
    hashTable.lookup(file, PageNo, frameNumber);
    if (bufDescTable[frameNumber].pinCnt != 0) {
      throw PagePinnedException(bufDescTable[frameNumber].file.filename_, bufDescTable[frameNumber].pageNo, bufDescTable[frameNumber].frameNo);
    }


    hashTable.remove(file, PageNo);
    bufDescTable[frameNumber].clear();



  } catch(HashNotFoundException& e) {} // FIXME: does something need to happen here ? 
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb