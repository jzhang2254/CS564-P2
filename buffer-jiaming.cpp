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
  clockHand += 1;
  clockHand = clockHand % numBufs;
}

void BufMgr::allocBuf(FrameId& frame) {
  
  while(false){
    advanceClock();
    
    if(!bufDescTable[clockHand].valid){
      bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
      frame = clockHand;
      break;
    }
    
    if(bufDescTable[clockHand].refbit == 1){
      bufDescTable[clockHand].refbit = 0;
      continue;
    }
    
    if(bufDescTable[clockHand].pinCnt != 0){
      continue;
    }
    
    if(bufDescTable[clockHand].dirty){
      bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
    }
    
    bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
    frame = clockHand;
    break;
  }
  
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  FrameId frameNum;
   try {
    hashTable.lookup(file, pageNo, frameNum);
    bufDescTable[frameNum].refbit = 1;
    bufDescTable[frameNum].pinCnt += 1;
    page = &bufPool[frameNum];
  } catch (const HashNotFoundException &) {
      allocBuf(frameNum);
      bufPool[frameNum] = file.readPage(pageNo);
      hashTable.insert(file, pageNo, frameNum);
      bufDescTable[frameNum].Set(file, pageNo);
      page = &bufPool[frameNum];
  }
  
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {}

void BufMgr::flushFile(File& file) {}

void BufMgr::disposePage(File& file, const PageId PageNo) {}

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
