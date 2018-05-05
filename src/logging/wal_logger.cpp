//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_logger.cpp
//
// Identification: src/logging/wal_logger.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <fstream>
#include "logging/wal_logger.h"
#include "logging/log_buffer.h"
#include "catalog/manager.h"
#include "common/container_tuple.h"


namespace peloton{
namespace logging{

bool WalLogger::IsFlushNeeded(bool pending_buffers){

  /* if the disk buffer is full then we flush to the disk
   * irrespective of whether there are pending callbacks
   */
  if(disk_buffer_->HasThresholdExceeded())
    return true;

  /* if the disk buffer is not full and there are pending buffers
   * in the queue then we don't flush to the disk now. We try to
   * consolidate the pending buffers to achieve group commit.
   */
  if(pending_buffers)
    return false;

  /* if there are no pending buffers (at the time of check), but
   * there are pending callbacks, we go ahead and flush the
   * buffers.
   */
  if(!callbacks_.empty())
    return true;

  return false;
}


void WalLogger::FlushToDisk(){

  if(disk_buffer_->GetSize()==0){
    return;
  }

  std::ofstream *stream = LogManager::GetInstance().GetFileStream();
  stream->write(disk_buffer_->GetData(), disk_buffer_->GetSize());

  if(stream->fail()){
    PELOTON_ASSERT(false);
  }

  stream->flush();

  if(stream->fail()){
    PELOTON_ASSERT(false);
  }


  disk_buffer_->GetCopySerializedOutput().Reset();


  /* send out the callbacks */
  while(!callbacks_.empty()){
    auto callback = callbacks_.front();
    callbacks_.pop_front();
    callback();
  }

}


void WalLogger::PerformCompaction(LogBuffer *log_buffer){

  if(nullptr==log_buffer)
    PELOTON_ASSERT(false);

  disk_buffer_->GetCopySerializedOutput()
          .WriteBytes(log_buffer->GetData(),
                      log_buffer->GetSize());

  auto callback = log_buffer->GetLoggerCallback();

  if(nullptr != callback){
    callbacks_.push_back(std::move(callback));
  }


}
}
}