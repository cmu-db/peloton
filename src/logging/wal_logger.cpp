//
// Created by Gandeevan R on 2/12/18.
//

#include <fstream>
#include "logging/wal_logger.h"
#include "logging/log_buffer.h"
#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "storage/tile_group.h"



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
  std::ofstream *stream = LogManager::GetInstance().GetFileStream();
  stream->write(disk_buffer_->GetData(), disk_buffer_->GetSize());
  stream->flush();
  disk_buffer_->GetCopySerializedOutput().Reset();

  /* send out the callbacks */
  while(!callbacks_.empty()){
    auto callback_pair = callbacks_.front();
    CallbackFunction callback_func = callback_pair.first;
    void *callback_args = callback_pair.second;

    callback_func(callback_args);
  }
}


void WalLogger::AppendLogBuffer(LogBuffer *log_buffer){

  if(nullptr==log_buffer)
    PL_ASSERT(false);

  disk_buffer_->GetCopySerializedOutput()
          .WriteBytes(log_buffer->GetData(),
                      log_buffer->GetSize());

  CallbackFunction callback = log_buffer->GetCallback();
  void *callback_args = log_buffer->GetCallbackArgs();

  if(nullptr != callback){
    callbacks_.emplace_back(callback, callback_args);
  }


}






}
}