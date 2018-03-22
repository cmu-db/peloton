#include "threadpool/logger_queue_pool.h"
#include "common/container/lock_free_queue.h"
#include "logging/wal_logger.h"


namespace peloton{
namespace threadpool{



void LoggerFunc(std::atomic_bool *is_running, LoggerQueue *logger_queue) {
  constexpr auto kMinPauseTime = std::chrono::microseconds(1);
  constexpr auto kMaxPauseTime = std::chrono::microseconds(1000);
  auto pause_time = kMinPauseTime;

  logging::WalLogger logger;

  while (is_running->load() || !logger_queue->IsEmpty()) {

    logging::LogBuffer *log_buffer = nullptr;

    if (!logger_queue->Dequeue(log_buffer)) {
      // Polling with exponential backoff
      std::this_thread::sleep_for(pause_time);
      pause_time = std::min(pause_time * 2, kMaxPauseTime);
    } else {

      logger.AppendLogBuffer(log_buffer);

      if(logger.IsFlushNeeded(logger_queue->IsEmpty())){
        logger.FlushToDisk();
      }

      // TODO(gandeevan): free log buffers

      pause_time = kMinPauseTime;
    }
  }

}

}
}