//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reordered_phylog_logger.cpp
//
// Identification: src/backend/logging/reordered_phylog_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <algorithm>
#include <dirent.h>
#include <cstdio>
#include "gc/gc_manager_factory.h"
#include "concurrency/transaction_manager_factory.h"
#include "index/index_factory.h"

#include "catalog/manager.h"
#include "concurrency/epoch_manager_factory.h"
#include "common/container_tuple.h"
#include "logging/logging_util.h"
#include "storage/tile_group.h"
#include "storage/database.h"
#include "storage/tile_group_header.h"
#include "type/ephemeral_pool.h"
#include "storage/storage_manager.h"

#include "logging/reordered_phylog_logger.h"

namespace peloton {
namespace logging {

void ReorderedPhyLogLogger::RegisterWorker(WorkerContext *phylog_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_[phylog_worker_ctx->worker_id].reset(phylog_worker_ctx);
  worker_map_lock_.Unlock();
}

void ReorderedPhyLogLogger::DeregisterWorker(WorkerContext *phylog_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_.erase(phylog_worker_ctx->worker_id);
  worker_map_lock_.Unlock();
}

void ReorderedPhyLogLogger::StartIndexRebulding(const size_t logger_count) {
  PL_ASSERT(recovery_threads_.size() != 0);
  recovery_threads_[0].reset(new std::thread(&ReorderedPhyLogLogger::RunSecIndexRebuildThread, this, logger_count));
}

void ReorderedPhyLogLogger::WaitForIndexRebuilding() {
  recovery_threads_[0]->join();
}

void ReorderedPhyLogLogger::StartRecovery(const size_t checkpoint_eid, const size_t persist_eid, const size_t recovery_thread_count) {

  GetSortedLogFileIdList(checkpoint_eid, persist_eid);
  recovery_pools_.resize(recovery_thread_count);
  recovery_threads_.resize(recovery_thread_count);

  for (size_t i = 0; i < recovery_thread_count; ++i) {

    recovery_pools_[i].reset(new type::EphemeralPool()); //VarlenPool(BACKEND_TYPE_MM)

    recovery_threads_[i].reset(new std::thread(&ReorderedPhyLogLogger::RunRecoveryThread, this, i, checkpoint_eid, persist_eid));
  }
}

void ReorderedPhyLogLogger::WaitForRecovery() {
  for (auto &recovery_thread : recovery_threads_) {
    recovery_thread->join();
  }
}


void ReorderedPhyLogLogger::GetSortedLogFileIdList(const size_t checkpoint_eid, const size_t persist_eid) {
  // Open the log dir
  struct dirent *file;
  DIR *dirp;
  dirp = opendir(this->log_dir_.c_str());
  if (dirp == nullptr) {
    LOG_ERROR("Can not open log directory %s\n", this->log_dir_.c_str());
    exit(EXIT_FAILURE);
  }


  concurrency::EpochManager &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  size_t file_epoch_count = (size_t)(new_file_interval_ / epoch_manager.GetEpochDurationMilliSecond());

  // Filter out all log files
  std::string base_name = logging_filename_prefix_ + "_" + std::to_string(logger_id_) + "_";

  file_eids_.clear();

  while ((file = readdir(dirp)) != nullptr) {
    if (strncmp(file->d_name, base_name.c_str(), base_name.length()) == 0) {
      // Find one log file
      LOG_TRACE("Logger %d find a log file %s\n", (int) logger_id_, file->d_name);
      // Get the file epoch id
      size_t file_eid = (size_t)std::stoi(std::string(file->d_name + base_name.length()));

      if (file_eid + file_epoch_count > checkpoint_eid && file_eid <= persist_eid) {
        file_eids_.push_back(file_eid);
      }

    }
  }

  // Sort in descending order
  std::sort(file_eids_.begin(), file_eids_.end(), std::less<size_t>());
  max_replay_file_id_ = file_eids_.size() - 1;

  // for (auto &entry : file_eids_) {
  //   printf("file id = %lu\n", entry);
  // }

}

txn_id_t ReorderedPhyLogLogger::LockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset) {
  txn_id_t txnid_logger = (this->logger_id_);
  while (true) {
    // We use the txn_id field as a lock. However this field also stored information about whether a tuple is deleted or not.
    // To restore that information, we need to return the old one before overwriting it.
    if (tg_header->SetAtomicTransactionId(tuple_offset, INITIAL_TXN_ID, txnid_logger) == INITIAL_TXN_ID) return INITIAL_TXN_ID;
    if (tg_header->SetAtomicTransactionId(tuple_offset, INVALID_TXN_ID, txnid_logger) == INVALID_TXN_ID) return INVALID_TXN_ID;
    _mm_pause();
  }
}

void ReorderedPhyLogLogger::UnlockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset, txn_id_t new_txn_id) {
  PL_ASSERT(new_txn_id == INVALID_TXN_ID || new_txn_id == INITIAL_TXN_ID);
  tg_header->SetAtomicTransactionId(tuple_offset, (txn_id_t) (this->logger_id_), new_txn_id);
}

bool ReorderedPhyLogLogger::InstallTupleRecord(LogRecordType type, storage::Tuple *tuple, storage::DataTable *table, cid_t cur_cid) {
  // First do an index look up, if current version is newer, skip this record
  //auto pindex = table->GetIndexWithOid(table->GetPrimaryIndexOid());
  //auto pindex_schema = pindex->GetKeySchema();
  //PL_ASSERT(pindex);

  //std::unique_ptr<storage::Tuple> key(new storage::Tuple(pindex_schema, true));
  //key->SetFromTuple(tuple, pindex_schema->GetIndexedColumns(), pindex->GetPool());

  //std::vector<ItemPointer *> itemptr_ptrs;
//  pindex->ScanKey(key.get(), itemptr_ptrs);


//  if (itemptr_ptrs.empty()) {
    // Try to insert a new tuple
    std::function<bool (const void *)> fn = [](const void *t UNUSED_ATTRIBUTE) -> bool {return true;};

    // Allocate a slot from the table's tile group
    ItemPointer insert_location = table->InsertTuple(tuple); // This function does not insert indexes
    if (insert_location.block == INVALID_OID) {
      LOG_ERROR("Failed to get tuple slot");
      return false;
    }

    auto insert_tg_header = catalog::Manager::GetInstance().GetTileGroup(insert_location.block)->GetHeader();

    // Get the lock before trying to insert it into the primary index
    txn_id_t old_txn_id = LockTuple(insert_tg_header, insert_location.offset);
    PL_ASSERT(old_txn_id == INVALID_TXN_ID);

    // TODO: Move some logic into data_table (some logics are similar to DataTable.InsertTuple) -- Jiexi
    // Insert into primary index
    //ItemPointer *itemptr_ptr = new ItemPointer(insert_location);
//    if (pindex->CondInsertEntryInTupleIndex(key.get(), itemptr_ptr, fn) == false) {
      // Already inserted by others (concurrently), fall back to the override approach
//      delete itemptr_ptr;
//      itemptr_ptr = nullptr;
//      UnlockTuple(insert_tg_header, insert_location.offset, old_txn_id);
//      gc::GCManagerFactory::GetInstance().DirectRecycleTuple(table->GetOid(), insert_location);

      // Redo the scan so that we have the correct item pointer
//      pindex->ScanKey(key.get(), itemptr_ptrs);
//    } else {
      // Successfully inserted into the primary index
      // Initialize the tuple's header
      // Fill in the master pointer
     // insert_tg_header->SetMasterPointer(insert_location.offset, itemptr_ptr);

      //concurrency::TransactionManagerFactory::GetInstance()
//        .InitInsertedTupleForRecovery(insert_tg_header, insert_location.offset, itemptr_ptr);

      // TODO: Insert the tuple in all secondary indexes.
      // TODO: May be we should rebuild all secondary indexes after we finish the log replay,
      // so that we can ensure some constraints on the sindexes. --Jiexi

      // Set the time stamp for the new tuple
      insert_tg_header->SetBeginCommitId(insert_location.offset, cur_cid);
      PL_ASSERT(insert_tg_header->GetEndCommitId(insert_location.offset) == MAX_CID);

      // Yield ownership
      UnlockTuple(insert_tg_header, insert_location.offset, (type == LogRecordType::TUPLE_DELETE) ? INVALID_TXN_ID : INITIAL_TXN_ID);
      return true;
//    }
  //}

  // Try to overwrite the tuple
//  PL_ASSERT(itemptr_ptrs.size() == 1);  Primary index property
//  ItemPointer overwrite_location = *(itemptr_ptrs.front());
//  auto tg = catalog::Manager::GetInstance().GetTileGroup(overwrite_location.block);
//  PL_ASSERT(tg);
//  auto tg_header = tg->GetHeader();

  // Acquire the ownership of the tuple, before doing any read/write
//  txn_id_t old_txn_id = LockTuple(tg_header, overwrite_location.offset);

  // Check if we have a newer version of that tuple
//  auto old_cid = tg_header->GetBeginCommitId(overwrite_location.offset);
//  PL_ASSERT(tg_header->GetEndCommitId(overwrite_location.offset) == MAX_CID);
//  if (old_cid < cur_cid) {
    // Overwrite the old version if we are not deleting a tuple
//    if (type != LOGRECORD_TYPE_TUPLE_DELETE) {
//      expression::ContainerTuple<storage::TileGroup> allocated_location(tg.get(), overwrite_location.offset);
//      for (oid_t col_id : tuple->GetSchema()->GetIndexedColumns()) {
//        auto value = tuple->GetValue(col_id);
//        allocated_location.SetValue(col_id, value);
//      }
//    }

    // TODO: May be we should rebuild all secondary indexes after we finish the log replay,
    // so that we can ensure some constraints on the sindexes. --Jiexi

    // Set the begin time stamp before release the lock
//    tg_header->SetBeginCommitId(overwrite_location.offset, cur_cid);
//    PL_ASSERT(tg_header->GetEndCommitId(overwrite_location.offset) == MAX_CID);

    // Release the lock
//    UnlockTuple(tg_header, overwrite_location.offset, (type == LOGRECORD_TYPE_TUPLE_DELETE ? INVALID_TXN_ID : INITIAL_TXN_ID));
//  } else {
    // The installed version is newer than the version in the log
    // Release the ownership without any modification
//    UnlockTuple(tg_header, overwrite_location.offset, old_txn_id);
//  }
//  return true;
}

bool ReorderedPhyLogLogger::ReplayLogFile(const size_t thread_id, FileHandle &file_handle, size_t checkpoint_eid, size_t persist_eid) {
  PL_ASSERT(file_handle.file != nullptr && file_handle.fd != INVALID_FILE_DESCRIPTOR);

  // Status
  size_t current_eid = INVALID_EID;
  cid_t current_cid = INVALID_CID;
  size_t buf_size = 4096;
  std::unique_ptr<char[]> buffer(new char[buf_size]);
  char length_buf[sizeof(int32_t)];

  // TODO: Need some file integrity check. Now we just rely on the the pepoch id and the checkpoint eid
  while (true) {
    // Read the frame length
    if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &length_buf, 4) == false) {
      LOG_TRACE("Reach the end of the log file");
      break;
    }
    CopySerializeInput length_decode((const void *) &length_buf, 4);
    int length = length_decode.ReadInt();
    // printf("length = %d\n", length);
    // Adjust the buffer
    if ((size_t) length > buf_size) {
      buffer.reset(new char[(int)(length * 1.2)]);
      buf_size = (size_t) length;
    }

    if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) buffer.get(), length) == false) {
      LOG_ERROR("Unexpected file eof");
      // TODO: How to handle damaged log file?
      return false;
    }
    CopySerializeInput record_decode((const void *) buffer.get(), length);

    // Check if we can skip this epoch
    if (current_eid != INVALID_EID && (current_eid < checkpoint_eid || current_eid > persist_eid)) {
      // Skip the record if current epoch is before the checkpoint or after persistent epoch
      continue;
    }

    /*
     * Decode the record
     */
    // Get record type
    LogRecordType record_type = (LogRecordType) (record_decode.ReadEnumInSingleByte());
    switch (record_type) {
      case LogRecordType::EPOCH_BEGIN: {
        if (current_eid != INVALID_EID) {
          LOG_ERROR("Incomplete epoch in log record");
          return false;
        }
        current_eid = (size_t) record_decode.ReadLong();
        // printf("begin epoch id = %lu\n", current_eid);
        break;
      } case LogRecordType::EPOCH_END: {
        size_t eid = (size_t) record_decode.ReadLong();
        if (eid != current_eid) {
          LOG_ERROR("Mismatched epoch in log record");
          return false;
        }
        // printf("end epoch id = %lu\n", current_eid);
        current_eid = INVALID_EID;
        break;
      } case LogRecordType::TRANSACTION_BEGIN: {
        if (current_eid == INVALID_EID) {
          LOG_ERROR("Invalid txn begin record");
          return false;
        }
        if (current_cid != INVALID_CID) {
          LOG_ERROR("Incomplete txn in log record");
          return false;
        }
        current_cid = (cid_t) record_decode.ReadLong();

        break;
      } case LogRecordType::TRANSACTION_COMMIT: {
        if (current_eid == INVALID_EID) {
          LOG_ERROR("Invalid txn begin record");
          return false;
        }
        cid_t cid = (cid_t) record_decode.ReadLong();
        if (cid != current_cid) {
          LOG_ERROR("Mismatched txn in log record");
          return false;
        }
        current_cid = INVALID_CID;
        break;
      } case LogRecordType::TUPLE_UPDATE:
      case LogRecordType::TUPLE_DELETE:
      case LogRecordType::TUPLE_INSERT: {
        if (current_cid == INVALID_CID || current_eid == INVALID_EID) {
          LOG_ERROR("Invalid txn tuple record");
          return false;
        }

        if (current_eid < checkpoint_eid || current_eid > persist_eid) {
          break;
        }

        oid_t database_id = (oid_t) record_decode.ReadLong();
        oid_t table_id = (oid_t) record_decode.ReadLong();

        // XXX: We still rely on an alive catalog manager
        auto table = storage::StorageManager::GetInstance()->GetTableWithOid(database_id, table_id);
        auto schema = table->GetSchema();

        // Decode the tuple from the record
        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
        tuple->DeserializeFrom(record_decode, this->recovery_pools_[thread_id].get());

        // FILE *fp = fopen("in_file.txt", "a");
        // for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
        //   int value = ValuePeeker::PeekAsInteger(tuple->GetValue(i));
        //   fprintf(stdout, "%d, ", value);
        // }
        // fprintf(stdout, "\n");
        // fclose(fp);

        // Install the record
        InstallTupleRecord(record_type, tuple.get(), table, current_cid);
        break;
      }
      default:
      LOG_ERROR("Unknown log record type");
        return false;
    }

  }

  return true;
}

void ReorderedPhyLogLogger::RunRecoveryThread(const size_t thread_id, const size_t checkpoint_eid, const size_t persist_eid) {

  while (true) {

    int replay_file_id = max_replay_file_id_.fetch_sub(1, std::memory_order_relaxed);
    if (replay_file_id < 0) {
      break;
    }

    size_t file_eid = file_eids_.at(replay_file_id);
    // printf("start replaying file id = %d, file eid = %lu\n", replay_file_id, file_eid);
    // Replay a single file
    std::string filename = GetLogFileFullPath(file_eid);
    FileHandle file_handle;
    // std::cout<<"filename = " << filename << std::endl;
    bool res = LoggingUtil::OpenFile(filename.c_str(), "rb", file_handle);
    if (res == false) {
      LOG_ERROR("Cannot open log file %s\n", filename.c_str());
      exit(EXIT_FAILURE);
    }
    ReplayLogFile(thread_id, file_handle, checkpoint_eid, persist_eid);

    // Safely close the file
    res = LoggingUtil::CloseFile(file_handle);
    if (res == false) {
      LOG_ERROR("Cannot close pepoch file");
      exit(EXIT_FAILURE);
    }

  }

}

void ReorderedPhyLogLogger::RunSecIndexRebuildThread(const size_t logger_count) {
  auto manager = storage::StorageManager::GetInstance();
  auto db_count = manager->GetDatabaseCount();

  // Loop all databases
  for (oid_t db_idx = 0; db_idx < db_count; db_idx ++) {
    auto database = manager->GetDatabaseWithOid(db_idx);
    auto table_count = database->GetTableCount();

    // Loop all tables
    for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      // Get the target table
      storage::DataTable *table = database->GetTable(table_idx);
      RebuildSecIndexForTable(logger_count, table);
    }
  }
}

void ReorderedPhyLogLogger::RebuildSecIndexForTable(const size_t logger_count, storage::DataTable *table) {
  size_t tg_count = table->GetTileGroupCount();

  // Loop all the tile groups, shared by thread id
  for (size_t tg_idx = logger_id_; tg_idx < tg_count; tg_idx += logger_count) {
    auto tg = table->GetTileGroup(tg_idx).get();
    auto tg_header = tg->GetHeader();
    //auto tg_id = tg->GetTileGroupId();
    oid_t active_tuple_count = tg->GetNextTupleSlot();

    // Loop all tuples headers in the tile group
    for (oid_t tuple_offset = 0; tuple_offset < active_tuple_count; ++tuple_offset) {
      // Check if the tuple is valid
      PL_ASSERT(tg_header->GetTransactionId(tuple_offset) == INITIAL_TXN_ID);
      // Insert in into the index
      //expression::ContainerTuple<storage::TileGroup> container_tuple(tg, tuple_offset);

      //bool res;

        //res = table->InsertInSecondaryIndexes(&container_tuple, ItemPointer(tg_id, tuple_offset), true);


//      if (res == false) {
//        LOG_ERROR("Index constraint violation");
//      }
    }
  }
}

void ReorderedPhyLogLogger::Run() {
  // TODO: Ensure that we have called run recovery before

  concurrency::EpochManager &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

  size_t file_epoch_count = (size_t)(new_file_interval_ / epoch_manager.GetEpochDurationMilliSecond());

  std::list<std::pair<FileHandle*, size_t>> file_handles;

  size_t current_file_eid = 0; //epoch_manager.GetCurrentEpochId();

  FileHandle *new_file_handle = new FileHandle();
  file_handles.push_back(std::make_pair(new_file_handle, current_file_eid));

  std::string filename = GetLogFileFullPath(current_file_eid);
  // Create a new file
  if (LoggingUtil::OpenFile(filename.c_str(), "wb", *new_file_handle) == false) {
    LOG_ERROR("Unable to create log file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  auto &epoch_mamager = concurrency::EpochManagerFactory::GetInstance();

  /**
   *  Main loop
   */
  while (true) {
    if (is_running_ == false) { break; }

    std::this_thread::sleep_for(
      std::chrono::microseconds(epoch_mamager.GetEpochLengthInMicroSecQuarter()));

    size_t current_global_eid = epoch_mamager.GetCurrentEpochId();

    // Pull log records from workers per epoch buffer
    {
      worker_map_lock_.Lock();

      size_t min_workers_persist_eid = INVALID_EID;
      for (auto &worker_entry : worker_map_) {
        // For every alive worker, move its buffer to the logger's local buffer
        auto worker_ctx_ptr = worker_entry.second.get();

        size_t last_persist_eid = worker_ctx_ptr->persist_eid;

        // Since idle worker has MAX_EID, we need a std::min here
        // Note: std::min pass a const & of into the function. We need to first copy the shared worker_ctx_ptr->current_commit_eid
        // into a local variable before calling std::min. Otherwise we will have a Heisenbug where std::min first check which one is smaller,
        // and before it returns, other thread modify the variable and we actually return the larger one.
        size_t worker_current_eid = worker_ctx_ptr->current_commit_eid;
        worker_current_eid = std::min(worker_current_eid, current_global_eid);

        PL_ASSERT(last_persist_eid <= worker_current_eid);

        if (last_persist_eid == worker_current_eid) {
          // The worker makes no progress
          continue;
        }

        for (size_t epoch_id = last_persist_eid + 1; epoch_id <= worker_current_eid; ++epoch_id) {

          size_t epoch_idx = epoch_id % epoch_manager.GetEpochQueueCapacity();
          // get all the buffers that are associated with the epoch.
          auto &buffers = worker_ctx_ptr->per_epoch_buffer_ptrs[epoch_idx];

          if (buffers.empty() == true) {
            // no transaction log is generated within this epoch.
            // or we have logged this epoch before.
            // just skip it.
            continue;
          }

          // if we have something to write, then check whether we need to create new file.
          FileHandle *file_handle = nullptr;

          for (auto &entry : file_handles) {
            if (epoch_id >= entry.second && epoch_id < entry.second + file_epoch_count) {
              file_handle = entry.first;
            }
          }
          while (file_handle == nullptr) {
            current_file_eid = current_file_eid + file_epoch_count;
            // printf("create new file with epoch id = %lu, last persist eid = %lu, current eid = %lu\n", current_file_eid, last_persist_eid, worker_current_eid);
            FileHandle *new_file_handle = new FileHandle();
            file_handles.push_back(std::make_pair(new_file_handle, current_file_eid));

            std::string filename = GetLogFileFullPath(current_file_eid);
            // Create a new file
            if (LoggingUtil::OpenFile(filename.c_str(), "wb", *new_file_handle) == false) {
              LOG_ERROR("Unable to create log file %s\n", filename.c_str());
              exit(EXIT_FAILURE);
            }
            if (epoch_id >= current_file_eid && epoch_id < current_file_eid + file_epoch_count) {
              file_handle = new_file_handle;
              break;
            }
          }


          PersistEpochBegin(*file_handle, epoch_id);
          // persist all the buffers.
          while (buffers.empty() == false) {
            // Check if the buffer is empty
            // TODO: is it possible to have an empty buffer??? --YINGJUN
            if (buffers.top()->Empty()) {
              worker_ctx_ptr->buffer_pool.PutBuffer(std::move(buffers.top()));
            } else {
              PersistLogBuffer(*file_handle, std::move(buffers.top()));
            }
            PL_ASSERT(buffers.top() == nullptr);
            buffers.pop();
          }
          PersistEpochEnd(*file_handle, epoch_id);
          // Call fsync
          LoggingUtil::FFlushFsync(*file_handle);
        } // end for

        worker_ctx_ptr->persist_eid = worker_current_eid - 1;

        if (min_workers_persist_eid == INVALID_EID || min_workers_persist_eid > (worker_current_eid - 1)) {
          min_workers_persist_eid = worker_current_eid - 1;
        }

      } // end for

      if (min_workers_persist_eid == INVALID_EID) {
        // in this case, it is likely that there's no registered worker or there's nothing to persist.
        worker_map_lock_.Unlock();
        continue;
      }

      // Currently when there is a long running txn, the logger can only know the worker's eid when the worker is committing the txn.
      // We should switch to localized epoch manager to solve this problem.
      // XXX: work around. We should switch to localized epoch manager to solve this problem
      if (min_workers_persist_eid < persist_epoch_id_) {
        min_workers_persist_eid = persist_epoch_id_;
      }

      PL_ASSERT(min_workers_persist_eid >= persist_epoch_id_);

      persist_epoch_id_ = min_workers_persist_eid;

      auto list_iter = file_handles.begin();

      while (list_iter != file_handles.end()) {
        if (list_iter->second + file_epoch_count <= persist_epoch_id_) {
          FileHandle *file_handle = list_iter->first;

          // Safely close the file
          bool res = LoggingUtil::CloseFile(*file_handle);
          if (res == false) {
            LOG_ERROR("Cannot close log file under directory %s", log_dir_.c_str());
            exit(EXIT_FAILURE);
          }

          delete file_handle;
          file_handle = nullptr;

          list_iter = file_handles.erase(list_iter);

        } else {
          ++list_iter;
        }
      }


      worker_map_lock_.Unlock();
    }
  }

  // Close the log file
  // TODO: Seek and write the integrity information in the header

  // Safely close all the files

  auto list_iter = file_handles.begin();

  while (list_iter != file_handles.end()) {
    FileHandle *file_handle = list_iter->first;

    bool res = LoggingUtil::CloseFile(*file_handle);
    if (res == false) {
      LOG_ERROR("Cannot close log file under directory %s", log_dir_.c_str());
      exit(EXIT_FAILURE);
    }

    delete file_handle;
    file_handle = nullptr;

    list_iter = file_handles.erase(list_iter);
  }
}

void ReorderedPhyLogLogger::PersistEpochBegin(FileHandle &file_handle, const size_t epoch_id) {
  // Write down the epoch begin record
  LogRecord record = LogRecordFactory::CreateEpochRecord(LogRecordType::EPOCH_BEGIN, epoch_id);

  logger_output_buffer_.Reset();

  size_t start = logger_output_buffer_.Position();
  logger_output_buffer_.WriteInt(0);

  logger_output_buffer_.WriteEnumInSingleByte(static_cast<std::underlying_type_t<LogRecordType>>(LogRecordType::EPOCH_BEGIN));
  logger_output_buffer_.WriteLong((uint64_t) epoch_id);

  logger_output_buffer_.WriteIntAt(start, (int32_t) (logger_output_buffer_.Position() - start - sizeof(int32_t)));

  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle.file);
}

void ReorderedPhyLogLogger::PersistEpochEnd(FileHandle &file_handle, const size_t epoch_id) {
  // Write down the epoch end record
  LogRecord record = LogRecordFactory::CreateEpochRecord(LogRecordType::EPOCH_END, epoch_id);

  logger_output_buffer_.Reset();

  size_t start = logger_output_buffer_.Position();
  logger_output_buffer_.WriteInt(0);

  logger_output_buffer_.WriteEnumInSingleByte(static_cast<std::underlying_type_t<LogRecordType>>(LogRecordType::EPOCH_END));
  logger_output_buffer_.WriteLong((uint64_t) epoch_id);

  logger_output_buffer_.WriteIntAt(start, (int32_t) (logger_output_buffer_.Position() - start - sizeof(int32_t)));

  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle.file);

}

void ReorderedPhyLogLogger::PersistLogBuffer(FileHandle &file_handle, std::unique_ptr<LogBuffer> log_buffer) {

  fwrite((const void *) (log_buffer->GetData()), log_buffer->GetSize(), 1, file_handle.file);

  // Return the buffer to the worker
  log_buffer->Reset();
  /*auto itr = worker_map_.find(log_buffer->GetWorkerId());
  if (itr != worker_map_.end()) {
    itr->second->buffer_pool.PutBuffer(std::move(log_buffer));
  } else {
    // In this case, the worker is already terminated and removed
    // Release the buffer
    log_buffer.reset(nullptr);
  }*/
}


}
}
