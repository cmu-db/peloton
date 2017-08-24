//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint_manager.cpp
//
// Identification: src/backend/logging/checkpoint_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/checkpoint_manager.h"
#include "logging/logging_util.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "concurrency/transaction_manager.h"
#include "concurrency/transaction_manager_factory.h"
#include "concurrency/epoch_manager_factory.h"
#include "type/ephemeral_pool.h"
#include "common/timer.h"
#include "storage/storage_manager.h"

namespace peloton {
namespace logging {

  size_t CheckpointManager::DoRecovery() {

    std::vector<std::vector<size_t>> database_structures;

    size_t persist_epoch_id = RecoverPepoch(database_structures);
    
    // reload all the files with file name containing "persist epoch id"
    RecoverCheckpoint(persist_epoch_id, database_structures);
    return persist_epoch_id;
  }

  size_t CheckpointManager::RecoverPepoch(std::vector<std::vector<size_t>> &database_structures) {
    FileHandle file_handle;
    std::string filename = ckpt_pepoch_dir_ + "/" + ckpt_pepoch_filename_;
    // Create a new file
    if (LoggingUtil::OpenFile(filename.c_str(), "rb", file_handle) == false) {
      LOG_ERROR("Unable to open pepoch file %s\n", filename.c_str());
      exit(EXIT_FAILURE);
    }

    size_t buf_size = 4096;
    std::unique_ptr<char[]> buffer(new char[buf_size]);
    char length_buf[sizeof(int32_t)];

    
    int length = 0;
    while (true) {
      // Read the frame length
      if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &length_buf, 4) == false) {
        LOG_TRACE("Reach the end of the log file");
        break;
      }

      CopySerializeInput length_decode((const void *) &length_buf, 4);
      length = length_decode.ReadInt();

      if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) buffer.get(), length) == false) {
        LOG_ERROR("Unexpected file eof");
        // TODO: How to handle damaged log file?
        return false;
      }
    }
    CopySerializeInput record_decode((const void *) buffer.get(), length);

    size_t persist_epoch_id = (size_t) record_decode.ReadLong();


    int database_count = record_decode.ReadInt();

    database_structures.resize(database_count);

    for (oid_t database_idx = 0; database_idx < (oid_t)database_count; ++database_idx) {
      
      int table_count = record_decode.ReadInt();

      for (oid_t table_idx = 0; table_idx < (oid_t)table_count; table_idx++) {
        int tile_group_count = record_decode.ReadInt();

        database_structures[database_idx].push_back(tile_group_count);
      }
    }

    // Safely close the file
    bool res = LoggingUtil::CloseFile(file_handle);
    if (res == false) {
      LOG_ERROR("Cannot close pepoch file");
      exit(EXIT_FAILURE);
    }

    return persist_epoch_id;
  }

  void CheckpointManager::RecoverCheckpoint(const size_t &epoch_id, const std::vector<std::vector<size_t>> &database_structures) {

    // open files
    FileHandle ***file_handles = new FileHandle**[database_structures.size()];
    
    for (oid_t database_idx = 0; database_idx < database_structures.size(); database_idx++) {
    
      file_handles[database_idx] = new FileHandle*[database_structures[database_idx].size()];

      for (oid_t table_idx = 0; table_idx < database_structures[database_idx].size(); table_idx++) {
        
        file_handles[database_idx][table_idx] = new FileHandle[max_checkpointer_count_];

        for (size_t virtual_checkpointer_id = 0; virtual_checkpointer_id < max_checkpointer_count_; virtual_checkpointer_id++) {
          
          std::string file_name = GetCheckpointFileFullPath(virtual_checkpointer_id % checkpointer_count_, virtual_checkpointer_id, database_idx, table_idx, epoch_id);
         
          bool success =
              LoggingUtil::OpenFile(file_name.c_str(), "rb", file_handles[database_idx][table_idx][virtual_checkpointer_id]);
          
          PL_ASSERT(success == true);
          if (success != true) {
            LOG_ERROR("create file failed!");
            exit(-1);
          }
        }
      }
    }

    // before parallel recovery, we may need to prepare tables.
    // PrepareTables(database_structures);

    /////////////////////////////////////////////////////////////////////
    // after obtaining the database structures, we can start recovering checkpoints in parallel.
    recovery_pools_.resize(recovery_thread_count_);
    for (size_t i = 0; i < recovery_thread_count_; ++i) {
      recovery_pools_[i].reset(new type::EphemeralPool()); //BACKEND_TYPE_MM
    }
    
    std::vector<std::unique_ptr<std::thread>> recovery_threads;
    recovery_threads.resize(recovery_thread_count_);
    for (size_t i = 0; i < recovery_thread_count_; ++i) {
      recovery_threads[i].reset(new std::thread(&CheckpointManager::RecoverCheckpointThread, this, i, epoch_id, std::ref(database_structures), file_handles));
    }
    for (size_t i = 0; i < recovery_thread_count_; ++i) {
      recovery_threads[i]->join();
    }


    /////////////////////////////////////////////////////////////////////
    // close files

    // auto &catalog_manager = catalog::Manager::GetInstance();

    // loop all databases
    for (oid_t database_idx = 0; database_idx < database_structures.size(); database_idx++) {
      // auto database = catalog_manager.GetDatabase(database_idx);
      
      // loop all tables
      for (oid_t table_idx = 0; table_idx < database_structures[database_idx].size(); table_idx++) {
    
        for (size_t virtual_checkpointer_id = 0; virtual_checkpointer_id < max_checkpointer_count_; virtual_checkpointer_id++) {
          
          fclose(file_handles[database_idx][table_idx][virtual_checkpointer_id].file);

        }
        delete[] file_handles[database_idx][table_idx];

        // Get the target table
        // storage::DataTable *target_table = database->GetTable(table_idx);
        // PL_ASSERT(target_table);

        // size_t num_tuples = (size_t)target_table->GetNumberOfTuples();
        // for (size_t index_id = 0; index_id < target_table->GetIndexCount(); ++index_id) {
        //   auto index = target_table->GetIndex(index_id);
        //  }


      }  // end table looping
      delete[] file_handles[database_idx];
    }  // end database looping
    delete[] file_handles;
  }



  void CheckpointManager::StartCheckpointing() {
    is_running_ = true;
    central_checkpoint_thread_.reset(new std::thread(&CheckpointManager::Running, this));
  }

  void CheckpointManager::StopCheckpointing() {
    is_running_ = false;
    central_checkpoint_thread_->join();
  }

  void CheckpointManager::Running() {
    FileHandle file_handle;
    std::string filename = ckpt_pepoch_dir_ + "/" + ckpt_pepoch_filename_;
    // Create a new file
    if (LoggingUtil::OpenFile(filename.c_str(), "wb", file_handle) == false) {
      LOG_ERROR("Unable to create ckpt pepoch file %s\n", filename.c_str());
      exit(EXIT_FAILURE);
    }

    bool is_init = true;
    int count = 0;
    while (1) {
      if (is_running_ == false) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      ++count;
      if (count == checkpoint_interval_ || is_init == true) {

        LOG_DEBUG("start performing checkpoint...");

        Timer<std::milli> checkpoint_timer;
        checkpoint_timer.Start();
        
        is_init = false;
        // first of all, we should get a snapshot txn id.
        auto transaction = concurrency::TransactionManagerFactory::GetInstance().BeginTransaction(IsolationLevelType::SNAPSHOT);
        //auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
        cid_t begin_cid = transaction->GetEpochId();
        //epoch_manager.EnterEpoch(transaction->GetThreadId(),);



        // prepare database structures.
        // each checkpoint thread must observe the same database structures.
        // this guarantees that the workload can be partitioned consistently.
        std::vector<std::vector<size_t>> database_structures;

        auto storage_manager = storage::StorageManager::GetInstance();
        auto database_count = storage_manager->GetDatabaseCount();

        database_structures.resize(database_count);

        for (oid_t database_idx = 0; database_idx < database_count; ++database_idx) {
          auto database = storage_manager->GetDatabaseWithOid(database_idx);
          auto table_count = database->GetTableCount();

          database_structures[database_idx].resize(table_count);

          for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      
            storage::DataTable *target_table = database->GetTable(table_idx);

            database_structures[database_idx][table_idx] = target_table->GetTileGroupCount();
          }
        }

        PerformCheckpoint(begin_cid, database_structures);
        size_t epoch_id = begin_cid >> 32;
        // exit epoch.
        //epoch_manager.ExitReadOnlyEpoch(epoch_id);
        concurrency::TransactionManagerFactory::GetInstance().EndTransaction(transaction);



        ckpt_pepoch_buffer_.Reset();

        size_t start = ckpt_pepoch_buffer_.Position();
        ckpt_pepoch_buffer_.WriteInt(0);

        ckpt_pepoch_buffer_.WriteLong((uint64_t) epoch_id);

        ckpt_pepoch_buffer_.WriteInt(database_count);

        for (oid_t database_idx = 0; database_idx < database_count; ++database_idx) {

          size_t table_count = database_structures[database_idx].size();
          ckpt_pepoch_buffer_.WriteInt(table_count);
  
          for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
            ckpt_pepoch_buffer_.WriteInt((int)database_structures[database_idx][table_idx]);
          }
        }

        ckpt_pepoch_buffer_.WriteIntAt(start, (int32_t) (ckpt_pepoch_buffer_.Position() - start - sizeof(int32_t)));

        fwrite((const void *) (ckpt_pepoch_buffer_.Data()), ckpt_pepoch_buffer_.Size(), 1, file_handle.file);
        
        LoggingUtil::FFlushFsync(file_handle);
    
        count = 0;

    
        checkpoint_timer.Stop();
    
        LOG_INFO("checkpoint duration: %lf ms", checkpoint_timer.GetDuration());
      }
    }
  }

  void CheckpointManager::PerformCheckpoint(const cid_t &begin_cid, const std::vector<std::vector<size_t>> &database_structures) {
    
    size_t epoch_id = begin_cid >> 32;

    // creating files
    FileHandle ***file_handles = new FileHandle**[database_structures.size()];
    
    for (oid_t database_idx = 0; database_idx < database_structures.size(); database_idx++) {
    
      file_handles[database_idx] = new FileHandle*[database_structures[database_idx].size()];

      for (oid_t table_idx = 0; table_idx < database_structures[database_idx].size(); table_idx++) {
        
        file_handles[database_idx][table_idx] = new FileHandle[max_checkpointer_count_];

        for (size_t virtual_checkpointer_id = 0; virtual_checkpointer_id < max_checkpointer_count_; virtual_checkpointer_id++) {
          
          std::string file_name = GetCheckpointFileFullPath(virtual_checkpointer_id % checkpointer_count_, virtual_checkpointer_id, database_idx, table_idx, epoch_id);
         
          bool success =
              LoggingUtil::OpenFile(file_name.c_str(), "wb", file_handles[database_idx][table_idx][virtual_checkpointer_id]);
          
          PL_ASSERT(success == true);
          if (success != true) {
            LOG_ERROR("create file failed!");
            exit(-1);
          }
        }
      }
    }
    
    /////////////////////////////////////////////////////////////////////
    // after obtaining the database structures, we can start performing checkpointing in parallel.
    std::vector<std::unique_ptr<std::thread>> checkpoint_threads;
    checkpoint_threads.resize(checkpointer_count_);
    for (size_t i = 0; i < checkpointer_count_; ++i) {
      checkpoint_threads[i].reset(new std::thread(&CheckpointManager::PerformCheckpointThread, this, i, begin_cid, std::ref(database_structures), file_handles));
    }
    
    for (size_t i = 0; i < checkpointer_count_; ++i) {
      checkpoint_threads[i]->join();
    }
    /////////////////////////////////////////////////////////////////////

    // close files
    for (oid_t database_idx = 0; database_idx < database_structures.size(); database_idx++) {
      
      for (oid_t table_idx = 0; table_idx < database_structures[database_idx].size(); table_idx++) {
    
        for (size_t virtual_checkpointer_id = 0; virtual_checkpointer_id < max_checkpointer_count_; virtual_checkpointer_id++) {
          
          // Call fsync
          LoggingUtil::FFlushFsync(file_handles[database_idx][table_idx][virtual_checkpointer_id]);

          fclose(file_handles[database_idx][table_idx][virtual_checkpointer_id].file);

        }
        delete[] file_handles[database_idx][table_idx];
      }
      delete[] file_handles[database_idx];
    }
    delete[] file_handles;
  }

  void CheckpointManager::PerformCheckpointThread(const size_t &thread_id, const cid_t &begin_cid, const std::vector<std::vector<size_t>> &database_structures, FileHandle ***file_handles) {

    auto storage_manager = storage::StorageManager::GetInstance();

    // loop all databases
    for (oid_t database_idx = 0; database_idx < database_structures.size(); database_idx++) {
      auto database = storage_manager->GetDatabaseWithOid(database_idx);
      
      // loop all tables
      for (oid_t table_idx = 0; table_idx < database_structures[database_idx].size(); table_idx++) {
        // Get the target table
        storage::DataTable *target_table = database->GetTable(table_idx);
        PL_ASSERT(target_table);

        size_t tile_group_count = database_structures.at(database_idx).at(table_idx);

        CheckpointTable(target_table, tile_group_count, thread_id, begin_cid, file_handles[database_idx][table_idx]);
      }
    }

  }


  void CheckpointManager::RecoverCheckpointThread(const size_t &thread_id, const size_t &epoch_id, const std::vector<std::vector<size_t>> &database_structures, FileHandle ***file_handles) {

    cid_t current_cid = (epoch_id << 32) | 0x0;

    auto storage_manager = storage::StorageManager::GetInstance();

    // loop all databases
    for (oid_t database_idx = 0; database_idx < database_structures.size(); database_idx++) {
      auto database = storage_manager->GetDatabaseWithOid(database_idx);
    
      // loop all tables
      for (oid_t table_idx = 0; table_idx < database_structures[database_idx].size(); table_idx++) {
        // Get the target table
        storage::DataTable *target_table = database->GetTable(table_idx);
        PL_ASSERT(target_table);

        RecoverTable(target_table, thread_id, current_cid, file_handles[database_idx][table_idx]);

      } // end table looping
    } // end database looping
  }




  // Visibility check
  bool CheckpointManager::IsVisible(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id, const cid_t &begin_cid) {
    txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
    cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
    cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

    bool activated = (begin_cid >= tuple_begin_cid);
    bool invalidated = (begin_cid >= tuple_end_cid);

    if (tuple_txn_id == INVALID_TXN_ID) {
      // the tuple is not available.
      return false;
    }

    // there are exactly two versions that can be owned by a transaction.
    // unless it is an insertion.

    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // never read an uncommitted version.
        return false;
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return true;
        } else {
          return false;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {
        return true;
      } else {
        return false;
      }
    }
  }

}
}
