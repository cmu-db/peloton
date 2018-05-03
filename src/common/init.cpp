//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// init.cpp
//
// Identification: src/common/init.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/init.h"

#include <gflags/gflags.h>
#include <google/protobuf/stubs/common.h>

#include "catalog/catalog.h"
#include "common/statement_cache_manager.h"
#include "common/thread_pool.h"
#include "concurrency/transaction_manager_factory.h"
#include "gc/gc_manager_factory.h"
#include "index/index.h"
#include "settings/settings_manager.h"
#include "threadpool/mono_queue_pool.h"
#include "tuning/index_tuner.h"
#include "tuning/layout_tuner.h"

namespace peloton {

ThreadPool thread_pool;

void PelotonInit::Initialize() {
  CONNECTION_THREAD_COUNT = settings::SettingsManager::GetInt(
      settings::SettingId::connection_thread_count);
  LOGGING_THREAD_COUNT = 1;
  GC_THREAD_COUNT = 1;
  EPOCH_THREAD_COUNT = 1;

  // set max thread number.
  thread_pool.Initialize(0, CONNECTION_THREAD_COUNT + 3);

  // start worker pool
  threadpool::MonoQueuePool::GetInstance().Startup();

  // start indextuner thread pool
  if (settings::SettingsManager::GetBool(settings::SettingId::brain)) {
    threadpool::MonoQueuePool::GetBrainInstance().Startup();
  }

  int parallelism = (CONNECTION_THREAD_COUNT + 3) / 4;
  storage::DataTable::SetActiveTileGroupCount(parallelism);
  storage::DataTable::SetActiveIndirectionArrayCount(parallelism);

  // start epoch.
  concurrency::EpochManagerFactory::GetInstance().StartEpoch();

  // start GC.
  gc::GCManagerFactory::Configure(settings::SettingsManager::GetInt(settings::SettingId::gc_num_threads));
  gc::GCManagerFactory::GetInstance().StartGC();

  // start index tuner
  if (settings::SettingsManager::GetBool(settings::SettingId::index_tuner)) {
    // Set the default visibility flag for all indexes to false
    index::IndexMetadata::SetDefaultVisibleFlag(false);
    auto &index_tuner = tuning::IndexTuner::GetInstance();
    index_tuner.Start();
  }

  // start layout tuner
  if (settings::SettingsManager::GetBool(settings::SettingId::layout_tuner)) {
    auto &layout_tuner = tuning::LayoutTuner::GetInstance();
    layout_tuner.Start();
  }

  // Initialize catalog
  auto pg_catalog = catalog::Catalog::GetInstance();
  pg_catalog->Bootstrap();  // Additional catalogs
  settings::SettingsManager::GetInstance().InitializeCatalog();

  // begin a transaction
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // initialize the catalog and add the default database, so we don't do this on
  // the first query
  pg_catalog->CreateDatabase(DEFAULT_DB_NAME, txn);

  txn_manager.CommitTransaction(txn);

  // Initialize the Statement Cache Manager
  StatementCacheManager::Init();
}

void PelotonInit::Shutdown() {
  // shut down index tuner
  if (settings::SettingsManager::GetBool(settings::SettingId::index_tuner)) {
    auto &index_tuner = tuning::IndexTuner::GetInstance();
    index_tuner.Stop();
  }

  // shut down layout tuner
  if (settings::SettingsManager::GetBool(settings::SettingId::layout_tuner)) {
    auto &layout_tuner = tuning::LayoutTuner::GetInstance();
    layout_tuner.Stop();
  }

  // shut down GC.
  gc::GCManagerFactory::GetInstance().StopGC();

  // shut down epoch.
  concurrency::EpochManagerFactory::GetInstance().StopEpoch();

  // stop worker pool
  threadpool::MonoQueuePool::GetInstance().Shutdown();

  // stop indextuner thread pool
  if (settings::SettingsManager::GetBool(settings::SettingId::brain)) {
    threadpool::MonoQueuePool::GetBrainInstance().Shutdown();
  }

  thread_pool.Shutdown();

  // shutdown protocol buf library
  google::protobuf::ShutdownProtobufLibrary();

  // clear parameters
  google::ShutDownCommandLineFlags();
}

void PelotonInit::SetUpThread() {}

void PelotonInit::TearDownThread() {}

}  // namespace peloton
