//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_tuner.cpp
//
// Identification: src/brain/index_tuner.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_tuner.h"
#include "brain/clusterer.h"

#include "catalog/schema.h"
#include "common/logger.h"
#include "storage/data_table.h"

namespace peloton {
namespace brain {

IndexTuner &IndexTuner::GetInstance() {
  static IndexTuner index_tuner;
  return index_tuner;
}

IndexTuner::IndexTuner(){
  // Nothing to do here !
}

IndexTuner::~IndexTuner(){
  // Nothing to do here !
}

void IndexTuner::Start(){

  // Set signal
  index_tuning_stop = false;

  // Launch thread
  index_tuner_thread = std::thread(&brain::IndexTuner::Tune, this);

}

static void IndexTuneHelper(storage::DataTable* table) {

  // Process all samples in table
  auto& samples = table->GetIndexSamples();

  // Check if we have any samples
  if (samples.empty()) {
    return;
  }

  // Clear all samples in table
  table->ClearIndexSamples();

}

void IndexTuner::Tune(){


  // Continue till signal is not false
  while(index_tuning_stop == false) {

    // Go over all tables
    for(auto table : tables) {

      // Update indices periodically
      IndexTuneHelper(table);

      // Sleep a bit
      std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
    }

  }

}

void IndexTuner::Stop(){

  // Stop tuning
  index_tuning_stop = true;

  // Stop thread
  index_tuner_thread.join();

}

void IndexTuner::AddTable(storage::DataTable* table){
  {
    std::lock_guard<std::mutex> lock(index_tuner_mutex);
    LOG_INFO("table : %p", table);

    tables.push_back(table);
  }
}

void IndexTuner::ClearTables() {
  {
    std::lock_guard<std::mutex> lock(index_tuner_mutex);
    tables.clear();
  }
}


}  // End brain namespace
}  // End peloton namespace
