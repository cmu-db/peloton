//
// Created by Rui Wang on 16-4-29.
//

#pragma once

#include "backend/planner/abstract_plan.h"
#include "backend/storage/data_table.h"
#include "backend/index/index.h"

namespace peloton {
namespace planner{

class HybridScanPlan : public AbstractPlan{
public:
  HybridScanPlan(const HybridScanPlan &) = delete;
  HybridScanPlan &operator=(const HybridScanPlan &) = delete;
  HybridScanPlan(HybridScanPlan &&) = delete;
  HybridScanPlan &operator=(HybridScanPlan &&) = delete;


  HybridScanPlan(index::Index *index, storage::DataTable *table)
    : index_(index), table_(table) {}


  index::Index *GetDataIndex() {
    return this->index_;
  }

  storage::DataTable *GetDataTable() {
    return this->table_;
  }

private:
  index::Index *index_ = nullptr;

  storage::DataTable *table_ = nullptr;

};

}  // namespace planner
}  // namespace peloton