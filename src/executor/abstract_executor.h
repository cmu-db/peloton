/**
 * @brief Header file for abstract executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <vector>

namespace nstore {
namespace executor {
class LogicalTile;

class AbstractExecutor {
 public:
  bool Init();

  LogicalTile *GetNextTile();

  void CleanUp();

 protected:
  void AddChild(AbstractExecutor *child);

  /** @brief Init function to be overriden by subclass. */
  virtual bool SubInit() = 0;

  /** @brief Workhorse function to be overriden by subclass. */
  virtual LogicalTile *SubGetNextTile() = 0;

  /** @brief Clean up function to be overriden by subclass. */
  virtual void SubCleanUp() = 0;

  /** @brief Children nodes of this executor in the executor tree. */
  std::vector<AbstractExecutor *> children_;
};

} // namespace executor
} // namespace nstore
