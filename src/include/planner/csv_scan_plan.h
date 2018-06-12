//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scan_plan.h
//
// Identification: src/include/planner/csv_scan_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "planner/abstract_scan_plan.h"
#include "planner/attribute_info.h"
#include "type/type_id.h"

namespace peloton {
namespace planner {

/**
 * This is the plan node when scanning a CSV file.
 */
class CSVScanPlan : public AbstractScan {
 public:
  struct ColumnInfo {
    std::string name;
    type::TypeId type;
  };

 public:
  /**
   * Constructs a sequential scan over a CSV file
   *
   * @param file_name The file path
   * @param cols Information of the columns expected in each row of the CSV
   * @param delimiter The character that separates columns within a row
   * @param quote The character used to quote data (i.e., strings)
   * @param escape The character that should appear before any data characters
   * that match the quote character.
   */
  CSVScanPlan(std::string file_name, std::vector<ColumnInfo> &&cols,
              char delimiter = ',', char quote = '"', char escape = '"',
              std::string null = "");

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  PlanNodeType GetPlanNodeType() const override;

  void GetOutputColumns(std::vector<oid_t> &columns) const override;

  const std::string &GetFileName() const { return file_name_; }

  void GetAttributes(std::vector<const AttributeInfo *> &ais) const override;

  char GetDelimiterChar() const { return delimiter_; }
  char GetQuoteChar() const { return quote_; }
  char GetEscapeChar() const { return escape_; }
  const std::string &GetNullString() const { return null_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utilities + Internal
  ///
  //////////////////////////////////////////////////////////////////////////////

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

  std::unique_ptr<AbstractPlan> Copy() const override;

  void PerformBinding(BindingContext &binding_context) override;

 private:
  const std::string file_name_;

  char delimiter_;
  char quote_;
  char escape_;
  const std::string null_;

  std::vector<planner::AttributeInfo> attributes_;
};

}  // namespace planner
}  // namespace peloton