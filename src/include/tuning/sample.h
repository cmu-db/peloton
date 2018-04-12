//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sample.h
//
// Identification: src/include/tuning/sample.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/printable.h"
#include "common/printable.h"
#include "common/internal_types.h"

namespace peloton {
namespace tuning {

#define DEFAULT_SAMPLE_WEIGHT 1.0
#define DEFAULT_COLUMN_VALUE 0.5
#define DEFAULT_METRIC_VALUE 0

/**
 * @brief      Class for sample type.
 */
enum class SampleType {
  INVALID = 0,
  ACCESS = 1,  /**< accessed attributes */
  UPDATE = 2   /**< updated attributes */
};

//===--------------------------------------------------------------------===//
// Sample
//===--------------------------------------------------------------------===//

/**
 * @brief      Class for sample.
 */
class Sample : public Printable {
 public:
  Sample(const size_t column_count)
      : columns_accessed_(
            std::vector<double>(column_count, DEFAULT_COLUMN_VALUE)),
        weight_(DEFAULT_SAMPLE_WEIGHT),
        sample_type_(SampleType::ACCESS) {}

  /**
   * @brief      The constructor
   *
   * @param[in]  columns_accessed  The columns accessed
   * @param[in]  weight            The weight
   * @param[in]  sample_type       The sample type
   */
  Sample(const std::vector<double> &columns_accessed,
         double weight = DEFAULT_SAMPLE_WEIGHT,
         SampleType sample_type = SampleType::ACCESS)
      : columns_accessed_(columns_accessed),
        weight_(weight),
        sample_type_(sample_type) {}

  /**
   * get the distance from other sample
   *
   * @param[in]  other  The other
   *
   * @return     The distance.
   */
  double GetDistance(const Sample &other) const;

  /**
   * get difference after removing other sample
   *
   * @param[in]  other  The other
   *
   * @return     The difference.
   */
  Sample GetDifference(const Sample &other) const;

  /**
   * multiplication operator with a scalar
   *
   * @param[in]  rhs   The right hand side
   *
   * @return     { description_of_the_return_value }
   */
  Sample &operator*(const double &rhs);

  /**
   * addition operator with a sample
   *
   * @param[in]  rhs   The right hand side
   *
   * @return     { description_of_the_return_value }
   */
  Sample &operator+(const Sample &rhs);

  /**
   * Get the sample's weight
   *
   * @return     The weight.
   */
  inline double GetWeight() const { return (weight_); }

  /**
   * Get the sample type
   *
   * @return     The sample type.
   */
  inline SampleType GetSampleType() const { return (sample_type_); }

  /**
   * @brief      Gets the columns accessed.
   *
   * @return     The columns accessed.
   */
  inline const std::vector<double> GetColumnsAccessed() const {
    return (columns_accessed_);
  }

  /**
   * Set the columns accessed
   *
   * @param[in]  columns_accessed  The columns accessed
   */
  inline void SetColumnsAccessed(const std::vector<double> columns_accessed) {
    columns_accessed_ = columns_accessed;
  }

  /**
   * Get enabled columns
   *
   * @return     The enabled columns.
   */
  std::vector<oid_t> GetEnabledColumns() const;

  /**
   * Get a string representation for debugging
   *
   * @return     The information.
   */
  const std::string GetInfo() const;

  /**
   * Convert this sample into a parseable string
   *
   * @return     String representation of the object.
   */
  const std::string ToString() const;

  bool operator==(const Sample &other) const;

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

 private:
  /** column accessed bitmap */
  std::vector<double> columns_accessed_;

  /** weight of the sample */
  double weight_;

  /** type of sample */
  SampleType sample_type_;

};

}  // namespace indextuner
}  // namespace peloton

namespace std {

template <>
struct hash<peloton::tuning::Sample> {
  size_t operator()(const peloton::tuning::Sample &sample) const {
    // Compute individual hash values using XOR and bit shifting:
    long hash = 31;
    auto columns = sample.GetColumnsAccessed();
    auto sample_size = columns.size();
    for (size_t sample_itr = 0; sample_itr < sample_size; sample_itr++) {
      hash *= (columns[sample_itr] + 31);
    }

    return hash;
  }
};
}
