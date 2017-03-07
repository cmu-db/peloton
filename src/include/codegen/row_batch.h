//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// row_batch.h
//
// Identification: src/include/codegen/row_batch.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>

#include "codegen/codegen.h"
#include "codegen/loop.h"
#include "codegen/value.h"
#include "codegen/vector.h"
#include "planner/attribute_info.h"

namespace peloton {
namespace codegen {

class RowBatch {
 public:
  class AttributeAccess;
  class Row;
  class OutputTracker;
  class Iterator;

 private:
  typedef std::unordered_map<const planner::AttributeInfo *, AttributeAccess *>
      AttributeMap;

 public:
  //===--------------------------------------------------------------------===//
  // An accessor of an attribute in a row
  //===--------------------------------------------------------------------===//
  class AttributeAccess {
   public:
    // Destructor
    virtual ~AttributeAccess() {}

    // Access the value given the row
    virtual codegen::Value Access(CodeGen &codegen, Row &row) = 0;
  };

  //===--------------------------------------------------------------------===//
  // A row in this batch
  //===--------------------------------------------------------------------===//
  class Row {
   public:
    // Constructor
    Row(const RowBatch &batch, llvm::Value *batch_pos,
        const AttributeMap &attributes, OutputTracker *output_tracker);

    // Mark this row as valid or invalid in the output
    void SetValidity(CodeGen &codegen, llvm::Value *valid);

    // Get this row's position in the batch
    llvm::Value *GetBatchPosition() const { return batch_position_; }

    // Get the unique TID of this row
    llvm::Value *GetTID(CodeGen &codegen);

    bool HasAttribute(const planner::AttributeInfo *ai) const;

    // Derive the value of the given attribute from this row
    codegen::Value GetAttribute(CodeGen &codegen,
                                const planner::AttributeInfo *ai);

    // Register the temporary availability of an attribute in this row
    void RegisterAttributeValue(const planner::AttributeInfo *ai,
                                codegen::Value &val);

   private:
    // The batch this row belongs to
    const RowBatch &batch_;

    // The row's physical (backing) position
    llvm::Value *tid_;

    // The position of the row in the batch
    llvm::Value *batch_position_;

    // The attributes of this row
    const AttributeMap &accessors_;
    // A cache of the calculated/derived attributes
    std::unordered_map<const planner::AttributeInfo *, codegen::Value> cache_;

    // The class that tracks which slot in the output this row belongs to
    OutputTracker *output_tracker_;
  };

  //===--------------------------------------------------------------------===//
  // A handy class to
  //===--------------------------------------------------------------------===//
  class OutputTracker {
   public:
    // Constructor
    OutputTracker(const Vector &output, llvm::Value *target_pos);

    // Append the given row into the output. The second delta argument indicates
    // whether the output counter should be bumped up or not.
    void AppendRowToOutput(CodeGen &codegen, Row &row, llvm::Value *delta);

    // Get the final position (i.e., the number of output tuples were appended
    // to the output)
    llvm::Value *GetFinalOutputPos() const;

   private:
    // The vector that collects row TIDs
    const Vector &output_;

    // The first position in the vector that we write out to
    llvm::Value *target_pos_;

    // The final output position
    llvm::Value *final_pos_;
  };

  //===--------------------------------------------------------------------===//
  //  The callback used during scalar tuple-at-a-time iteration over the batch
  //===--------------------------------------------------------------------===//
  struct IterateCallback {
    // Destructor
    virtual ~IterateCallback() {}

    // The callback
    virtual void ProcessRow(RowBatch::Row &row) = 0;
  };

  //===--------------------------------------------------------------------===//
  // The callback used during vectorized iteration over the batch
  //===--------------------------------------------------------------------===//
  struct VectorizedIterateCallback {
    // Handy little struct to capture all iteration variables
    struct IterationInstance {
      llvm::Value *start = nullptr;
      llvm::Value *end = nullptr;
      llvm::Value *write_pos = nullptr;
    };

    // Destructor
    virtual ~VectorizedIterateCallback() {}

    // What the size of the vectors the callback wants?
    virtual uint32_t GetVectorSize() const = 0;

    // The callback
    virtual llvm::Value *ProcessRows(IterationInstance &iter_instance) = 0;
  };

 public:
  // Constructor
  RowBatch(llvm::Value *tid_start, llvm::Value *tid_end,
           Vector &selection_vector, bool filtered);

  // Add an attribute to batch
  void AddAttribute(const planner::AttributeInfo *ai, AttributeAccess *access);

  // Get the row at the given position
  Row GetRowAt(llvm::Value *batch_position,
               OutputTracker *output_tracker = nullptr) const;

  // Iterate over all the rows in the batch
  void Iterate(CodeGen &codegen, IterateCallback &cb);
  void Iterate(CodeGen &codegen, const std::function<void(RowBatch::Row &)> cb);

  // Iterate over all the rows in vectors of a given size
  void VectorizedIterate(CodeGen &codegen, VectorizedIterateCallback &cb);
  void VectorizedIterate(
      CodeGen &codegen, uint32_t vector_size,
      const std::function<llvm::Value *(
          RowBatch::VectorizedIterateCallback::IterationInstance &)> &cb);

  // Return only the number of valid rows in this batch
  llvm::Value *GetNumValidRows(CodeGen &codegen);

  // Return the total number of rows in this batch (valid _and_ invalid)
  llvm::Value *GetNumTotalRows(CodeGen &codegen);

  // Have the rows in this batch been filtered by the selection vector?
  bool IsFiltered() const { return filtered_; }

  // Return a const reference to the selection vector
  const Vector &GetSelectionVector() const { return selection_vector_; }

  void UpdateWritePosition(llvm::Value *sz);

 private:
  // Get all the attributes of a row in this batch
  const AttributeMap &GetAttributes() const { return attributes_; }

  // Get the physical TID of the given row
  llvm::Value *GetPhysicalPosition(CodeGen &codegen, const Row &row) const;

 private:
  // A batch either captures _all_ rows between two physical positions or all
  // rows whose positions are stored in the selection vector

  // The range of TIDs this batch covers
  llvm::Value *tid_start_;
  llvm::Value *tid_end_;

  // The total number of rows in this batch
  llvm::Value *num_rows_;

  // The selection vector used to filter rows in the batch
  Vector &selection_vector_;

  // The bool flag indicating whether th
  bool filtered_;

  // Attributes for all rows
  AttributeMap attributes_;

 private:
  // Don't copy or move
  DISALLOW_COPY_AND_MOVE(RowBatch);
};

}  // namespace codegen
}  // namespace peloton