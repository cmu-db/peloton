#pragma once

#include "type/value.h"
#include "type/type.h"
#include "common/internal_types.h"
#include "type/value_factory.h"
#include "expression/abstract_expression.h"
#include "common/macros.h"
#include "common/logger.h"
#include "count_min_sketch.h"

#include <cmath>
#include <cassert>
#include <cinttypes>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <stack>
#include <queue>
#include <vector>
#include <utility>

namespace peloton {
namespace optimizer {

/*
 * Providing Top K Elements retrieval functions
 * and bookkeeping of Top K Elements
 */
class TopKElements {
  // Peloton type compatible
  using ValueFrequencyPair = std::pair<type::Value, double>;

 public:
  /*
   * Class Declaration
   */
  class ApproxTopEntry;
  class ApproxTopEntryElem;
  template <class T, class Container, class Compare>
  class UpdatableQueue;
  class TopKQueue;

  /*
   * Class Definition
   */

  /*
   * The element of an ApproxTopEntry.
   * Wrapping a concrete data type like integer or string
   */
  class ApproxTopEntryElem {
   public:
    /*
     * types:
     * NON_TYPE: no type / not initialized
     * INT_TYPE: integer
     * STR_TYPE: string
     * DEC_TYPE: decimal
     *
     * store decimal values as strings here but maintaining the type info
     */
    enum class ElemType { NON_TYPE, INT_TYPE, STR_TYPE, DEC_TYPE };

    ElemType item_type;
    int64_t int_item;
    std::string str_item;  // encodes both string and decimal

    /*
     * Constructors
     */
    ApproxTopEntryElem()
        : item_type{ElemType::NON_TYPE}, int_item{-1}, str_item{""} {}

    ApproxTopEntryElem(int64_t intItem)
        : item_type{ElemType::INT_TYPE}, int_item{intItem}, str_item{""} {}

    ApproxTopEntryElem(const char* strItem,
                       ApproxTopEntryElem::ElemType type = ElemType::STR_TYPE)
        : item_type{type}, int_item{-1}, str_item{strItem} {}

    ApproxTopEntryElem(std::string strItem,
                       ApproxTopEntryElem::ElemType type = ElemType::STR_TYPE)
        : item_type{type}, int_item{-1}, str_item{strItem} {}

    /*
     * Overriding operator ==
     */
    bool operator==(const ApproxTopEntryElem& other) const {
      if (item_type == other.item_type) {
        switch (item_type) {
          case ElemType::INT_TYPE:
            return int_item == other.int_item;
          case ElemType::DEC_TYPE:
          case ElemType::STR_TYPE:
            if (str_item.compare(other.str_item) == 0) {
              return true;
            } else {
              return false;
            }
          default:
            return false;
        }
      }
      return false;
    }

  };  // end of class ApproxTopEntryElem

  /*
   * An entry of the TopKQueue datastructure
   * Working as a pair of element and count
   */
  class ApproxTopEntry {
   public:
    ApproxTopEntryElem approx_top_elem;
    int64_t approx_count;

    /*
     * Constructors
     */
    ApproxTopEntry() : approx_top_elem{}, approx_count{0} {}

    ApproxTopEntry(ApproxTopEntryElem elem, int64_t freq)
        : approx_top_elem{elem}, approx_count{freq} {}

    /*
     * Overriding operator ==
     */
    bool operator==(const ApproxTopEntry& other) const {
      // just check the elem, not the count
      return approx_top_elem == other.approx_top_elem;
    }

    /*
     * Format the entry for printing
     */
    std::string print() const {
      // char * ret ;
      std::string ret;
      switch (approx_top_elem.item_type) {
        case ApproxTopEntryElem::ElemType::INT_TYPE:
          ret += "{int_elem: ";
          ret += std::to_string(approx_top_elem.int_item);
          ret += " count: ";
          ret += std::to_string(approx_count);
          ret += "}";
          break;
        case ApproxTopEntryElem::ElemType::STR_TYPE:
          ret += "{str_elem: ";
          ret += approx_top_elem.str_item;
          ret += " count: ";
          ret += std::to_string(approx_count);
          ret += "}";
          break;
        default:

          break;
      }
      return ret;
    }

  };  // end of class ApproxTopEntry

  /*
   * Customized priority_queue
   * Containing functions with access to
   * the underlying container (for e.g. vector)
   */
  template <class T, class Container = std::vector<T>,
            class Compare = std::less<typename Container::value_type> >
  class UpdatableQueue : public std::priority_queue<T, Container, Compare> {
   public:
    typedef typename std::priority_queue<
        T, Container, Compare>::container_type::const_iterator const_iterator;

    /*
     * Check if a value exists in the queue
     */
    bool is_exist(const T& val) {
      auto first = this->c.cbegin();
      auto last = this->c.cend();
      while (first != last) {
        if (*first == val) {
          return true;
        }
        ++first;
      }
      return false;
    }

    /*
     * Remove a value from the queue
     */
    bool remove(const T& val) {
      // find the former entry first
      auto first = this->c.begin();
      auto last = this->c.end();
      auto idx_iter = last;

      while (first != last) {
        if (*first == val) {
          idx_iter = first;
        }
        ++first;
      }
      // if existing, remove it
      if (idx_iter != this->c.end()) {
        this->c.erase(idx_iter);
        std::make_heap(this->c.begin(), this->c.end(), this->comp);
        return true;
      } else {
        return false;
      }
    }

    /*
     * Retrieve all the items in the queue, unordered
     */
    std::vector<ApproxTopEntry> retrieve_all() const {
      std::vector<ApproxTopEntry> vec;
      const_iterator first = this->c.cbegin(), last = this->c.cend();
      while (first != last) {
        vec.push_back(*first);
        ++first;
      }
      return vec;
    }

    /*
     * Retrieve all the items in the queue, ordered, queue is maintained
     */
    std::vector<ApproxTopEntry> retrieve_all_ordered() {
      std::vector<ApproxTopEntry> vec;
      while (!this->empty()) {
        vec.push_back(this->top());
        this->pop();
      }
      auto first = this->c.begin(), last = this->c.end();
      while (first != last) {
        this->push(*first);
        ++first;
      }
      return vec;
    }

    /*
     * Retrieve all elements, ordered
     * Max first
     */
    std::vector<ApproxTopEntry> retrieve_all_ordered_max_first() {
      std::stack<ApproxTopEntry> stack;
      std::vector<ApproxTopEntry> vec_ret;
      while (!this->empty()) {
        stack.push(this->top());
        this->pop();
      }

      while (stack.size() != 0) {
        this->push(stack.top());
        vec_ret.push_back(stack.top());
        stack.pop();
      }
      return vec_ret;
    }

    /*
     * Retrieve given number of elements, ordered
     * Max first
     */
    std::vector<ApproxTopEntry> retrieve_ordered_max_first(int num) {
      std::stack<ApproxTopEntry> stack;
      std::vector<ApproxTopEntry> vec_ret;
      int i = 0;
      while (!this->empty()) {
        stack.push(this->top());
        this->pop();
      }

      while (stack.size() != 0) {
        this->push(stack.top());
        if (i < num) {
          ++i;
          vec_ret.push_back(stack.top());
        }
        stack.pop();
      }
      return vec_ret;
    }

    /*
     * Retrieve all elements, ordered
     * Max first
     * Return a vector of ValueFrequencyPair
     */
    std::vector<ValueFrequencyPair> get_all_ordered_max_first() {
      std::stack<ApproxTopEntry> stack;
      std::vector<ValueFrequencyPair> vec_ret;
      while (!this->empty()) {
        stack.push(this->top());
        this->pop();
      }

      while (stack.size() != 0) {
        ApproxTopEntry t = stack.top();
        this->push(t);
        type::Value val;
        switch (t.approx_top_elem.item_type) {
          case ApproxTopEntryElem::ElemType::INT_TYPE:
            val =
                type::ValueFactory::GetIntegerValue(t.approx_top_elem.int_item);
            break;
          case ApproxTopEntryElem::ElemType::STR_TYPE:
            val =
                type::ValueFactory::GetVarcharValue(t.approx_top_elem.str_item);
            break;
          case ApproxTopEntryElem::ElemType::DEC_TYPE:
            val = type::ValueFactory::GetDecimalValue(
                std::stod(t.approx_top_elem.str_item));
            break;
          default:
            break;
        }
        vec_ret.push_back(std::make_pair(val, (double)t.approx_count));
        stack.pop();
      }
      return vec_ret;
    }

    /*
     * Retrieve given number of elements, ordered
     * Max first
     * Return a vector of ValueFrequencyPair
     */
    std::vector<ValueFrequencyPair> get_ordered_max_first(int num) {
      std::stack<ApproxTopEntry> stack;
      std::vector<ValueFrequencyPair> vec_ret;
      int i = 0;
      while (!this->empty()) {
        stack.push(this->top());
        this->pop();
      }

      while (stack.size() != 0) {
        ApproxTopEntry t = stack.top();
        this->push(t);
        if (i < num) {
          ++i;
          type::Value val;
          switch (t.approx_top_elem.item_type) {
            case ApproxTopEntryElem::ElemType::INT_TYPE:
              val = type::ValueFactory::GetIntegerValue(
                  t.approx_top_elem.int_item);
              break;
            case ApproxTopEntryElem::ElemType::STR_TYPE:
              val = type::ValueFactory::GetVarcharValue(
                  t.approx_top_elem.str_item);
              break;
            case ApproxTopEntryElem::ElemType::DEC_TYPE:
              val = type::ValueFactory::GetDecimalValue(
                  std::stod(t.approx_top_elem.str_item));
              break;
            default:
              break;
          }
          vec_ret.push_back(std::make_pair(val, (double)t.approx_count));
        }
        stack.pop();
      }
      return vec_ret;
    }

  };  // end of UpdatableQueue

  /*
   * TopKQueue
   *    Wrapping a customized priotity queue
   *    for the top k approximate entries
   */
  class TopKQueue {
   public:
    // struct for comparison in the queue
    // lowest count comes in the top (MinHeap)
    struct compare {
      bool operator()(const ApproxTopEntry& l, const ApproxTopEntry& r) {
        return l.approx_count > r.approx_count;
      }
    };

    /*
     * Constructor
     */
    TopKQueue(int param_k)
        : k{param_k},

          size{0} {}

    int get_k() { return this->k; }

    int get_size() { return this->size; }

    inline void incr_size() { size++; }

    inline void decr_size() { size--; }

    /*
     * Check if the entry exists in the queue
     */
    bool is_exist(ApproxTopEntry entry) { return queue.is_exist(entry); }

    /*
     * push an entry onto the queue
     */
    void push(ApproxTopEntry entry) {
      // here we suppose the is_exist returns false
      // if less than K-items, just insert, increase the size
      if (size < k) {
        queue.push(entry);
        incr_size();
      }
      // if we have more than K-items (including K),
      // remove the item with the lowest frequency from our data structure
      else {
        // if and only if the lowest frequency is lower than the new one
        if (queue.top().approx_count < entry.approx_count) {
          queue.pop();
          queue.push(entry);
        }
      }
    }

    /*
     * Update a designated entry
     */
    void update(ApproxTopEntry entry) {
      // remove the former one (with the same entry elem) and insert the new
      // entry
      // first remove the former entry
      if (queue.remove(entry)) {
        queue.push(entry);
      }
    }

    /*
     * Remove a designated entry
     */
    bool remove(ApproxTopEntry entry) {
      decr_size();
      return queue.remove(entry);
    }

    /*
     * Pop the top element of the queue
     */
    ApproxTopEntry pop() {
      decr_size();
      ApproxTopEntry e = std::move(queue.top());
      queue.pop();
      return e;
    }

    /*
     * Retrieve all the items in the queue, unordered
     */
    std::vector<ApproxTopEntry> retrieve_all() const {
      return queue.retrieve_all();
    }

    /*
     * Retrieve all the items in the queue, ordered, queue is maintained
     * Min first (smallest count first)
     */
    std::vector<ApproxTopEntry> retrieve_all_ordered() {
      return queue.retrieve_all_ordered();
    }

    /*
     * Retrieve all elements, ordered
     * Max first
     */
    std::vector<ApproxTopEntry> retrieve_all_ordered_max_first() {
      return queue.retrieve_all_ordered_max_first();
    }

    /*
     * Retrieve given number of elements, ordered
     * Max first
     */
    std::vector<ApproxTopEntry> retrieve_ordered_max_first(int num) {
      return queue.retrieve_ordered_max_first(num);
    }

    /*
     * Retrieve all elements, ordered
     * Max first
     * Peloton Compatible
     */
    std::vector<ValueFrequencyPair> get_all_ordered_max_first() {
      return queue.get_all_ordered_max_first();
    }

    /*
     * Retrieve given number of elements, ordered
     * Max first
     * Peloton Compatible
     */
    std::vector<ValueFrequencyPair> get_ordered_max_first(int num) {
      return queue.get_ordered_max_first(num);
    }

   private:
    // the K as in "top K"
    int k;
    // the priority queue managing the top entries
    UpdatableQueue<ApproxTopEntry, std::vector<ApproxTopEntry>, compare> queue;
    // number of entries in the priority queue
    int size;

  };  // end of class TopKQueue

  // TopKElements members
  TopKQueue tkq;
  CountMinSketch cmsketch;

  /*
   * TopKElements Constructor
   */
  TopKElements(CountMinSketch& sketch, int k) : tkq{k}, cmsketch{sketch} {}

  /*
   * Add an item into this bookkeeping datastructure as well as
   * the sketch
   */
  void Add(int64_t item, int count = 1) {
    // Increment the count for this item in the Count-Min sketch
    cmsketch.Add(item, count);

    // Estimate the frequency of this item using the sketch
    // Add it to the queue
    ApproxTopEntryElem elem{item};
    ApproxTopEntry e(elem, cmsketch.EstimateItemCount(item));
    AddFreqItem(e);
  }

  void Add(const char* item, int count = 1,
           ApproxTopEntryElem::ElemType type =
               ApproxTopEntryElem::ElemType::STR_TYPE) {
    // Increment the count for this item in the Count-Min sketch
    cmsketch.Add(item, count);

    // Estimate the frequency of this item using the sketch
    // Add it to the queue
    ApproxTopEntryElem elem{std::string(item), type};
    ApproxTopEntry e(elem, cmsketch.EstimateItemCount(item));
    AddFreqItem(e);
  }

  void Add(std::string item, int count = 1,
           ApproxTopEntryElem::ElemType type =
               ApproxTopEntryElem::ElemType::STR_TYPE) {
    // Increment the count for this item in the Count-Min sketch
    cmsketch.Add(item.c_str(), count);

    // Estimate the frequency of this item using the sketch
    // Add it to the queue
    ApproxTopEntryElem elem{item, type};
    ApproxTopEntry e(elem, cmsketch.EstimateItemCount(item.c_str()));
    AddFreqItem(e);
  }

  /*
   * Peloton type compatible / adaptor
   */
  void Add(const type::Value& value) {
    switch (value.GetTypeId()) {
      case type::TypeId::TINYINT:
        int8_t n_8;
        n_8 = value.GetAs<int8_t>();
        Add((int64_t)n_8, 1);
        break;
      case type::TypeId::SMALLINT:
        int32_t n_16;
        n_16 = value.GetAs<int16_t>();
        Add((int64_t)n_16, 1);
        break;
      case type::TypeId::INTEGER:
      case type::TypeId::PARAMETER_OFFSET:
      case type::TypeId::TIMESTAMP:
        int32_t n_32;
        n_32 = value.GetAs<int32_t>();
        Add((int64_t)n_32, 1);
        break;
      case type::TypeId::BIGINT:
        int64_t n_64;
        n_64 = value.GetAs<int64_t>();
        Add(n_64, 1);
        break;
      case type::TypeId::DECIMAL: {
        std::string sd = value.ToString();
        Add(sd, 1, ApproxTopEntryElem::ElemType::DEC_TYPE);
        break;
      }
      case type::TypeId::VARCHAR:
      default: {
        // valgrind reports error on value.ToString().c_str();
        std::string sv = value.ToString();
        Add(sv, 1);
        break;
      }
    }
  }

  // TODO:
  // Need to retrieve new elements after eviction of current element(s)

  // UPDATE:
  // This this TOP K ELEMENTS function is used normally in a full scan
  // to collect stats, the removal is unnecessary in this context
  /*
   * Remove an item from the TopKQueue (as well as the sketch)
   */
  void Remove(int64_t item, int count = 1) {
    cmsketch.Remove(item, count);
    ApproxTopEntryElem elem{item};
    ApproxTopEntry e(elem, cmsketch.EstimateItemCount(item));
    DecrFreqItem(e);
  }

  void Remove(const char* item, int count = 1) {
    cmsketch.Remove(item, count);
    ApproxTopEntryElem elem{std::string(item)};
    ApproxTopEntry e(elem, cmsketch.EstimateItemCount(item));
    DecrFreqItem(e);
  }

  void Remove(std::string item, int count = 1) {
    cmsketch.Remove(item.c_str(), count);
    ApproxTopEntryElem elem{item};
    ApproxTopEntry e(elem, cmsketch.EstimateItemCount(item.c_str()));
    DecrFreqItem(e);
  }

  /*
   * Top K Elements Retrieval Functions
   */

  /*
   * Retrieve all the items in the queue, unordered
   */
  std::vector<ApproxTopEntry> RetrieveAll() { return tkq.retrieve_all(); }

  /*
   * Retrieve all the items in the queue, ordered, queue is maintained
   * small count to large count (min first)
   */
  std::vector<ApproxTopEntry> RetrieveAllOrdered() {
    return tkq.retrieve_all_ordered();
  }

  /*
   * Retrieve all the items in the queue, ordered, queue is maintained
   * large count to small count (max first)
   */
  std::vector<ApproxTopEntry> RetrieveAllOrderedMaxFirst() {
    return tkq.retrieve_all_ordered_max_first();
  }

  /*
   * Retrieve given number of elements, ordered
   * Max first
   */
  std::vector<ApproxTopEntry> RetrieveOrderedMaxFirst(int num) {
    if (num >= tkq.get_k()) return tkq.retrieve_all_ordered_max_first();
    return tkq.retrieve_ordered_max_first(num);
  }

  /*
   * Retrieve all the items in the queue, ordered, queue is maintained
   * large count to small count (max first)
   * Peloton Type Compatible
   */
  std::vector<ValueFrequencyPair> GetAllOrderedMaxFirst() {
    return tkq.get_all_ordered_max_first();
  }

  /*
   * Retrieve given number of elements, ordered
   * Max first
   * Peloton Type Compatible
   */
  std::vector<ValueFrequencyPair> GetOrderedMaxFirst(int num) {
    if (num >= tkq.get_k()) return tkq.get_all_ordered_max_first();
    return tkq.get_ordered_max_first(num);
  }

  /*
   * Printing Functions
   */

  /*
   * Print the entire queue, unordered
   */
  void PrintTopKQueue() const {
    std::vector<ApproxTopEntry> vec = tkq.retrieve_all();
    for (auto const& e : vec) {
      LOG_INFO("\n [PrintTopKQueue Entries] %s", e.print().c_str());
    }
  }

  /*
   * Print the entire queue, ordered
   * Min count first
   * Queue is empty afterwards
   */
  void PrintTopKQueuePops() {
    while (tkq.get_size() != 0) {
      LOG_INFO("\n [PrintTopKQueuePops Entries] %s", tkq.pop().print().c_str());
    }
  }

  /*
   * Print the entire queue, ordered
   * Min count first
   * Queue is intact
   */
  void PrintTopKQueueOrdered() {
    std::vector<ApproxTopEntry> vec = tkq.retrieve_all_ordered();
    for (auto const& e : vec) {
      LOG_INFO("\n [Print k Entries] %s", e.print().c_str());
    }
  }

  /*
   * Print the entire queue, ordered
   * Max count first
   * Queue is intact
   */
  void PrintTopKQueueOrderedMaxFirst() {
    std::vector<ApproxTopEntry> vec = tkq.retrieve_all_ordered_max_first();
    for (auto const& e : vec) {
      LOG_INFO("\n [Print k Entries MaxFirst] %s", e.print().c_str());
    }
  }

  /*
   * Print a given number of elements in the top k queue, ordered
   * Max count first
   * Queue is intact
   * Retriving a vector of ValueFrequencyPair
   */
  void PrintTopKQueueOrderedMaxFirst(int num) {
    std::vector<ApproxTopEntry> vec = tkq.retrieve_ordered_max_first(num);
    for (auto const& e : vec) {
      LOG_INFO("\n [Print top %d Entries MaxFirst] %s", num, e.print().c_str());
    }
  }

  /*
   * Print the entire queue, ordered
   * Max count first
   * Queue is intact
   */
  void PrintAllOrderedMaxFirst() {
    std::vector<ValueFrequencyPair> vec = GetAllOrderedMaxFirst();
    for (auto const& p : vec) {
      LOG_INFO("\n [Print k Values MaxFirst] %s, %f", p.first.GetInfo().c_str(),
               p.second);
    }
  }

  /*
   * Print a given number of elements in the top k queue, ordered
   * Max count first
   * Queue is intact
   * Retriving a vector of ValueFrequencyPair
   */
  void PrintOrderedMaxFirst(int num) {
    std::vector<ValueFrequencyPair> vec = GetOrderedMaxFirst(num);
    for (auto const& p : vec) {
      LOG_INFO("\n [Print %d Values MaxFirst] %s, %f", num,
               p.first.GetInfo().c_str(), p.second);
    }
  }

 private:
  /*
   * Forge an ApproxTopEntry of integer
   */
  ApproxTopEntry MakeApproxTopEntry(uint64_t freq, int64_t item) {
    ApproxTopEntryElem elem{item};
    return ApproxTopEntry(elem, freq);
  }

  /*
   * Forge an ApproxTopEntry of string
   */
  ApproxTopEntry MakeApproxTopEntry(uint64_t freq, std::string item) {
    ApproxTopEntryElem elem{item};
    return ApproxTopEntry(elem, freq);
  }

  /*
   * Add the frequency (approx count) and item (Element) pair (ApproxTopEntry)
   * to the queue / update tkq structure
   */
  void AddFreqItem(ApproxTopEntry& entry) {
    // If we have more than K-items, remove the item with the lowest frequency
    // from our data structure
    // If freq_item was already in our data structure, just update it instead.

    if (!tkq.is_exist(entry)) {
      // not in the structure
      // insert it
      tkq.push(entry);
    } else {
      // if in the structure
      // update
      tkq.update(entry);
    }
  }

  /*
   * Decrease / Remove
   */
  void DecrFreqItem(ApproxTopEntry& entry) {
    if (!tkq.is_exist(entry)) {
      // not in the structure
      // do nothing
    } else {
      // if in the structure
      // update
      if (entry.approx_count == 0) {
        tkq.remove(entry);
      } else {
        tkq.update(entry);
      }
    }
  }

};  // end of class TopKElements

} /* namespace optimizer */
} /* namespace peloton */
