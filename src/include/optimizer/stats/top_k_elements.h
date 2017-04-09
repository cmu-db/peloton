#pragma once

#include "common/logger.h"
#include "count_min_sketch.h"

#include <cmath>
#include <vector>
#include <cassert>
#include <cstdlib>
#include <cinttypes>
#include <queue>
#include <vector>
#include <cstring>
#include <string>
#include <cstdio>

namespace peloton{
namespace optimizer{

class TopKElements {
 public:
  /*
   * class declaration
   */
  template<class T, class Container, class Compare> class UpdatableQueue;
  class ApproxTopEntry;
  class ApproxTopEntryElem;
  class TopKQueue;

  /*
   * class definition
   */

  template<
    class T,
    class Container = std::vector<T>,
    class Compare = std::less<typename Container::value_type>
  > class UpdatableQueue : public std::priority_queue<T, Container, Compare> {
  public:
    typedef typename
        std::priority_queue<
        T,
        Container,
        Compare>::container_type::const_iterator const_iterator;

    const_iterator find_const(const T&val) const {
      const_iterator first = this->c.cbegin();
      const_iterator last = this->c.cend();
      while (first!=last) {
          if (*first==val) return first;
          ++first;
      }
      return last;
    }

    bool is_exist(const T&val) {
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

    bool remove(const T&val) {
      
      // find the former entry first
      auto first = this->c.begin();
      auto last = this->c.end();
      auto idx_iter = last;

      while (first!=last) {
        if (*first==val) { 
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

    std::vector<ApproxTopEntry> retrieve_all() const{
      std::vector<ApproxTopEntry> vec;
      const_iterator first = this->c.cbegin();
      const_iterator last = this->c.cend();
      while (first!=last) {
        vec.push_back(*first);
        ++first;
      }
      return std::move(vec);
    }

  };

  class ApproxTopEntryElem {
  public:

    /* 
     * types:
     * NON_TYPE: no type / not initialized
     * INT_TYPE: int
     * STR_TYPE: str
     */

    enum class ElemType {
      NON_TYPE,
      INT_TYPE,
      STR_TYPE
    };
    
    ElemType item_type;
    int64_t int_item;
    const char* str_item;

    ApproxTopEntryElem() : 
    item_type{ElemType::NON_TYPE},
    int_item{-1},
    str_item{nullptr}
    {}
    
    ApproxTopEntryElem(int64_t intItem) :
    item_type{ElemType::INT_TYPE},
    int_item{intItem},
    str_item{nullptr}
    {}

    ApproxTopEntryElem(const char* strItem) :
    item_type{ElemType::STR_TYPE},
    int_item{-1},
    str_item{strItem}
    {}
    
    /*
    ApproxTopEntryElem(int64_t intItem) :
    int_item{intItem},
    str_item{nullptr}
    {}

    ApproxTopEntryElem(int64_t intItem = -1, const char* strItem) :
    int_item{intItem},
    str_item{strItem}
    {}
    */

    bool operator==(const ApproxTopEntryElem &other) const {
      if (item_type == other.item_type) {
        switch (item_type) {
          case ElemType::INT_TYPE:
            return int_item == other.int_item;
          case ElemType::STR_TYPE:
            if (strcmp(str_item, other.str_item) == 0) {
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
    
  }; // end of class ApproxTopEntryElem


  class ApproxTopEntry {
  public:
    ApproxTopEntryElem approx_top_elem;
    int64_t approx_count;

    ApproxTopEntry() : 
    approx_top_elem{},
    approx_count{0}
    {}

    ApproxTopEntry(ApproxTopEntryElem elem, int64_t freq) : 
    approx_top_elem{elem},
    approx_count{freq}
    {}

    bool operator==(const ApproxTopEntry &other) const {
      // just check the elem, not the count
      return approx_top_elem == other.approx_top_elem;
    }

    std::string print() const {
      //char * ret ;
      std::string ret;
      switch (approx_top_elem.item_type) {
        case ApproxTopEntryElem::ElemType::INT_TYPE:
          //sprintf(ret, "{int_elem: %ld, count: %ld}", approx_top_elem.int_item,  approx_count);
          ret += "{int_elem: ";
          ret += std::to_string(approx_top_elem.int_item); 
          ret +=" count: "; 
          ret += std::to_string(approx_count);
          ret += "}";
          break;
        case ApproxTopEntryElem::ElemType::STR_TYPE:
          //sprintf(ret, "{str_elem: %s, count: %ld}", approx_top_elem.str_item, approx_count);
          ret += "{str_elem: ";
          ret += approx_top_elem.str_item; 
          ret +=" count: "; 
          ret += std::to_string(approx_count);
          ret += "}";
          break;
        default:
          
          break;
      }
      return ret;
    }

  }; // end of class ApproxTopEntry

  /*
   * TopKQueue
   *    a priotity queue for the top k approx entries
   */
  class TopKQueue {
   public:

    // struct for comparison in the queue
    // lowest count comes in the top (MinHeap)
    struct compare
    {
      bool operator()(const ApproxTopEntry& l, const ApproxTopEntry& r)
      {
          return l.approx_count > r.approx_count;
      }
    };

    // countstructor
    TopKQueue(int param_k) :
    k{param_k},
    //lowest_count{0},
    size{0}
    {}

    inline void incr_size() {
      size++;
    }

    inline void decr_size() {
      size--;
    }

    bool is_exist(ApproxTopEntry entry) {
      return queue.is_exist(entry);
    }

    // void update_lowest_count() {
    //   lowest_count = queue.top().approx_count();
    // }

    void push(ApproxTopEntry entry) {
      // here we suppose the is_exist returns false
      // if less than K-items, just insert, increase the size
      if (size < k) {
        queue.push(entry);
        incr_size();
        // update_lowest_count();
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

    void update(ApproxTopEntry entry) {
      // remove the former one (with the same entry elem) and insert the new entry
      // first remove the former entry
      if (queue.remove(entry)) { 
        queue.push(entry);
      }
    }

    bool remove(ApproxTopEntry entry) {
      decr_size();
      return queue.remove(entry);
    }

    std::vector<ApproxTopEntry> retrieve_all() const{
      return queue.retrieve_all();
    }

   private:
    // members
    int k;
    //ApproxTopEntry **entries; // array of ApproxTopEntry pointers
    // maintain a MinHeap (lowest count on top)
    //auto cmp = [](ApproxTopEntry left, ApproxTopEntry right) { 
    //  return left.approx_count > right.approx_count;
    //};
    //std::priority_queue<ApproxTopEntry, std::vector<ApproxTopEntry>, decltype(cmp)> queue; 
    UpdatableQueue<ApproxTopEntry, std::vector<ApproxTopEntry>, compare> queue; 
    //int lowest_count;
    int size;
    
  }; // end of class TopKQueue

  // TopKElements members
  TopKQueue tkq;
  CountMinSketch cmsketch;

  // TopKElements constructor
  TopKElements(CountMinSketch sketch, int k) :
    tkq{k},
    cmsketch{sketch}
  {}

  
  void Add(int64_t item, int count = 1) {
    // Increment the count for this item in the Count-Min sketch
    cmsketch.Add(item, count);

    // Estimate the frequency of this item using the sketch
    // Add it to the queue
    ApproxTopEntryElem elem{item};
    AddFreqItem(ApproxTopEntry(elem, cmsketch.EstimateItemCount(item)));
    //AddFreqItem(MakeApproxTopEntry(EstimateItemCount(item), item));
  }

  void Add(const char* item, int count = 1) {
    // Increment the count for this item in the Count-Min sketch
    cmsketch.Add(item, count);

    // Estimate the frequency of this item using the sketch
    // Add it to the queue
    ApproxTopEntryElem elem{item};
    AddFreqItem(ApproxTopEntry(elem, cmsketch.EstimateItemCount(item)));
    //AddFreqItem(MakeApproxTopEntry(EstimateItemCount(item), item));
  }

  // TODO:
  // Need to retrieve new elements after eviction of current element(s)
  void Remove(int64_t item, int count = 1) {
    cmsketch.Remove(item, count);
    ApproxTopEntryElem elem{item};
    DecrFreqItem(ApproxTopEntry(elem, cmsketch.EstimateItemCount(item)));
  }

  void Remove(const char* item, int count = 1) {
    cmsketch.Remove(item, count);
    ApproxTopEntryElem elem{item};
    DecrFreqItem(ApproxTopEntry(elem, cmsketch.EstimateItemCount(item)));
  }

  std::vector<ApproxTopEntry> RetrieveAll() {
    return tkq.retrieve_all();
  }

  void PrintTopKQueue() const{
    std::vector<ApproxTopEntry> vec = tkq.retrieve_all();
    //for (std::vector<ApproxTopEntry>::iterator iter = vec.cbegin(); iter != vec.cend(); ++iter) {
    //  LOG_INFO("\n [PrintTopKQueue Entries] %s", (*iter).print());
    //}
    for (auto const& e : vec) {
      LOG_INFO("\n [PrintTopKQueue Entries] %s", e.print().c_str());
    }
  }

 private:
  ApproxTopEntry MakeApproxTopEntry(uint64_t freq, int64_t item) {
    ApproxTopEntryElem elem{item};
    return std::move(ApproxTopEntry(elem, freq));
  }

  ApproxTopEntry MakeApproxTopEntry(uint64_t freq, const char* item) {
    ApproxTopEntryElem elem{item};
    return std::move(ApproxTopEntry(elem, freq));
  }

  // Add the frequency (approx count) and item (Element) pair (ApproxTopEntry) 
  // to the queue / update tkq structure
  void AddFreqItem(ApproxTopEntry entry) {
    // If we have more than K-items, remove the item with the lowest frequency from our data structure
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

  void DecrFreqItem(ApproxTopEntry entry) {
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


}; // end of class TopKElements

} /* namespace optimizer */
} /* namespace peloton */
