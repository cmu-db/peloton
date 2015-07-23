/*
 * projection.cpp
 *
 */

#include "projection_info.h"

namespace peloton {
namespace executor {

/**
 * @brief Mainly release the expression in target list.
 */
ProjectionInfo::~ProjectionInfo(){
  for(auto target : target_list_){
    delete target.second;
  }
}


} /* namespace expression */
} /* namespace peloton */
