/*
 * project_info.cpp
 *
 */

#include "../planner/project_info.h"

namespace peloton {
namespace planner {

/**
 * @brief Mainly release the expression in target list.
 */
ProjectInfo::~ProjectInfo(){
  for(auto target : target_list_){
    delete target.second;
  }
}


} /* namespace planner */
} /* namespace peloton */
