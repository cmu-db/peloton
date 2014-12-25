#include "iostream"

extern "C" {
#include "postgres.h"
}

#include "access/heap_manager.h"

void cpp_fetcher(){
        elog(WARNING, "cpp_fetcher");
        std::cout<<"HEAP MANAGER"<<std::endl;
}
