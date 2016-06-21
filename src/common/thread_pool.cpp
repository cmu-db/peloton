//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_pool.cpp
//
// Identification: src/common/thread_pool.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/macros.h"
#include "common/init.h"
#include "common/thread_pool.h"

namespace peloton {

ThreadPool::ThreadPool()
 : ThreadPool(std::max(std::thread::hardware_concurrency(), 2u) - 1u){
}

// the constructor just launches some amount of workers
ThreadPool::ThreadPool(size_t num_threads_) :
    stop(false),
    num_threads(num_threads_) {

  for(size_t i = 0; i <num_threads; ++i)
    workers.emplace_back(
        [this]
         {
    for(;;)
    {
      std::function<void()> task;

      {
        std::unique_lock<std::mutex> lock(this->queue_mutex);
        this->condition.wait(lock,
                             [this]{ return this->stop || !this->tasks.empty(); });
        if(this->stop && this->tasks.empty())
          return;
        task = std::move(this->tasks.front());
        this->tasks.pop();
      }

      task();
    }
         }
    );
}

// add new work item to the pool
template<class Func, class... Args>
auto ThreadPool::Enqueue(Func&& f, Args&&... args)
    -> std::future<typename std::result_of<Func(Args...)>::type>{
    using return_type = typename std::result_of<Func(Args...)>::type;

    auto task = std::make_shared< std::packaged_task<return_type()> >(
            std::bind(std::forward<Func>(f), std::forward<Args>(args)...)
        );

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        if(stop)
            throw std::runtime_error("enqueue on stopped ThreadPool");

        tasks.emplace([task](){ (*task)(); });
    }
    condition.notify_one();
    return res;
}

// the destructor joins all threads
ThreadPool::~ThreadPool() {

  {
    std::unique_lock<std::mutex> lock(queue_mutex);
    stop = true;
  }

  condition.notify_all();

  for(std::thread &worker: workers)
    worker.join();
}

}  // End peloton namespace
