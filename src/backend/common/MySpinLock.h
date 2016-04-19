#pragma once

#include <cstring>

#include <boost/smart_ptr/detail/spinlock.hpp>

struct MySpinLock{
  MySpinLock(){
    memset(&spinlock_, 0, sizeof(spinlock_));
  }

  inline void Lock(){
    spinlock_.lock();
  }

  inline void Unlock(){
    spinlock_.unlock();
  }

  inline bool IsLocked() const{
    return spinlock_.v_ == 1;
  }

private:
  boost::detail::spinlock spinlock_;
};