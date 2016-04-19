#pragma once

#include <cassert>
#include "backend/common/MySpinLock.h"

class MyRWLock {
  enum LockType : size_t{ NO_LOCK, READ_LOCK, WRITE_LOCK };
public:
  MyRWLock() : lock_type_(0), reader_count_(0) {}

  void AcquireReadLock() {
    while (1) {
      while (lock_type_ == WRITE_LOCK);
      spinlock_.Lock();
      if (lock_type_ == WRITE_LOCK) {
        spinlock_.Unlock();
      }
      else {
        if (lock_type_ == NO_LOCK) {
          lock_type_ = READ_LOCK;
          ++reader_count_;
        }
        else {
          assert(lock_type_ == READ_LOCK);
          ++reader_count_;
        }
        spinlock_.Unlock();
        return;
      }
    }
  }

  bool TryReadLock() {
    bool rt = false;
    spinlock_.Lock();
    if (lock_type_ == NO_LOCK) {
      lock_type_ = READ_LOCK;
      ++reader_count_;
      rt = true;
    }
    else if (lock_type_ == READ_LOCK) {
      ++reader_count_;
      rt = true;
    }
    else {
      rt = false;
    }
    spinlock_.Unlock();
    return rt;
  }

  void AcquireWriteLock() {
    while (1) {
      while (lock_type_ != NO_LOCK);
      spinlock_.Lock();
      if (lock_type_ != NO_LOCK) {
        spinlock_.Unlock();
      }
      else {
        assert(lock_type_ == NO_LOCK);
        lock_type_ = WRITE_LOCK;
        spinlock_.Unlock();
        return;
      }
    }
  }

  bool TryWriteLock() {
    bool rt = false;
    spinlock_.Lock();
    if (lock_type_ == NO_LOCK) {
      lock_type_ = WRITE_LOCK;
      rt = true;
    }
    else {
      rt = false;
    }
    spinlock_.Unlock();
    return rt;
  }

  void ReleaseReadLock() {
    spinlock_.Lock();
    --reader_count_;
    if (reader_count_ == 0) {
      lock_type_ = NO_LOCK;
    }
    spinlock_.Unlock();
  }

  void ReleaseWriteLock() {
    spinlock_.Lock();
    lock_type_ = NO_LOCK;
    spinlock_.Unlock();
  }

  bool ExistsWriteLock() const {
    return (lock_type_ == WRITE_LOCK);
  }

private:
  MySpinLock spinlock_;
  volatile size_t lock_type_;
  volatile size_t reader_count_;
};
