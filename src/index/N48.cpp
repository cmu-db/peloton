//
// Created by Min Huang on 9/20/17.
//

#include <assert.h>
#include <algorithm>
#include "index/N.h"
#include "index/N4.h"
#include "index/N16.h"
#include "index/N48.h"
#include "index/N256.h"

namespace peloton {
namespace index {
bool N48::isFull() const {
  return count == 48;
}

bool N48::isUnderfull() const {
  return count == 12;
}

void N48::insert(uint8_t key, N *n) {
  unsigned pos = count;
  if (children[pos]) {
    for (pos = 0; children[pos] != nullptr; pos++);
  }
  children[pos] = n;
  childIndex[key] = (uint8_t) pos;
  count++;
}

bool N48::change(uint8_t key, N *val) {
  children[childIndex[key]] = val;
  return true;
}

N *N48::getChild(const uint8_t k) const {
  if (childIndex[k] == emptyMarker) {
    return nullptr;
  } else {
    return children[childIndex[k]];
  }
}

void N48::remove(uint8_t k) {
  assert(childIndex[k] != emptyMarker);
  children[childIndex[k]] = nullptr;
  childIndex[k] = emptyMarker;
  count--;
  assert(getChild(k) == nullptr);
}

N *N48::getAnyChild() const {
  N *anyChild = nullptr;
  for (unsigned i = 0; i < 256; i++) {
    if (childIndex[i] != emptyMarker) {
      if (N::isLeaf(children[childIndex[i]])) {
        return children[childIndex[i]];
      } else {
        anyChild = children[childIndex[i]];
      };
    }
  }
  return anyChild;
}

void N48::deleteChildren() {
  for (unsigned i = 0; i < 256; i++) {
    if (childIndex[i] != emptyMarker) {
      N::deleteChildren(children[childIndex[i]]);
      N::deleteNode(children[childIndex[i]]);
    }
  }
}

uint64_t N48::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                          uint32_t &childrenCount) const {
  restart:
  bool needRestart = false;
  uint64_t v;
  v = readLockOrRestart(needRestart);
  if (needRestart) goto restart;
  childrenCount = 0;
  for (unsigned i = start; i <= end; i++) {
    if (this->childIndex[i] != emptyMarker) {
      children[childrenCount] = std::make_tuple(i, this->children[this->childIndex[i]]);
      childrenCount++;
    }
  }
  readUnlockOrRestart(v, needRestart);
  if (needRestart) goto restart;
  return v;
}

}
}