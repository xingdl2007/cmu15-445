/**
 * LRU implementation
 */
#include <cassert>

#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer():size_(0) {
  head_ = std::make_unique<node>();  // dummy node
  tail_ = head_.get();               // head & tail point to dummy node
}

template <typename T> LRUReplacer<T>::~LRUReplacer() = default;

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = table_.find(value);
  if (it == table_.end()) {
    auto ptr = std::make_unique<node>(value, tail_);
    tail_->next = std::move(ptr);
    tail_ = tail_->next.get();
    table_.emplace(value, tail_);
    ++size_;
  } else {
    node *pre = it->second->pre;
    std::unique_ptr<node> cur = std::move(pre->next);
    pre->next = std::move(cur->next);
    pre->next->pre = pre;

    // add to tail
    cur->pre = tail_;
    tail_->next = std::move(cur);
    tail_ = tail_->next.get();
  }
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (size_ == 0) {
    assert(head_.get() == tail_);
    return false;
  }

  value = head_->next->data;
  head_->next = std::move(head_->next->next);
  table_.erase(value);
  if (--size_ == 0) {
    tail_ = head_.get();   // reset tail
  }
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = table_.find(value);
  if (it != table_.end()) {
    node *pre = it->second->pre;
    std::unique_ptr<node> cur = std::move(pre->next);
    pre->next = std::move(cur->next);
    pre->next->pre = pre;

    table_.erase(value);
    if (--size_ == 0) {
      tail_ = head_.get();  // reset tail
    }
    return true;
  }
  return false;
}

template <typename T> size_t LRUReplacer<T>::Size() {
  std::lock_guard<std::mutex> lock(mutex_);
  return size_;
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
