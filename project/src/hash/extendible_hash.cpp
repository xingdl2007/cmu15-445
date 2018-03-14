#include <cassert>
#include <functional>
#include <list>
#include <bitset>
#include <iostream>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size):
    bucket_size_(size), bucket_count_(0), depth(0) {
  buckets_.emplace_back(new Bucket(0, 0));
  // initial: 1 bucket
  bucket_count_ = 1;
}

/*
 * helper function to calculate the hashing address of input key
 * std::hash<>: assumption already has specialization for type K
 * namespace std have standard specializations for basic types.
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return std::hash<K>()(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return depth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  assert(0 <= bucket_id && bucket_id < static_cast<int>(buckets_.size()));
  if (buckets_[bucket_id]) {
    return buckets_[bucket_id]->depth;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return bucket_count_;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  size_t index = bucketIndex(key);
  if (buckets_[index]) {
    auto bucket = buckets_[index];
    if (bucket->items.find(key) != bucket->items.end()) {
      value = bucket->items[key];
      return true;
    }
    // search overflow bucket if has
    while (bucket->next) {
      auto next = bucket->next;
      if (next->items.find(key) != next->items.end()) {
        value = next->items[key];
        return true;
      }
      bucket = next;
    }
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  size_t cnt = 0;
  size_t index = bucketIndex(key);
  if (buckets_[index]) {
    auto bucket = buckets_[index];
    cnt += bucket->items.erase(key);

    // search overflow bucket if has
    while (bucket->next) {
      auto next = bucket->next;
      cnt += next->items.erase(key);
      bucket = next;
    }
  }
  return cnt != 0;
}

/*
 * helper function to split a bucket when is full, overflow if necessary
 */
template <typename K, typename V>
std::unique_ptr<typename ExtendibleHash<K, V>::Bucket>
ExtendibleHash<K, V>::split(std::shared_ptr<Bucket> &b) {
  auto res = std::make_unique<Bucket>(0, b->depth);
  while (res->items.empty()) {
    ++b->depth;
    ++res->depth;
    for (auto it = b->items.begin(); it != b->items.end();) {
      if (HashKey(it->first) & (1 << (b->depth - 1))) {
        res->items.insert(*it);
        res->id = HashKey(it->first) & ((1 << b->depth) - 1);
        it = b->items.erase(it);
      } else {
        ++it;
      }
    }
    if (b->items.empty()) {
      b->items.swap(res->items);

      // update id;
      b->id = res->id;
    }
    // which all keys in current bucket have same hash
    if (b->depth == sizeof(size_t)) {
      break;
    }
  }
  // maintain bucket count current in use
  ++bucket_count_;

  // overflow condition, should be a rare case
  if (b->depth == sizeof(size_t)) {
    // last one
    while (b->next) {
      b = b->next;
    }
    b->next = std::move(res);
    return nullptr;
  }
  return res;
}

/*
 * helper function to find bucket index
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::bucketIndex(const K &key) {
  return HashKey(key) & ((1 << depth) - 1);
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  size_t bucket_id = bucketIndex(key);

  if (buckets_[bucket_id] == nullptr) {
    buckets_[bucket_id] = std::make_shared<Bucket>(bucket_id, depth);
    ++bucket_count_;
  }

  auto bucket = buckets_[bucket_id];

  // already in bucket, override
  if (bucket->items.find(key) != bucket->items.end()) {
    bucket->items[key] = value;
    return;
  }

  // insert to target bucket
  bucket->items.insert({key, value});

  // may need split & redistribute bucket
  if (bucket->items.size() > bucket_size_) {
    std::shared_ptr<Bucket> new_bucket = split(bucket);

    // if overflow, alloc overflow bucket
    if (new_bucket == nullptr) {
      return;
    }

    // rearrange pointers, may need increase global depth
    if (bucket->depth > depth) {
      auto size = buckets_.size();
      auto factor = (1 << (bucket->depth - depth));

      // global depth always greater equal than local depth
      depth = bucket->depth;

      buckets_.resize(buckets_.size()*factor);

      // update to right index: for not the split point
      for (size_t i = 0; i < size; ++i) {
        if (buckets_[i] != bucket) {
          for (size_t j = i + size; j < buckets_.size(); j += size) {
            buckets_[j] = buckets_[i];
          }
        }
      }

      // update to right index: for split point
      if (bucket->id != bucket_id) {
        buckets_[bucket_id].reset();
        buckets_[bucket->id] = bucket;
      }
    }
    buckets_[new_bucket->id] = new_bucket;
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
