/**
 * lru_replacer_test.cpp
 */

#include <cstdio>

#include "buffer/lru_replacer.h"
#include "gtest/gtest.h"

namespace cmudb {

TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer<int> lru_replacer;
  
  // push element into replacer
  lru_replacer.Insert(1);
  lru_replacer.Insert(2);
  lru_replacer.Insert(3);
  lru_replacer.Insert(4);
  lru_replacer.Insert(5);
  lru_replacer.Insert(6);
  lru_replacer.Insert(1);
  EXPECT_EQ(6, lru_replacer.Size());
  
  // pop element from replacer
  int value;
  lru_replacer.Victim(value);
  EXPECT_EQ(2, value);
  lru_replacer.Victim(value);
  EXPECT_EQ(3, value);
  lru_replacer.Victim(value);
  EXPECT_EQ(4, value);
  
  // remove element from replacer
  EXPECT_EQ(false, lru_replacer.Erase(4));
  EXPECT_EQ(true, lru_replacer.Erase(6));
  EXPECT_EQ(2, lru_replacer.Size());
  
  // pop element from replacer after removal
  lru_replacer.Victim(value);
  EXPECT_EQ(5, value);
  lru_replacer.Victim(value);
  EXPECT_EQ(1, value);
}

} // namespace cmudb
