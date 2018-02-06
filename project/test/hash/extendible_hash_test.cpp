/**
 * extendible_hash_test.cpp
 */

#include <thread>

#include "hash/extendible_hash.h"
#include "gtest/gtest.h"

namespace cmudb {

TEST(ExtendibleHashTest, SampleTest) {
  // set leaf size as 2
  ExtendibleHash<int, std::string> *test =
      new ExtendibleHash<int, std::string>(2);

  // insert several key/value pairs
  test->Insert(1, "a");
  test->Insert(2, "b");
  test->Insert(3, "c");
  test->Insert(4, "d");
  test->Insert(5, "e");
  test->Insert(6, "f");
  test->Insert(7, "g");
  test->Insert(8, "h");
  test->Insert(9, "i");
  EXPECT_EQ(2, test->GetLocalDepth(0));
  EXPECT_EQ(3, test->GetLocalDepth(1));
  EXPECT_EQ(2, test->GetLocalDepth(2));
  EXPECT_EQ(2, test->GetLocalDepth(3));

  // find test
  std::string result;
  test->Find(9, result);
  EXPECT_EQ("i", result);
  test->Find(8, result);
  EXPECT_EQ("h", result);
  test->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_EQ(0, test->Find(10, result));

  // delete test
  EXPECT_EQ(1, test->Remove(8));
  EXPECT_EQ(1, test->Remove(4));
  EXPECT_EQ(1, test->Remove(1));
  EXPECT_EQ(0, test->Remove(20));

  delete test;
}

TEST(ExtendibleHashTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;
  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    std::shared_ptr<ExtendibleHash<int, int>> test{new ExtendibleHash<int, int>(2)};
    std::vector<std::thread> threads;
    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([tid, &test]() {
        test->Insert(tid, tid);
      }));
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }
    EXPECT_EQ(test->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(test->Find(i, val));
      EXPECT_EQ(val, i);
    }
  }
}

TEST(ExtendibleHashTest, ConcurrentRemoveTest) {
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    std::shared_ptr<ExtendibleHash<int, int>> test{new ExtendibleHash<int, int>(2)};
    std::vector<std::thread> threads;
    std::vector<int> values{0, 10, 16, 32, 64};
    for (int value : values) {
      test->Insert(value, value);
    }
    EXPECT_EQ(test->GetGlobalDepth(), 6);
    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([tid, &test, &values]() {
        test->Remove(values[tid]);
        test->Insert(tid + 4, tid + 4);
      }));
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }
    EXPECT_EQ(test->GetGlobalDepth(), 6);
    int val;
    EXPECT_EQ(0, test->Find(0, val));
    EXPECT_EQ(1, test->Find(8, val));
    EXPECT_EQ(0, test->Find(16, val));
    EXPECT_EQ(0, test->Find(3, val));
    EXPECT_EQ(1, test->Find(4, val));
  }
}

} // namespace cmudb
