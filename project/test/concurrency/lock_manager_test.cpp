/**
 * lock_manager_test.cpp
 */

#include <thread>

#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace cmudb {

// std::thread is movable
class scoped_guard {
  std::thread t;
public:
  explicit scoped_guard(std::thread t_) : t(std::move(t_)) {
    if (!t.joinable()) {
      throw std::logic_error("No thread");
    }
  }
  ~scoped_guard() {
    t.join();
  }
  scoped_guard(const scoped_guard &) = delete;
  scoped_guard &operator=(const scoped_guard &)= delete;
};

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */
TEST(LockManagerTest, BasicTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::thread t0([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t1([&] {
    Transaction txn(1);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  t0.join();
  t1.join();
}

// basic wait-die test
TEST(LockManagerTest, DeadlockTest1) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, go2, t1, t2;
  std::shared_future<void> ready(go.get_future());

  std::thread thread0([&, ready] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);

    t1.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    // waiting thread2 call LockExclusive before unlock
    go2.get_future().wait();

    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread1([&, ready] {
    Transaction txn(1);

    // wait thread t0 to get shared lock first
    t2.set_value();
    ready.wait();

    bool res = lock_mgr.LockExclusive(&txn, rid);
    go2.set_value();

    EXPECT_EQ(res, false);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
  });

  t1.get_future().wait();
  t2.get_future().wait();

  // go!
  go.set_value();

  thread0.join();
  thread1.join();
}

TEST(LockManagerTest, DeadlockTest2) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, t1, t2;
  std::shared_future<void> ready(go.get_future());

  std::thread thread0([&, ready] {
    Transaction txn(0);

    t1.set_value();
    ready.wait();

    // will block and can wait
    bool res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread1([&, ready] {
    Transaction txn(1);

    bool res = lock_mgr.LockExclusive(&txn, rid);

    t2.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  t1.get_future().wait();
  t2.get_future().wait();

  // go!
  go.set_value();

  thread0.join();
  thread1.join();
}

TEST(LockManagerTest, DeadlockTest3) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  RID rid2{0, 1};

  std::promise<void> go, t1, t2;
  std::shared_future<void> ready(go.get_future());

  std::thread thread0([&, ready] {
    Transaction txn(0);

    // try get exclusive lock on rid2, will succeed
    bool res = lock_mgr.LockExclusive(&txn, rid2);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    t1.set_value();
    ready.wait();

    // will block and can wait
    res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    // unlock rid1
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);

    // unblock rid2
    res = lock_mgr.Unlock(&txn, rid2);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread1([&, ready] {
    Transaction txn(1);

    // try to get shared lock on rid, will succeed
    bool res = lock_mgr.LockExclusive(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    t2.set_value();
    ready.wait();

    res = lock_mgr.LockShared(&txn, rid2);

    EXPECT_EQ(res, false);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // unlock rid
    res = lock_mgr.Unlock(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
  });

  t1.get_future().wait();
  t2.get_future().wait();

  // go!
  go.set_value();

  thread0.join();
  thread1.join();
}

} // namespace cmudb
