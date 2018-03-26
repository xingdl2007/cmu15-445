/**
 * lock_manager.cpp
 */

#include <cassert>
#include "concurrency/lock_manager.h"

namespace cmudb {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(mutex_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // must be in growing state
  assert(txn->GetState() == TransactionState::GROWING);

  Request req{txn->GetTransactionId(), LockMode::SHARED, false};
  if (lock_table_.count(rid) == 0) {
    Waiting waiting;
    waiting.exclusive_cnt = 0;
    waiting.oldest = txn->GetTransactionId();
    waiting.list.push_back(req);
    lock_table_[rid] = std::move(waiting);
  } else {
    if (lock_table_[rid].exclusive_cnt != 0 &&
        txn->GetTransactionId() > lock_table_[rid].oldest) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    if (lock_table_[rid].oldest > txn->GetTransactionId()) {
      lock_table_[rid].oldest = txn->GetTransactionId();
    }
    lock_table_[rid].list.push_back(req);
  }

  // maybe blocked
  Request *cur = nullptr;
  cond.wait(latch, [&]() -> bool {
    // all requests before this one are shared and granted
    bool all_shared = true, all_granted = true;
    for (auto &r: lock_table_[rid].list) {
      if (r.txn_id != txn->GetTransactionId()) {
        if (r.mode != LockMode::SHARED || !r.granted) {
          return false;
        }
      } else {
        cur = &r;
        return all_shared && all_granted;
      }
    }
    return false;
  });

  // granted shared lock
  assert(cur != nullptr && cur->txn_id == txn->GetTransactionId());
  cur->granted = true;
  txn->GetSharedLockSet()->insert(rid);

  // notify other threads
  cond.notify_all();
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(mutex_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // must be in growing state
  assert(txn->GetState() == TransactionState::GROWING);

  Request req{txn->GetTransactionId(), LockMode::EXCLUSIVE, false};
  if (lock_table_.count(rid) == 0) {
    Waiting waiting;
    waiting.oldest = txn->GetTransactionId();
    waiting.list.push_back(req);
    lock_table_[rid] = std::move(waiting);
  } else {
    // die
    if (txn->GetTransactionId() > lock_table_[rid].oldest) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    // wait
    lock_table_[rid].oldest = txn->GetTransactionId();
    lock_table_[rid].list.push_back(req);
  }

  ++lock_table_[rid].exclusive_cnt;

  // must be first of the waiting list
  cond.wait(latch, [&]() -> bool {
    return lock_table_[rid].list.front().txn_id == txn->GetTransactionId();
  });

  // granted exclusive lock
  assert(lock_table_[rid].list.front().txn_id == txn->GetTransactionId());

  lock_table_[rid].list.front().granted = true;
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(mutex_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // must be in growing state
  assert(txn->GetState() == TransactionState::GROWING);

  // maybe blocked
  cond.wait(latch, [&]() -> bool {
    return lock_table_[rid].list.front().txn_id == txn->GetTransactionId();
  });

  // upgrade to  lock
  assert(lock_table_[rid].list.front().txn_id == txn->GetTransactionId() &&
      lock_table_[rid].list.front().mode == LockMode::SHARED &&
      lock_table_[rid].list.front().granted);

  lock_table_[rid].list.front().mode = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(mutex_);

  // if strict 2pl, when unlock txn must be in committed or abort state
  if (strict_2PL_) {
    if (txn->GetState() != TransactionState::COMMITTED ||
        txn->GetState() != TransactionState::ABORTED) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  } else {
    if (txn->GetState() == TransactionState::GROWING) {
      // turn to shrinking state
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  assert(lock_table_.count(rid));
  for (auto it = lock_table_[rid].list.begin();
       it != lock_table_[rid].list.end(); ++it) {
    if (it->txn_id == txn->GetTransactionId()) {
      bool first = it == lock_table_[rid].list.begin();
      bool exclusive = it->mode == LockMode::EXCLUSIVE;

      if (exclusive) {
        --lock_table_[rid].exclusive_cnt;
      }
      lock_table_[rid].list.erase(it);

      // if it's first(shared) or exclusive(must be the first), notify all
      if (first || exclusive) {
        cond.notify_all();
      }
      break;
    }
  }
  return true;
}

} // namespace cmudb
