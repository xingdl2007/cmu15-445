/**
 * b_plus_tree.cpp
 */

#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

template <typename KeyType, typename ValueType, typename KeyComparator>
BPlusTree<KeyType, ValueType, KeyComparator>::
BPlusTree(const std::string &name,
          BufferPoolManager *buffer_pool_manager,
          const KeyComparator &comparator,
          page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
GetValue(const KeyType &key, std::vector<ValueType> &result,
         Transaction *transaction) {
  // empty?
  if (IsEmpty()) {
    return false;
  }
  auto *node = reinterpret_cast<BPlusTreePage *>(
      buffer_pool_manager_->FetchPage(root_page_id_));

  // no buffer?
  if (node == nullptr) {
    return false;
  }

  // find the leaf node
  while (node->IsLeafPage()) {
    auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    page_id_t next = internal->Lookup(key, comparator_);

    node = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(next));

    // if no buffer, return immediately
    if (node == nullptr) {
      return false;
    }

    // necessary?
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  }

  auto *leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
  ValueType value;
  if (leaf->Lookup(key, value, comparator_)) {
    result.push_back(value);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
StartNewTree(const KeyType &key, const ValueType &value) {
  auto root = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->NewPage(root_page_id_));

  // throw "out of memory" exception
  if (root == nullptr) {
    throw std::bad_alloc();
  }
  UpdateRootPageId(true);
  root->Insert(key, value, comparator_);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto node = reinterpret_cast<BPlusTreePage *>(
      buffer_pool_manager_->FetchPage(root_page_id_));

  assert(node->IsRootPage());

  while (!node->IsLeafPage()) {
    auto child_page_id =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>
        (node)->Lookup(key, comparator_);

    // unpin when we are done
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);

    node = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(child_page_id));
  }

  // find the leaf node
  auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    ValueType v;
    if (leaf->Lookup(key, v, comparator_)) {
      return false;
    }
    leaf->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  } else {
    // split
    auto *leaf2 = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf);

    if (comparator_(key, leaf2->KeyAt(0)) < 0) {
      leaf->Insert(key, value, comparator_);
    } else {
      leaf2->Insert(key, value, comparator_);
    }
    InsertIntoParent(leaf, leaf2->KeyAt(0), leaf2, transaction);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N> N *BPlusTree<KeyType, ValueType, KeyComparator>::
Split(N *node) {
  page_id_t page_id;
  auto new_node = reinterpret_cast<N *>(buffer_pool_manager_->NewPage(page_id));
  if (new_node == nullptr) {
    throw std::bad_alloc();
  }
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                 BPlusTreePage *new_node, Transaction *transaction) {
  if (old_node->IsRootPage()) {
    auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>
    (buffer_pool_manager_->NewPage(root_page_id_));
    if (root == nullptr) {
      throw std::bad_alloc();
    }
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // update to new 'root_page_id'
    UpdateRootPageId(false);

    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

    // root is also done
    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
  } else {
    auto internal =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>
        (buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));

    if (internal == nullptr) {
      throw std::bad_alloc();
    }

    if (internal->GetSize() < internal->GetMaxSize()) {
      internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      // set ParentPageID
      new_node->SetParentPageId(internal->GetPageId());

      // new_node is split from old_node, must be dirty
      buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

      // internal is also done
      buffer_pool_manager_->UnpinPage(internal->GetPageId(), true);
    } else {
      // split
      auto internal2 =
          Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(internal);

      if (comparator_(key, internal2->KeyAt(0)) < 0) {
        internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(internal->GetPageId());
      } else {
        internal2->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(internal2->GetPageId());
      }

      buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

      // recursive
      InsertIntoParent(internal, internal2->KeyAt(0), internal2);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto node = reinterpret_cast<BPlusTreePage *>(
      buffer_pool_manager_->FetchPage(root_page_id_));

  assert(node->IsRootPage());
  while (!node->IsLeafPage()) {
    auto child_page_id =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>
        (node)->Lookup(key, comparator_);
    // unpin when we are done
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    node = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(child_page_id));
  }

  // find the leaf node
  auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);

  leaf->RemoveAndDeleteRecord(key, comparator_);

  if (CoalesceOrRedistribute(leaf, transaction)) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    buffer_pool_manager_->DeletePage(leaf->GetPageId());
  } else {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // Base condition: reach root node
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  // no need to delete node
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  // find sibling first, always find the previous one if possible
  auto parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>
      (buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  if (parent == nullptr) {
    throw std::bad_alloc();
  }

  // should be the same parent
  int value_index = parent->ValueIndex(node->GetPageId());
  int sibling_page_id;
  if (value_index == 0) {
    sibling_page_id = parent->ValueAt(value_index + 1);
  } else {
    sibling_page_id = parent->ValueAt(value_index - 1);
  }
  auto sibling = reinterpret_cast<N *>
  (buffer_pool_manager_->FetchPage(sibling_page_id));

  // redistribute
  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
    if (value_index == 0) {
      Redistribute<N>(sibling, node, 0);   // sibling is successor of node
    } else {
      Redistribute<N>(sibling, node, 1);   // sibling is predecessor of node
    }
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return false;
  }

  // merge
  if (value_index == 0) {
    if (Coalesce<N>(node, sibling, parent, value_index, transaction)) {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      buffer_pool_manager_->DeletePage(parent->GetPageId());
    } else {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }

    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), false);
    buffer_pool_manager_->DeletePage(sibling->GetPageId());

    // node should not be deleted
    return false;
  } else {
    if (Coalesce<N>(sibling, node, parent, value_index, transaction)) {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      buffer_pool_manager_->DeletePage(parent->GetPageId());
    } else {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }

    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);

    // node should be deleted
    return true;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
Coalesce(N *&neighbor_node, N *&node,
         BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
         int index, Transaction *transaction) {

  // neighbor_node is predecessor of node
  node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
  parent->Remove(index);

  if (CoalesceOrRedistribute(parent, transaction)) {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    buffer_pool_manager_->DeletePage(parent->GetPageId());
    return true;
  } else {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return false;
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
void BPlusTree<KeyType, ValueType, KeyComparator>::
Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  } else {
    auto parent =
        reinterpret_cast<BPlusTreeInternalPage<KeyType,
                                               page_id_t,
                                               KeyComparator> *>
        (buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    if (parent == nullptr) {
      throw std::bad_alloc();
    }
    int index = parent->ValueIndex(node->GetPageId());
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
AdjustRoot(BPlusTreePage *old_root_node) {
  // root is a leaf node, case 2
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
    return false;
  }

  // root is a internal node, case 1
  if (old_root_node->GetSize() == 1) {
    auto root =
        reinterpret_cast<BPlusTreeInternalPage<KeyType,
                                               page_id_t,
                                               KeyComparator> *>(old_root_node);
    root_page_id_ = root->ValueAt(0);
    UpdateRootPageId(false);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
INDEXITERATOR_TYPE BPlusTree<KeyType, ValueType, KeyComparator>::
Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
INDEXITERATOR_TYPE BPlusTree<KeyType, ValueType, KeyComparator>::
Begin(const KeyType &key) {
  return INDEXITERATOR_TYPE();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
B_PLUS_TREE_LEAF_PAGE_TYPE *BPlusTree<KeyType, ValueType, KeyComparator>::
FindLeafPage(const KeyType &key, bool leftMost) {
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method every time root page id is changed.
 * @parameter: insert_record default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
UpdateRootPageId(bool insert_record) {
  auto *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree structure, rank by rank
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
std::string BPlusTree<KeyType, ValueType, KeyComparator>::
ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
