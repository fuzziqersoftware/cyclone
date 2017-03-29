#pragma once

#include <stdint.h>

#include <atomic>
#include <mutex>
#include <phosg/Concurrency.hh>
#include <phosg/LRUSet.hh>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "DiskStore.hh"
#include "FixedAtomicRotator.hh"
#include "Whisper.hh"


class CachedDiskStore : public DiskStore {
public:
  CachedDiskStore() = delete;
  CachedDiskStore(const CachedDiskStore& rhs) = delete;
  explicit CachedDiskStore(const std::string& root_directory);
  virtual ~CachedDiskStore() = default;

  const CachedDiskStore& operator=(const CachedDiskStore& rhs) = delete;

  virtual std::unordered_map<std::string, std::string> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior);
  virtual std::unordered_map<std::string, std::string> delete_series(
      const std::vector<std::string>& key_names);

  virtual std::unordered_map<std::string, ReadResult> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time);
  virtual std::unordered_map<std::string, std::string> write(
      const std::unordered_map<std::string, Series>& data);

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns);

  virtual std::unordered_map<std::string, int64_t> get_stats(bool rotate);

  virtual int64_t delete_from_cache(const std::string& path);

protected:
  // TODO: eviction
  // TODO: cache data as well as metadata

  struct KeyPath {
    std::vector<std::string> directories;
    std::string basename;

    KeyPath(const std::string& key_name);
  };

  std::string filename_for_path(const KeyPath& p);


  // the subdirectory locking structure here is kind of complex, but can be
  // thought of in terms of the following invariants:
  // 1. holding a read lock on a level guarantees that no file/directory in that
  //    level will be deleted (but items in subdirectories can be deleted)
  // 2. holding a write lock on a level guarantees that no locks of any type are
  //    held anywhere in that subtree
  // to maintain these invariants, we always lock subtrees starting at the root.
  // a traversal ends up holding read locks on all the directories from the root
  // to the leaf, but not holding any locks on the leaf - holding a read lock on
  // the parent directory is sufficient to ensure that another thread doesn't
  // delete the leaf while it's being accessed. this locking structure also
  // isn't vulnerable to deadlocks since we follow a strict ordering when taking
  // locks.
  struct CachedDirectoryContents {
    std::atomic<bool> list_complete;

    std::unordered_map<std::string, std::unique_ptr<CachedDirectoryContents>> subdirectories;
    std::unordered_map<std::string, WhisperArchive> files;
    mutable rw_lock subdirectories_lock;
    mutable rw_lock files_lock;

    // if one of the above locks is held for writing, the LRUs below can be
    // modified without holding the appropriate lock
    LRUSet<std::string> subdirectories_lru;
    LRUSet<std::string> files_lru;
    mutable std::mutex subdirectories_lru_lock;
    mutable std::mutex files_lru_lock;

    CachedDirectoryContents() = default;
    CachedDirectoryContents(const CachedDirectoryContents& other) = delete;
    CachedDirectoryContents(CachedDirectoryContents&& other) = default;
    CachedDirectoryContents& operator=(const CachedDirectoryContents& other) = delete;

    void touch_subdirectory(const std::string& item);
    void touch_file(const std::string& item);

    std::pair<size_t, size_t> get_counts() const;
  };
  CachedDirectoryContents cache_root;

  bool create_cache_subdirectory(CachedDirectoryContents* level,
      const std::string& item);
  bool create_cache_file(CachedDirectoryContents* level,
      const std::string& item, const std::string& filesystem_path);
  void check_and_delete_cache_path(const KeyPath& path);
  void populate_cache_level(CachedDirectoryContents* level,
      const std::string& filesystem_path);


  struct CacheTraversal {
    std::vector<CachedDirectoryContents*> levels;
    CachedDirectoryContents* level;
    WhisperArchive* archive;

    std::string filesystem_path;
    rw_guard_multi guards;

    CacheTraversal(CachedDirectoryContents* root, std::string root_directory,
        size_t expected_levels);
    void move_to_level(const std::string& item);
  };

  CacheTraversal traverse_cache_tree(const std::vector<std::string>& cache_path,
      bool create = false);
  CacheTraversal traverse_cache_tree(const KeyPath& p,
      const SeriesMetadata* metadata_to_create = NULL);


  struct CacheStats : public Stats { // Stats comes from DiskStore
    std::atomic<size_t> cache_directory_hits;
    std::atomic<size_t> cache_directory_misses;
    std::atomic<size_t> cache_directory_creates;
    std::atomic<size_t> cache_directory_deletes;
    std::atomic<size_t> cache_directory_populates;
    std::atomic<size_t> cache_file_hits;
    std::atomic<size_t> cache_file_misses;
    std::atomic<size_t> cache_file_creates;
    std::atomic<size_t> cache_file_deletes;

    CacheStats();
    CacheStats& operator=(const CacheStats& other);

    std::unordered_map<std::string, int64_t> to_map() const;
  };
  FixedAtomicRotator<CacheStats> stats;
  std::atomic<size_t> cache_directory_count;
  std::atomic<size_t> cache_file_count;
};
