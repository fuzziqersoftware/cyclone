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
  explicit CachedDiskStore(const std::string& root_directory,
      size_t directory_limit, size_t file_limit);
  virtual ~CachedDiskStore();
  const CachedDiskStore& operator=(const CachedDiskStore& rhs) = delete;

  size_t get_directory_limit() const;
  size_t get_file_limit() const;
  void set_directory_limit(size_t new_value);
  void set_file_limit(size_t new_value);
  virtual void set_directory(const std::string& new_value);

  virtual std::unordered_map<std::string, Error> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, int64_t> delete_series(
      const std::vector<std::string>& patterns, bool local_only,
      BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, Error> rename_series(
      const std::unordered_map<std::string, std::string>& renames,
      bool local_only, BaseFunctionProfiler* profiler);

  virtual std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time, bool local_only, BaseFunctionProfiler* profiler);
  virtual ReadAllResult read_all(const std::string& key_name, bool local_only,
      BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, Error> write(
      const std::unordered_map<std::string, Series>& data, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns, bool local_only,
      BaseFunctionProfiler* profiler);

  virtual std::unordered_map<std::string, int64_t> get_stats(bool rotate);

  virtual int64_t delete_from_cache(const std::string& path, bool local_only);

  virtual std::string str() const;

protected:
  // TODO: cache data as well as metadata

  // convenience object for splitting/representing a key path
  struct KeyPath {
    std::vector<std::string> directories;
    std::string basename;

    KeyPath(const std::string& key_name);
    std::string str() const;
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
    LRUSet<std::string> subdirectories_lru;
    mutable std::mutex subdirectories_lru_lock;
    LRUSet<std::string> files_lru;
    mutable std::mutex files_lru_lock;

    CachedDirectoryContents();
    CachedDirectoryContents(const CachedDirectoryContents& other) = delete;
    CachedDirectoryContents(CachedDirectoryContents&& other) = default;
    CachedDirectoryContents& operator=(const CachedDirectoryContents& other) = delete;

    void touch_subdirectory(const std::string& item);
    void touch_file(const std::string& item);

    std::pair<size_t, size_t> get_counts() const;

    std::string str() const;
  };
  CachedDirectoryContents cache_root;

  bool create_cache_directory_locked(CachedDirectoryContents* level,
      const std::string& item);
  bool create_cache_file_locked(CachedDirectoryContents* level,
      const std::string& item, const std::string& filesystem_path);
  void check_and_delete_cache_path(const KeyPath& path);
  void populate_cache_level(CachedDirectoryContents* level,
      const std::string& filesystem_path);

  void find_all_recursive(FindResult& r, CachedDirectoryContents* level,
      const std::string& level_path, BaseFunctionProfiler* profiler);

  // object representing a path down the cache tree
  struct CacheTraversal {
    std::vector<CachedDirectoryContents*> levels;
    CachedDirectoryContents* level;
    WhisperArchive* archive;

    std::string filesystem_path;
    vector<rw_guard> guards;

    CacheTraversal(CachedDirectoryContents* root, std::string root_directory,
        size_t expected_levels);

    CacheTraversal(const CacheTraversal&) = delete;
    CacheTraversal(CacheTraversal&&) = default;
    CacheTraversal& operator=(const CacheTraversal&) = delete;
    CacheTraversal& operator=(CacheTraversal&&) = default;

    void move_to_level(const std::string& item);
  };

  CacheTraversal traverse_cache_tree(const std::vector<std::string>& cache_path,
      bool create = false);
  CacheTraversal traverse_cache_tree(const KeyPath& p,
      const SeriesMetadata* metadata_to_create = NULL,
      bool write_lock_files = false);


  // statistics tracking - these are rotated every minute
  struct Stats : public DiskStore::Stats {
    std::atomic<size_t> directory_hits;
    std::atomic<size_t> directory_misses;
    std::atomic<size_t> directory_creates;
    std::atomic<size_t> directory_deletes;
    std::atomic<size_t> directory_populates;
    std::atomic<size_t> file_hits;
    std::atomic<size_t> file_misses;
    std::atomic<size_t> file_creates;
    std::atomic<size_t> file_deletes;

    Stats();
    Stats& operator=(const Stats& other);

    std::unordered_map<std::string, int64_t> to_map() const;
  };
  FixedAtomicRotator<Stats> stats;
  std::atomic<size_t> directory_count;
  std::atomic<size_t> file_count;


  // size limitation (eviction)
  std::atomic<size_t> directory_limit;
  std::atomic<size_t> file_limit;
  std::atomic<bool> should_exit;
  std::thread evict_items_thread;

  bool evict_items();
  void evict_items_thread_routine();
};
