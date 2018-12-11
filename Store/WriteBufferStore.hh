#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <deque>

#include <phosg/Concurrency.hh>

#include "Store.hh"
#include "Utils/RateLimiter.hh"


class WriteBufferStore : public Store {
public:
  WriteBufferStore() = delete;
  WriteBufferStore(const WriteBufferStore& rhs) = delete;
  WriteBufferStore(std::shared_ptr<Store> store, size_t num_write_threads,
      size_t batch_size, size_t max_update_metadatas_per_second,
      size_t max_write_batches_per_second,
      ssize_t disable_rate_limit_for_queue_length, bool merge_find_patterns,
      bool enable_deferred_deletes);
  virtual ~WriteBufferStore();
  const WriteBufferStore& operator=(const WriteBufferStore& rhs) = delete;

  size_t get_batch_size() const;
  void set_batch_size(size_t new_value);

  size_t get_max_update_metadatas_per_second() const;
  void set_max_update_metadatas_per_second(size_t new_value);
  size_t get_max_write_batches_per_second() const;
  void set_max_write_batches_per_second(size_t new_value);
  ssize_t get_disable_rate_limit_for_queue_length() const;
  void set_disable_rate_limit_for_queue_length(ssize_t new_value);
  bool get_merge_find_patterns() const;
  void set_merge_find_patterns(bool new_value);
  bool get_enable_deferred_deletes() const;
  void set_enable_deferred_deletes(bool new_value);

  std::shared_ptr<Store> get_substore() const;

  virtual void set_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>>& autocreate_rules);

  virtual std::unordered_map<std::string, Error> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, DeleteResult> delete_series(
      const std::vector<std::string>& patterns, bool deferred, bool local_only,
      BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, Error> rename_series(
      const std::unordered_map<std::string, std::string>& renames,
      bool merge, bool local_only, BaseFunctionProfiler* profiler);

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

  virtual void flush();

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  virtual int64_t delete_pending_writes(const std::string& pattern, bool local_only);

  virtual std::string str() const;

private:
  std::shared_ptr<Store> store;
  std::atomic<size_t> max_update_metadatas_per_second;
  std::atomic<size_t> max_write_batches_per_second;
  std::atomic<ssize_t> disable_rate_limit_for_queue_length;
  std::atomic<bool> merge_find_patterns;
  std::atomic<bool> enable_deferred_deletes;

  std::atomic<bool> should_exit;

  struct QueueItem {
    SeriesMetadata metadata;

    bool create_new;
    UpdateMetadataBehavior update_behavior;

    std::map<uint32_t, double> data;

    QueueItem() = default;
    QueueItem(const QueueItem&) = delete;
    QueueItem(QueueItem&&) = default;
    explicit QueueItem(const Series& s);
    QueueItem(const SeriesMetadata& metadata, bool create_new,
        UpdateMetadataBehavior update_behavior);

    bool has_update_metadata() const;
    bool has_data() const;

    void erase_update_metadata();
  };

  // TODO: add writes-in-progress so they can be merged with reads too
  std::atomic<size_t> queued_update_metadatas;
  std::atomic<size_t> queued_writes;
  std::atomic<size_t> queued_datapoints;
  std::map<std::string, QueueItem> queue;
  mutable rw_lock queue_lock;

  void merge_earlier_queue_items_locked(
      std::unordered_map<std::string, QueueItem>&& items);

  RateLimiter update_metadata_rate_limiter;
  RateLimiter write_batch_rate_limiter;

  size_t batch_size;
  struct WriteThread {
    std::thread t;
    std::atomic<int64_t> last_queue_restart;
    std::atomic<int64_t> queue_sweep_time;

    WriteThread();
    WriteThread(const WriteThread&) = delete;
    WriteThread(WriteThread&&) = delete;
    WriteThread& operator=(const WriteThread&) = delete;
    WriteThread& operator=(WriteThread&&) = delete;
  };
  std::vector<std::unique_ptr<WriteThread>> write_threads;

  std::deque<std::string> delete_queue;
  mutable rw_lock delete_queue_lock;

  std::thread delete_thread;

  void write_thread_routine(size_t thread_index);
  void delete_thread_routine();
};
