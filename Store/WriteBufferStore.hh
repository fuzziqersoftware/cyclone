#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include <phosg/Concurrency.hh>

#include "Store.hh"
#include "RateLimiter.hh"


class WriteBufferStore : public Store {
public:
  WriteBufferStore() = delete;
  WriteBufferStore(const WriteBufferStore& rhs) = delete;
  WriteBufferStore(std::shared_ptr<Store> store, size_t num_write_threads,
      size_t batch_size, size_t max_update_metadatas_per_second,
      size_t max_write_batches_per_second,
      ssize_t disable_rate_limit_for_queue_length);
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

  std::shared_ptr<Store> get_substore() const;

  virtual void set_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>>& autocreate_rules);

  virtual std::unordered_map<std::string, std::string> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior, bool skip_buffering,
      bool local_only);
  virtual std::unordered_map<std::string, int64_t> delete_series(
      const std::vector<std::string>& patterns, bool local_only);

  virtual std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time, bool local_only);
  virtual ReadAllResult read_all(const std::string& key_name, bool local_only);
  virtual std::unordered_map<std::string, std::string> write(
      const std::unordered_map<std::string, Series>& data, bool skip_buffering,
      bool local_only);

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns, bool local_only);

  virtual void flush();

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  virtual int64_t delete_from_cache(const std::string& path, bool local_only);
  virtual int64_t delete_pending_writes(const std::string& pattern, bool local_only);

  virtual std::string str() const;

private:
  std::shared_ptr<Store> store;
  std::atomic<size_t> max_update_metadatas_per_second;
  std::atomic<size_t> max_write_batches_per_second;
  std::atomic<ssize_t> disable_rate_limit_for_queue_length;

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
  };

  // TODO: add writes-in-progress so they can be merged with reads too
  std::atomic<size_t> queued_update_metadatas;
  std::atomic<size_t> queued_writes;
  std::atomic<size_t> queued_datapoints;
  std::map<std::string, QueueItem> queue;
  mutable rw_lock queue_lock;

  static std::string get_directory_match_for_queue_item(
      const std::string& queue_item, const std::string& pattern);

  RateLimiter update_metadata_rate_limiter;
  RateLimiter write_batch_rate_limiter;

  size_t batch_size;
  std::atomic<bool> should_exit;
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

  void write_thread_routine(size_t thread_index);
};
