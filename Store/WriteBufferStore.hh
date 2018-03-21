#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include <phosg/Concurrency.hh>

#include "Store.hh"


class WriteBufferStore : public Store {
public:
  WriteBufferStore() = delete;
  WriteBufferStore(const WriteBufferStore& rhs) = delete;
  WriteBufferStore(std::shared_ptr<Store> store, size_t num_write_threads,
      size_t batch_size);
  virtual ~WriteBufferStore();

  const WriteBufferStore& operator=(const WriteBufferStore& rhs) = delete;

  virtual void set_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>> autocreate_rules);

  virtual std::unordered_map<std::string, std::string> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior);
  virtual std::unordered_map<std::string, std::string> delete_series(
      const std::vector<std::string>& key_names);

  virtual std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time);
  virtual std::unordered_map<std::string, std::string> write(
      const std::unordered_map<std::string, Series>& data);

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns);

  virtual void flush();

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  virtual int64_t delete_from_cache(const std::string& path);
  virtual int64_t delete_pending_writes(const std::string& pattern);

  virtual std::string str() const;

private:
  std::shared_ptr<Store> store;

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

  std::atomic<size_t> queued_update_metadatas;
  std::atomic<size_t> queued_writes;
  std::atomic<size_t> queued_datapoints;
  std::map<std::string, QueueItem> queue;
  mutable rw_lock queue_lock;

  static std::string get_directory_match_for_queue_item(
      const std::string& queue_item, const std::string& pattern);

  // TODO: switch from multithreaded model to async i/o model here
  // (WhisperArchive needs to support async i/o first)
  size_t batch_size;
  std::atomic<bool> should_exit;
  std::vector<std::thread> write_threads;

  void write_thread_routine();
};
