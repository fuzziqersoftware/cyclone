#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <phosg/ConsistentHashRing.hh>
#include <thread>
#include <unordered_map>

#include "Store.hh"
#include "MultiStore.hh"


class ConsistentHashMultiStore : public MultiStore {
public:
  ConsistentHashMultiStore() = delete;
  ConsistentHashMultiStore(const ConsistentHashMultiStore& rhs) = delete;
  ConsistentHashMultiStore(
      const std::unordered_map<std::string, std::shared_ptr<Store>>& stores,
      int64_t precision);
  virtual ~ConsistentHashMultiStore();
  const ConsistentHashMultiStore& operator=(const ConsistentHashMultiStore& rhs) = delete;

  int64_t get_precision() const;
  void set_precision(int64_t new_precision);

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

  virtual std::unordered_map<std::string, int64_t> get_stats(bool rotate);

  struct VerifyProgress {
    std::atomic<int64_t> keys_examined;
    std::atomic<int64_t> keys_moved;
    std::atomic<int64_t> read_all_errors;
    std::atomic<int64_t> update_metadata_errors;
    std::atomic<int64_t> write_errors;
    std::atomic<int64_t> delete_errors;
    std::atomic<int64_t> find_queries_executed;
    std::atomic<int64_t> start_time;
    std::atomic<int64_t> end_time; // 0 if in progress
    std::atomic<bool> repair;
    std::atomic<bool> cancelled;

    VerifyProgress();

    bool in_progress() const;
  };

  const VerifyProgress& get_verify_progress() const;
  bool start_verify(bool repair);
  bool cancel_verify();

  bool get_read_from_all() const;
  bool set_read_from_all(bool read_from_all);

private:
  std::shared_ptr<ConsistentHashRing> ring;
  int64_t precision;

  void create_ring();

  std::atomic<bool> should_exit;
  std::thread verify_thread;
  std::mutex verify_thread_lock;
  VerifyProgress verify_progress;
  std::atomic<bool> read_from_all;

  void verify_thread_routine();
};
