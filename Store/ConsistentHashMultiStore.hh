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
      const std::unordered_map<std::string, StoreInfo>& substores,
      int64_t precision);
  virtual ~ConsistentHashMultiStore();
  const ConsistentHashMultiStore& operator=(const ConsistentHashMultiStore& rhs) = delete;

  int64_t get_precision() const;
  void set_precision(int64_t new_precision);

  virtual std::shared_ptr<UpdateMetadataTask> update_metadata(
      StoreTaskManager* m, std::shared_ptr<const UpdateMetadataArguments> args,
      BaseFunctionProfiler* profiler);
  virtual std::shared_ptr<DeleteSeriesTask> delete_series(StoreTaskManager* m,
      std::shared_ptr<const DeleteSeriesArguments> args,
      BaseFunctionProfiler* profiler);
  virtual std::shared_ptr<RenameSeriesTask> rename_series(StoreTaskManager* m,
      std::shared_ptr<const RenameSeriesArguments> args,
      BaseFunctionProfiler* profiler);

  virtual std::shared_ptr<ReadTask> read(StoreTaskManager* m,
      std::shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler);
  virtual std::shared_ptr<ReadAllTask> read_all(StoreTaskManager* m,
      std::shared_ptr<const ReadAllArguments> args,
      BaseFunctionProfiler* profiler);

  virtual std::shared_ptr<WriteTask> write(StoreTaskManager* m,
      std::shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler);

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

protected:
  std::shared_ptr<ConsistentHashRing> ring;
  int64_t precision;

  void create_ring();

  std::atomic<bool> should_exit;
  std::thread verify_thread;
  std::mutex verify_thread_lock;
  VerifyProgress verify_progress;
  std::atomic<bool> read_from_all;

  void verify_thread_routine();

  class ConsistentHashMultiStoreUpdateMetadataTask;
  class ConsistentHashMultiStoreDeleteSeriesTask;
  class ConsistentHashMultiStoreRenameSeriesTask;
  class ConsistentHashMultiStoreReadTask;
  class ConsistentHashMultiStoreReadAllTask;
  class ConsistentHashMultiStoreWriteTask;
};
