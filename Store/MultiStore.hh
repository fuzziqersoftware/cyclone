#pragma once

#include <memory>
#include <unordered_map>

#include "Store.hh"


class MultiStore : public Store {
public:
  struct StoreInfo {
    std::shared_ptr<Store> store;
    bool requires_network_calls;

    StoreInfo(std::shared_ptr<Store> store, bool requires_network_calls);
  };

  MultiStore() = delete;
  MultiStore(const MultiStore& rhs) = delete;
  MultiStore(const std::unordered_map<std::string, StoreInfo>& substores);
  virtual ~MultiStore() = default;
  const MultiStore& operator=(const MultiStore& rhs) = delete;

  const std::unordered_map<std::string, std::shared_ptr<Store>>& get_substores() const;

  virtual void set_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>>& autocreate_rules);

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

  virtual std::shared_ptr<FindTask> find(StoreTaskManager* m,
      std::shared_ptr<const FindArguments> args, BaseFunctionProfiler* profiler);

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  virtual std::string str() const;

protected:
  std::unordered_map<std::string, std::shared_ptr<Store>> all_stores;
  std::vector<std::string> ordered_store_names;
  std::vector<std::shared_ptr<Store>> ordered_stores;

  class MultiStoreUpdateMetadataTask;
  class MultiStoreDeleteSeriesTask;
  class MultiStoreRenameSeriesTask;
  class MultiStoreReadTask;
  class MultiStoreReadAllTask;
  class MultiStoreWriteTask;
  class MultiStoreFindTask;
};
