#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "Store.hh"
#include "Query/Parser.hh"


class QueryStore : public Store {
public:
  QueryStore() = delete;
  QueryStore(const QueryStore& rhs) = delete;
  QueryStore(std::shared_ptr<Store> stores);
  virtual ~QueryStore() = default;
  const QueryStore& operator=(const QueryStore& rhs) = delete;

  std::shared_ptr<Store> get_substore() const;

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
  std::shared_ptr<Store> store;

  static void extract_series_references_into(
      std::unordered_set<std::string>& substore_reads_set, const Query& q);
  Error execute_query(Query& q,
      const std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>& substore_results);

  class QueryStoreReadTask;
};
