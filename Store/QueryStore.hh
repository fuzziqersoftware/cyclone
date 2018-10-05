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

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  virtual int64_t delete_from_cache(const std::string& path, bool local_only);
  virtual int64_t delete_pending_writes(const std::string& pattern,
      bool local_only);

  virtual std::string str() const;

protected:
  std::shared_ptr<Store> store;

  static void extract_series_references_into(
      std::unordered_set<std::string>& substore_reads_set, const Query& q);
  Error execute_query(Query& q,
      const std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>& substore_results);
};
