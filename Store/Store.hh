#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <phosg/Concurrency.hh>
#include <vector>

#include "../gen-cpp/Cyclone.h"
#include "Utils/FunctionProfiler.hh"
#include "StoreTask.hh"



class Store {
public:
  Store(const Store& rhs) = delete;
  virtual ~Store() = default;

  const Store& operator=(const Store& rhs) = delete;



  // forward declarations for public api
  class UpdateMetadataTask;
  class DeleteSeriesTask;
  class RenameSeriesTask;
  class ReadTask;
  class ReadAllTask;
  class WriteTask;
  class FindTask;
  class ResolvePatternsTask;
  class EmulateRenameSeriesTask;



  // client api

  virtual void set_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>>& autocreate_rules);

  // sync with UpdateMetadataBehavior in cyclone_if.thrift
  enum UpdateMetadataBehavior {
    // values passed to update_metadata to determine behavior on existing series
    Update = 0, // update existing series, preserving data
    Ignore = 1, // make no changes to existing series
    Recreate = 2, // update existing series, discarding data
  };

  struct UpdateMetadataArguments {
    SeriesMetadataMap metadata;
    UpdateMetadataBehavior behavior;
    bool create_new;
    bool skip_buffering;
    bool local_only;
  };
  virtual std::shared_ptr<UpdateMetadataTask> update_metadata(
      StoreTaskManager* m, std::shared_ptr<const UpdateMetadataArguments> args,
      BaseFunctionProfiler* profiler) = 0;
  std::shared_ptr<UpdateMetadataTask> update_metadata(
      StoreTaskManager* m, const SeriesMetadataMap& metadata,
      UpdateMetadataBehavior behavior, bool create_new, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);
  std::shared_ptr<UpdateMetadataTask> update_metadata(
      StoreTaskManager* m, SeriesMetadataMap&& metadata,
      UpdateMetadataBehavior behavior, bool create_new, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);

  struct DeleteSeriesArguments {
    std::vector<std::string> patterns;
    bool deferred;
    bool local_only;
  };
  virtual std::shared_ptr<DeleteSeriesTask> delete_series(StoreTaskManager* m,
      std::shared_ptr<const DeleteSeriesArguments> args,
      BaseFunctionProfiler* profiler) = 0;
  std::shared_ptr<DeleteSeriesTask> delete_series(StoreTaskManager* m,
      const std::vector<std::string>& patterns, bool deferred, bool local_only,
      BaseFunctionProfiler* profiler);
  std::shared_ptr<DeleteSeriesTask> delete_series(StoreTaskManager* m,
      std::vector<std::string>&& patterns, bool deferred, bool local_only,
      BaseFunctionProfiler* profiler);

  struct RenameSeriesArguments {
    std::unordered_map<std::string, std::string> renames;
    bool merge;
    bool local_only;
  };
  virtual std::shared_ptr<RenameSeriesTask> rename_series(StoreTaskManager* m,
      std::shared_ptr<const RenameSeriesArguments> args,
      BaseFunctionProfiler* profiler) = 0;
  std::shared_ptr<RenameSeriesTask> rename_series(StoreTaskManager* m,
      const std::unordered_map<std::string, std::string>& renames, bool merge,
      bool local_only, BaseFunctionProfiler* profiler);
  std::shared_ptr<RenameSeriesTask> rename_series(StoreTaskManager* m,
      std::unordered_map<std::string, std::string>&& renames, bool merge,
      bool local_only, BaseFunctionProfiler* profiler);

  struct ReadArguments {
    std::vector<std::string> key_names;
    int64_t start_time;
    int64_t end_time;
    bool local_only;
  };
  virtual std::shared_ptr<ReadTask> read(StoreTaskManager* m,
      std::shared_ptr<const ReadArguments> args,
      BaseFunctionProfiler* profiler) = 0;
  std::shared_ptr<ReadTask> read(StoreTaskManager* m,
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time, bool local_only, BaseFunctionProfiler* profiler);
  std::shared_ptr<ReadTask> read(StoreTaskManager* m,
      std::vector<std::string>&& key_names, int64_t start_time,
      int64_t end_time, bool local_only, BaseFunctionProfiler* profiler);

  struct ReadAllArguments {
    std::string key_name;
    bool local_only;
  };
  virtual std::shared_ptr<ReadAllTask> read_all(StoreTaskManager* m,
      std::shared_ptr<const ReadAllArguments> args,
      BaseFunctionProfiler* profiler) = 0;
  std::shared_ptr<ReadAllTask> read_all(StoreTaskManager* m,
      const std::string& key_name, bool local_only,
      BaseFunctionProfiler* profiler);
  std::shared_ptr<ReadAllTask> read_all(StoreTaskManager* m,
      std::string&& key_name, bool local_only, BaseFunctionProfiler* profiler);

  struct WriteArguments {
    std::unordered_map<std::string, Series> data;
    bool skip_buffering;
    bool local_only;
  };
  virtual std::shared_ptr<WriteTask> write(StoreTaskManager* m,
      std::shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler) = 0;
  std::shared_ptr<WriteTask> write(StoreTaskManager* m,
      const std::unordered_map<std::string, Series>& data, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);
  std::shared_ptr<WriteTask> write(StoreTaskManager* m,
      std::unordered_map<std::string, Series>&& data, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);

  struct FindArguments {
    std::vector<std::string> patterns;
    bool local_only;
  };
  virtual std::shared_ptr<FindTask> find(StoreTaskManager* m,
      std::shared_ptr<const FindArguments> args,
      BaseFunctionProfiler* profiler) = 0;
  std::shared_ptr<FindTask> find(StoreTaskManager* m,
      const std::vector<std::string>& patterns, bool local_only,
      BaseFunctionProfiler* profiler);
  std::shared_ptr<FindTask> find(StoreTaskManager* m,
      std::vector<std::string>&& patterns, bool local_only,
      BaseFunctionProfiler* profiler);

  virtual void flush(StoreTaskManager* m);

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);



  // profiling utilities

  static std::string string_for_update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior, bool skip_buffering,
      bool local_only);
  static std::string string_for_delete_series(
      const std::vector<std::string>& patterns, bool deferred, bool local_only);
  static std::string string_for_rename_series(
      const std::unordered_map<std::string, std::string>& renames,
      bool merge, bool local_only);
  static std::string string_for_read(const std::vector<std::string>& key_names,
      int64_t start_time, int64_t end_time, bool local_only);
  static std::string string_for_read_all(const std::string& key_name,
      bool local_only);
  static std::string string_for_write(
      const std::unordered_map<std::string, Series>& data, bool skip_buffering,
      bool local_only);
  static std::string string_for_find(const std::vector<std::string>& patterns,
      bool local_only);



  // debugging functions

  virtual std::string str() const = 0;



  // helper functions

  static bool token_is_pattern(const std::string& token);
  static bool pattern_is_basename(const std::string& token);
  static bool pattern_is_indeterminate(const std::string& pattern);
  static bool name_matches_pattern(const std::string& name,
      const std::string& pattern, size_t name_offset = 0,
      size_t pattern_offset = 0);

  static bool key_char_is_valid(char ch);
  static bool key_name_is_valid(const std::string& key_name);



  // task definitions

  class UpdateMetadataTask : public TypedStoreTask<std::unordered_map<std::string, Error>> {
  public:
    explicit UpdateMetadataTask(StoreTaskManager* m,
        std::shared_ptr<const UpdateMetadataArguments> args,
        BaseFunctionProfiler* profiler = NULL) : TypedStoreTask(m, profiler), args(args) { }
    explicit UpdateMetadataTask(std::unordered_map<std::string, Error>&& v) {
      this->set_value(std::move(v));
    }
    virtual ~UpdateMetadataTask() = default;
  protected:
    std::shared_ptr<const UpdateMetadataArguments> args;
  };

  class DeleteSeriesTask : public TypedStoreTask<std::unordered_map<std::string, DeleteResult>> {
  public:
    explicit DeleteSeriesTask(StoreTaskManager* m,
        std::shared_ptr<const DeleteSeriesArguments> args,
        BaseFunctionProfiler* profiler = NULL) : TypedStoreTask(m, profiler), args(args) { }
    explicit DeleteSeriesTask(std::unordered_map<std::string, DeleteResult>&& v) {
      this->set_value(std::move(v));
    }
    virtual ~DeleteSeriesTask() = default;
  protected:
    std::shared_ptr<const DeleteSeriesArguments> args;
  };

  class RenameSeriesTask : public TypedStoreTask<std::unordered_map<std::string, Error>> {
  public:
    explicit RenameSeriesTask(StoreTaskManager* m,
        std::shared_ptr<const RenameSeriesArguments> args,
        BaseFunctionProfiler* profiler = NULL) : TypedStoreTask(m, profiler), args(args) { }
    explicit RenameSeriesTask(std::unordered_map<std::string, Error>&& v) {
      this->set_value(std::move(v));
    }
    virtual ~RenameSeriesTask() = default;
  protected:
    std::shared_ptr<const RenameSeriesArguments> args;
  };

  class ReadTask : public TypedStoreTask<std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>> {
  public:
    explicit ReadTask(StoreTaskManager* m,
        std::shared_ptr<const ReadArguments> args,
        BaseFunctionProfiler* profiler = NULL) : TypedStoreTask(m, profiler), args(args) { }
    explicit ReadTask(std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>&& v) {
      this->set_value(std::move(v));
    }
    virtual ~ReadTask() = default;
  protected:
    std::shared_ptr<const ReadArguments> args;
  };

  class ReadAllTask : public TypedStoreTask<ReadAllResult> {
  public:
    explicit ReadAllTask(StoreTaskManager* m,
        std::shared_ptr<const ReadAllArguments> args,
        BaseFunctionProfiler* profiler = NULL) : TypedStoreTask(m, profiler), args(args) { }
    explicit ReadAllTask(ReadAllResult&& v) {
      this->set_value(std::move(v));
    }
    virtual ~ReadAllTask() = default;
  protected:
    std::shared_ptr<const ReadAllArguments> args;
  };

  class WriteTask : public TypedStoreTask<std::unordered_map<std::string, Error>> {
  public:
    explicit WriteTask(StoreTaskManager* m,
        std::shared_ptr<const WriteArguments> args,
        BaseFunctionProfiler* profiler = NULL) : TypedStoreTask(m, profiler), args(args) { }
    explicit WriteTask(std::unordered_map<std::string, Error>&& v) {
      this->set_value(std::move(v));
    }
    virtual ~WriteTask() = default;
  protected:
    std::shared_ptr<const WriteArguments> args;
  };

  class FindTask : public TypedStoreTask<std::unordered_map<std::string, FindResult>> {
  public:
    explicit FindTask(StoreTaskManager* m,
        std::shared_ptr<const FindArguments> args,
        BaseFunctionProfiler* profiler = NULL) : TypedStoreTask(m, profiler), args(args) { }
    explicit FindTask(std::unordered_map<std::string, FindResult>&& v) {
      this->set_value(std::move(v));
    }
    virtual ~FindTask() = default;
  protected:
    std::shared_ptr<const FindArguments> args;
  };

  class ResolvePatternsTask : public TypedStoreTask<std::unordered_map<std::string, std::vector<std::string>>> {
  public:
    ResolvePatternsTask(StoreTaskManager* m, Store* s,
        const std::vector<std::string>& key_names, bool local_only,
        BaseFunctionProfiler* profiler);
    virtual ~ResolvePatternsTask() = default;
    void on_find_complete();

  private:
    std::shared_ptr<FindTask> find_task;
  };

  class EmulateRenameSeriesTask : public TypedStoreTask<Error> {
  public:
    EmulateRenameSeriesTask(StoreTaskManager* m, Store* from_store,
        const std::string& from_key_name, Store* to_store,
        const std::string& to_key_name, bool merge,
        BaseFunctionProfiler* profiler);
    virtual ~EmulateRenameSeriesTask() = default;

    void on_read_all_complete();
    void on_update_metadata_complete();
    void on_write_complete();
    void on_delete_complete();

  private:
    Store* from_store;
    std::string from_key_name;
    Store* to_store;
    std::string to_key_name;
    bool merge;

    std::shared_ptr<ReadAllTask> read_all_task;
    std::shared_ptr<UpdateMetadataTask> update_metadata_task;
    std::shared_ptr<WriteTask> write_task;
    std::shared_ptr<DeleteSeriesTask> delete_task;
  };

protected:
  Store() = default;

  std::vector<std::pair<std::string, SeriesMetadata>> autocreate_rules;
  rw_lock autocreate_rules_lock;

  static void validate_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>> autocreate_rules);
  SeriesMetadata get_autocreate_metadata_for_key(const std::string& key_name);

  std::shared_ptr<ResolvePatternsTask> resolve_patterns(StoreTaskManager* m,
      const std::vector<std::string>& key_names, bool local_only,
      BaseFunctionProfiler* profiler);

  static std::shared_ptr<EmulateRenameSeriesTask> emulate_rename_series(
      StoreTaskManager* m, Store* from_store, const std::string& from_key_name,
      Store* to_store, const std::string& to_key_name, bool merge,
      BaseFunctionProfiler* profiler);

  struct Stats {
    std::atomic<uint64_t> start_time;
    std::atomic<uint64_t> duration;

    Stats();
    Stats& operator=(const Stats& other);

    std::unordered_map<std::string, int64_t> to_map() const;
  };

  static void combine_simple_results(
      std::unordered_map<std::string, Error>& into,
      std::unordered_map<std::string, Error>&& from);
  static void combine_delete_results(
      std::unordered_map<std::string, DeleteResult>& into,
      std::unordered_map<std::string, DeleteResult>&& from);
  static void combine_read_results(
      std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>& into,
      std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>&& from);
  static void combine_find_results(
      std::unordered_map<std::string, FindResult>& into,
      std::unordered_map<std::string, FindResult>&& from);
};
