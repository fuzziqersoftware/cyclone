#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <deque>

#include <thrift/transport/TSocket.h>

#include "../gen-cpp/Cyclone.h"
#include "Store.hh"
#include "Utils/FixedAtomicRotator.hh"

#ifdef _THRIFT_STDCXX_H_
#define thrift_ptr apache::thrift::stdcxx::shared_ptr
#else
#define thrift_ptr boost::shared_ptr
#endif


class RemoteStore : public Store {
public:
  RemoteStore() = delete;
  RemoteStore(const RemoteStore& rhs) = delete;
  RemoteStore(const std::string& hostname, int port, size_t connection_limit);
  virtual ~RemoteStore() = default;
  const RemoteStore& operator=(const RemoteStore& rhs) = delete;

  size_t get_connection_limit() const;
  int get_port() const;
  std::string get_hostname() const;
  void set_connection_limit(size_t new_value);
  void set_netloc(const std::string& new_hostname, int new_port);

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
  struct Client {
    thrift_ptr<apache::thrift::transport::TSocket> socket;
    std::shared_ptr<CycloneClient> client;
    int64_t netloc_token;
  };

  std::mutex clients_lock;
  std::deque<std::unique_ptr<Client>> clients;
  std::atomic<int64_t> netloc_token;

  std::unique_ptr<Client> get_client();
  void return_client(std::unique_ptr<Client>&& client);

  std::string hostname;
  int port;
  std::atomic<size_t> connection_limit;

  struct Stats : public Store::Stats {
    std::atomic<size_t> connects;
    std::atomic<size_t> server_disconnects;
    std::atomic<size_t> client_disconnects;
    std::atomic<size_t> update_metadata_commands;
    std::atomic<size_t> delete_series_commands;
    std::atomic<size_t> rename_series_commands;
    std::atomic<size_t> read_commands;
    std::atomic<size_t> read_all_commands;
    std::atomic<size_t> write_commands;
    std::atomic<size_t> find_commands;
    std::atomic<size_t> restore_series_commands;
    std::atomic<size_t> serialize_series_commands;
    std::atomic<size_t> delete_pending_writes_commands;

    Stats();
    Stats& operator=(const Stats& other);

    std::unordered_map<std::string, int64_t> to_map() const;
  };

  FixedAtomicRotator<Stats> stats;

  class RemoteStoreUpdateMetadataTask;
  class RemoteStoreDeleteSeriesTask;
  class RemoteStoreRenameSeriesTask;
  class RemoteStoreReadTask;
  class RemoteStoreReadAllTask;
  class RemoteStoreWriteTask;
  class RemoteStoreFindTask;
};
