#include "ReadOnlyStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <phosg/Strings.hh>
#include <stdexcept>
#include <string>
#include <vector>

#include "Utils/Errors.hh"

using namespace std;


ReadOnlyStore::ReadOnlyStore(shared_ptr<Store> store) : Store(), store(store) { }

shared_ptr<Store> ReadOnlyStore::get_substore() const {
  return this->store;
}

shared_ptr<Store::UpdateMetadataTask> ReadOnlyStore::update_metadata(
    StoreTaskManager* m, shared_ptr<const UpdateMetadataArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : args->metadata) {
    ret.emplace(it.first, make_error("writes not allowed"));
  }
  return shared_ptr<UpdateMetadataTask>(new UpdateMetadataTask(move(ret)));
}

shared_ptr<Store::DeleteSeriesTask> ReadOnlyStore::delete_series(
    StoreTaskManager* m, shared_ptr<const DeleteSeriesArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, DeleteResult> ret;
  for (const auto& pattern : args->patterns) {
    auto& res = ret[pattern];
    res.disk_series_deleted = 0;
    res.buffer_series_deleted = 0;
    res.error = make_error("writes not allowed");
  }
  return shared_ptr<DeleteSeriesTask>(new DeleteSeriesTask(move(ret)));
}

shared_ptr<Store::RenameSeriesTask> ReadOnlyStore::rename_series(
    StoreTaskManager* m, shared_ptr<const RenameSeriesArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : args->renames) {
    ret.emplace(it.first, make_error("writes not allowed"));
  }
  return shared_ptr<RenameSeriesTask>(new RenameSeriesTask(move(ret)));
}

shared_ptr<Store::ReadTask> ReadOnlyStore::read(
    StoreTaskManager* m, shared_ptr<const ReadArguments> args,
    BaseFunctionProfiler* profiler) {
  return this->store->read(m, args, profiler);
}

shared_ptr<Store::ReadAllTask> ReadOnlyStore::read_all(
    StoreTaskManager* m, shared_ptr<const ReadAllArguments> args,
    BaseFunctionProfiler* profiler) {
  return this->store->read_all(m, args, profiler);
}

shared_ptr<Store::WriteTask> ReadOnlyStore::write(
    StoreTaskManager* m, shared_ptr<const WriteArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : args->data) {
    ret.emplace(it.first, make_error("writes not allowed"));
  }
  return shared_ptr<WriteTask>(new WriteTask(move(ret)));
}

shared_ptr<Store::FindTask> ReadOnlyStore::find(
    StoreTaskManager* m, shared_ptr<const FindArguments> args,
    BaseFunctionProfiler* profiler) {
  return this->store->find(m, args, profiler);
}

unordered_map<string, int64_t> ReadOnlyStore::get_stats(bool rotate) {
  return this->store->get_stats(rotate);
}

string ReadOnlyStore::str() const {
  return "ReadOnlyStore(" + this->store->str() + ")";
}
