#include "EmptyStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "Utils/Errors.hh"

using namespace std;


shared_ptr<Store::UpdateMetadataTask> EmptyStore::update_metadata(
    StoreTaskManager*, shared_ptr<const UpdateMetadataArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : args->metadata) {
    ret.emplace(it.first, make_ignored());
  }
  return shared_ptr<UpdateMetadataTask>(new UpdateMetadataTask(move(ret)));
}

shared_ptr<Store::DeleteSeriesTask> EmptyStore::delete_series(
    StoreTaskManager*, shared_ptr<const DeleteSeriesArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, DeleteResult> ret;
  for (const auto& pattern : args->patterns) {
    auto& res = ret[pattern];
    res.disk_series_deleted = 0;
    res.buffer_series_deleted = 0;
    res.error = make_success();
  }
  return shared_ptr<DeleteSeriesTask>(new DeleteSeriesTask(move(ret)));
}

shared_ptr<Store::RenameSeriesTask> EmptyStore::rename_series(
    StoreTaskManager*, shared_ptr<const RenameSeriesArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : args->renames) {
    ret.emplace(it.first, make_error("series does not exist"));
  }
  return shared_ptr<RenameSeriesTask>(new RenameSeriesTask(move(ret)));
}

shared_ptr<Store::ReadTask> EmptyStore::read(
    StoreTaskManager*, shared_ptr<const ReadArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : args->key_names) {
    ret.emplace(piecewise_construct, forward_as_tuple(it), forward_as_tuple());
  }
  return shared_ptr<ReadTask>(new ReadTask(move(ret)));
}

shared_ptr<Store::ReadAllTask> EmptyStore::read_all(StoreTaskManager*,
    shared_ptr<const ReadAllArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<ReadAllTask>(new ReadAllTask(ReadAllResult()));
}

shared_ptr<Store::WriteTask> EmptyStore::write(
    StoreTaskManager*, shared_ptr<const WriteArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : args->data) {
    ret.emplace(it.first, make_success());
  }
  return shared_ptr<WriteTask>(new WriteTask(move(ret)));
}

shared_ptr<Store::FindTask> EmptyStore::find(
    StoreTaskManager*, shared_ptr<const FindArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, FindResult> ret;
  for (const auto& it : args->patterns) {
    ret.emplace(piecewise_construct, forward_as_tuple(it), forward_as_tuple());
  }
  return shared_ptr<FindTask>(new FindTask(move(ret)));
}

string EmptyStore::str() const {
  return "EmptyStore";
}
