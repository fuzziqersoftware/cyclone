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

unordered_map<string, Error> ReadOnlyStore::update_metadata(
      const SeriesMetadataMap& metadata_map, bool create_new,
      UpdateMetadataBehavior update_behavior, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : metadata_map) {
    ret.emplace(it.first, make_error("writes not allowed"));
  }
  return ret;
}

unordered_map<string, DeleteResult> ReadOnlyStore::delete_series(
    const vector<string>& patterns, bool deferred, bool local_only,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, DeleteResult> ret;
  for (const auto& pattern : patterns) {
    auto& res = ret[pattern];
    res.disk_series_deleted = 0;
    res.buffer_series_deleted = 0;
    res.error = make_error("writes not allowed");
  }
  return ret;
}

unordered_map<string, Error> ReadOnlyStore::rename_series(
    const unordered_map<string, string>& renames, bool merge, bool local_only,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : renames) {
    ret.emplace(it.first, make_error("writes not allowed"));
  }
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> ReadOnlyStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only, BaseFunctionProfiler* profiler) {
  return this->store->read(key_names, start_time, end_time, local_only,
      profiler);
}

ReadAllResult ReadOnlyStore::read_all(const string& key_name, bool local_only,
    BaseFunctionProfiler* profiler) {
  return this->store->read_all(key_name, local_only, profiler);
}

unordered_map<string, Error> ReadOnlyStore::write(
    const unordered_map<string, Series>& data, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : data) {
    ret.emplace(it.first, make_error("writes not allowed"));
  }
  return ret;
}

unordered_map<string, FindResult> ReadOnlyStore::find(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  return this->store->find(patterns, local_only, profiler);
}

unordered_map<string, int64_t> ReadOnlyStore::get_stats(bool rotate) {
  return this->store->get_stats(rotate);
}

string ReadOnlyStore::str() const {
  return "ReadOnlyStore(" + this->store->str() + ")";
}
