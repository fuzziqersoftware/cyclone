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


unordered_map<string, Error> EmptyStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {

  unordered_map<string, Error> ret;
  for (const auto& it : metadata) {
    ret.emplace(it.first, make_success());
  }
  return ret;
}

unordered_map<string, DeleteResult> EmptyStore::delete_series(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, DeleteResult> ret;
  for (const auto& pattern : patterns) {
    auto& res = ret[pattern];
    res.disk_series_deleted = 0;
    res.buffer_series_deleted = 0;
    res.error = make_success();
  }
  return ret;
}

unordered_map<string, Error> EmptyStore::rename_series(
    const unordered_map<string, string>& renames, bool local_only,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : renames) {
    ret.emplace(it.first, make_error("series does not exist"));
  }
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> EmptyStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only, BaseFunctionProfiler* profiler) {

  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : key_names) {
    ret.emplace(piecewise_construct, forward_as_tuple(it), forward_as_tuple());
  }
  return ret;
}

ReadAllResult EmptyStore::read_all(const string& key_name, bool local_only,
    BaseFunctionProfiler* profiler) {
  return ReadAllResult();
}

unordered_map<string, Error> EmptyStore::write(
    const unordered_map<string, Series>& data, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {

  unordered_map<string, Error> ret;
  for (const auto& it : data) {
    ret.emplace(it.first, make_success());
  }
  return ret;
}

unordered_map<string, FindResult> EmptyStore::find(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {

  unordered_map<string, FindResult> ret;
  for (const auto& it : patterns) {
    ret.emplace(piecewise_construct, forward_as_tuple(it), forward_as_tuple());
  }
  return ret;
}

string EmptyStore::str() const {
  return "EmptyStore";
}
