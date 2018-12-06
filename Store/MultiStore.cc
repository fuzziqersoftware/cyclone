#include "MultiStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <phosg/Strings.hh>
#include <stdexcept>
#include <string>
#include <vector>

#include "Utils/Errors.hh"

using namespace std;


MultiStore::MultiStore(const unordered_map<string, shared_ptr<Store>>& stores) :
    Store(), stores(stores) { }

unordered_map<string, shared_ptr<Store>> MultiStore::get_substores() const {
  return this->stores;
}

void MultiStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>>& autocreate_rules) {
  this->Store::set_autocreate_rules(autocreate_rules);
  for (const auto& it : this->stores) {
    it.second->set_autocreate_rules(autocreate_rules);
  }
}

unordered_map<string, Error> MultiStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {

  unordered_map<string, Error> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->update_metadata(metadata, create_new,
        update_behavior, skip_buffering, local_only, profiler);
    this->combine_simple_results(ret, move(results));
  }
  return ret;
}

unordered_map<string, DeleteResult> MultiStore::delete_series(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {

  unordered_map<string, DeleteResult> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->delete_series(patterns, local_only, profiler);
    this->combine_delete_results(ret, move(results));
  }
  return ret;
}

unordered_map<string, Error> MultiStore::rename_series(
    const unordered_map<string, string>& renames, bool merge, bool local_only,
    BaseFunctionProfiler* profiler) {

  unordered_map<string, Error> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->rename_series(renames, merge, local_only, profiler);
    this->combine_simple_results(ret, move(results));
  }
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> MultiStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only, BaseFunctionProfiler* profiler) {
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->read(key_names, start_time, end_time, local_only,
        profiler);
    this->combine_read_results(ret, move(results));
  }
  return ret;
}

ReadAllResult MultiStore::read_all(const string& key_name, bool local_only,
    BaseFunctionProfiler* profiler) {
  ReadAllResult ret;
  for (const auto& it : this->stores) {
    auto result = it.second->read_all(key_name, local_only, profiler);
    if (result.metadata.archive_args.empty()) {
      continue;
    }
    if (!ret.metadata.archive_args.empty()) {
      ret.error = make_error("multiple stores returned nonempty results");
    } else {
      ret = move(result);
    }
  }
  return ret;
}

unordered_map<string, Error> MultiStore::write(
    const unordered_map<string, Series>& data, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->write(data, skip_buffering, local_only, profiler);
    this->combine_simple_results(ret, move(results));
  }
  return ret;
}

unordered_map<string, FindResult> MultiStore::find(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, FindResult> ret;
  for (const auto it : this->stores) {
    auto results = it.second->find(patterns, local_only, profiler);
    this->combine_find_results(ret, move(results));
  }
  return ret;
}

unordered_map<string, int64_t> MultiStore::get_stats(bool rotate) {
  unordered_map<string, int64_t> ret;
  for (const auto& it : this->stores) {
    // replace any invalid characters in the store name
    string store_name;
    for (char ch : it.first) {
      if (!this->key_char_is_valid(ch)) {
        ch = '_';
      }
      store_name.push_back(ch);
    }

    for (const auto& stat : it.second->get_stats(rotate)) {
      ret.emplace(string_printf("%s:%s", store_name.c_str(),
          stat.first.c_str()), stat.second);
    }
  }
  return ret;
}

int64_t MultiStore::delete_pending_writes(const std::string& pattern, bool local_only) {
  int64_t ret = 0;
  for (const auto& it : this->stores) {
    ret += it.second->delete_pending_writes(pattern, local_only);
  }
  return ret;
}

string MultiStore::str() const {
  string ret = "MultiStore(";
  for (const auto& it : this->stores) {
    if (ret.size() > 11) {
      ret += ", ";
    }
    ret += it.first;
    ret += '=';
    ret += it.second->str();
  }
  ret += ')';
  return ret;
}
