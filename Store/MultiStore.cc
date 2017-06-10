#include "MultiStore.hh"

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

using namespace std;


MultiStore::MultiStore(const unordered_map<string, shared_ptr<Store>>& stores) :
    Store(), stores(stores) { }

void MultiStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>> autocreate_rules) {
  this->validate_autocreate_rules(autocreate_rules);
  for (const auto& it : this->stores) {
    it.second->set_autocreate_rules(autocreate_rules);
  }
}

unordered_map<string, string> MultiStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior) {

  unordered_map<string, string> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->update_metadata(metadata, create_new,
        update_behavior);
    this->merge_maps(ret, move(results), this->combine_error_strings);
  }
  return ret;
}

unordered_map<string, string> MultiStore::delete_series(
    const vector<string>& key_names) {

  unordered_map<string, string> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->delete_series(key_names);
    this->merge_maps(ret, move(results), this->combine_error_strings);
  }
  return ret;
}

unordered_map<string, ReadResult> MultiStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {
  unordered_map<string, ReadResult> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->read(key_names, start_time, end_time);
    this->merge_maps(ret, move(results), this->combine_read_results);
  }
  return ret;
}

unordered_map<string, string> MultiStore::write(
    const unordered_map<string, Series>& data) {
  unordered_map<string, string> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->write(data);
    this->merge_maps(ret, move(results), this->combine_error_strings);
  }
  return ret;
}

unordered_map<string, FindResult> MultiStore::find(
    const vector<string>& patterns) {
  unordered_map<string, FindResult> ret;
  for (const auto it : this->stores) {
    auto results = it.second->find(patterns);
    this->merge_maps(ret, move(results), this->combine_find_results);
  }
  return ret;
}

unordered_map<string, int64_t> MultiStore::get_stats(bool rotate) {
  unordered_map<string, int64_t> ret;
  for (const auto& it : this->stores) {
    for (const auto& stat : it.second->get_stats(rotate)) {
      ret.emplace(string_printf("%s:%s", it.first.c_str(), stat.first.c_str()),
          stat.second);
    }
  }
  return ret;
}

int64_t MultiStore::delete_from_cache(const std::string& path) {
  int64_t ret = 0;
  for (const auto& it : this->stores) {
    ret += it.second->delete_from_cache(path);
  }
  return ret;
}

int64_t MultiStore::delete_pending_writes(const std::string& pattern) {
  int64_t ret = 0;
  for (const auto& it : this->stores) {
    ret += it.second->delete_pending_writes(pattern);
  }
  return ret;
}


void MultiStore::combine_error_strings(const string& k, string& v1,
    string& v2) {
  if (!v1.empty() && !v2.empty()) {
    v1 += " && ";
    v1 += move(v2);

  } else if (!v2.empty()) {
    v1 = move(v2);
  }
}

void MultiStore::combine_read_results(const string& k, ReadResult& v1,
    ReadResult& v2) {
  // currently we just keep the first nonempty result
  if (v1.data.empty() && !v2.data.empty()) {
    v1 = move(v2);
  }
}

void MultiStore::combine_find_results(const string& k, FindResult& v1,
    FindResult& v2) {
  v1.results.insert(v1.results.end(), make_move_iterator(v2.results.begin()),
      make_move_iterator(v2.results.end()));
  MultiStore::combine_error_strings(k, v1.error, v2.error);
}
