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
    Store(), stores(stores) {
  vector<string> names;
  for (const auto& it : this->stores) {
    names.push_back(it.first);
  }
  this->ring = shared_ptr<ConsistentHashRing>(new ConsistentHashRing(names));
}

void MultiStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>> autocreate_rules) {
  for (const auto& it : this->stores) {
    it.second->set_autocreate_rules(autocreate_rules);
  }
}

unordered_map<string, string> MultiStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior) {
  unordered_map<uint8_t, unordered_map<string, SeriesMetadata>> partitioned_data;

  for (const auto& it : metadata) {
    uint8_t store_id = this->ring->server_id_for_key(it.first);
    partitioned_data[store_id].emplace(it.first, it.second);
  }

  unordered_map<string, string> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->server_name_for_id(it.first);
    auto results = this->stores[store_name]->update_metadata(it.second,
        create_new, update_behavior);
    for (auto& result : results) {
      ret.emplace(move(result.first), move(result.second));
    }
  }
  return ret;
}

unordered_map<string, string> MultiStore::delete_series(
    const vector<string>& key_names) {

  unordered_map<uint8_t, vector<string>> partitioned_data;
  for (size_t x = 0; x < key_names.size(); x++) {
    uint8_t store_id = this->ring->server_id_for_key(key_names[x]);
    partitioned_data[store_id].push_back(key_names[x]);
  }

  unordered_map<string, string> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->server_name_for_id(it.first);
    auto results = this->stores[store_name]->delete_series(it.second);
    for (auto& result : results) {
      ret.emplace(move(result.first), move(result.second));
    }
  }
  return ret;
}

unordered_map<string, ReadResult> MultiStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {
  unordered_map<uint8_t, vector<string>> partitioned_data;
  for (size_t x = 0; x < key_names.size(); x++) {
    uint8_t store_id = this->ring->server_id_for_key(key_names[x]);
    partitioned_data[store_id].push_back(key_names[x]);
  }

  unordered_map<string, ReadResult> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->server_name_for_id(it.first);
    auto this_ret = this->stores[store_name]->read(it.second, start_time, end_time);
    for (auto& it : this_ret) {
      ret.emplace(it.first, move(it.second));
    }
  }
  return ret;
}

unordered_map<string, string> MultiStore::write(
    const unordered_map<string, Series>& data) {

  unordered_map<uint8_t, unordered_map<string, Series>> partitioned_data;
  for (const auto& it : data) {
    uint8_t store_id = this->ring->server_id_for_key(it.first);
    partitioned_data[store_id].emplace(it.first, it.second);
  }

  unordered_map<string, string> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->server_name_for_id(it.first);
    auto results = this->stores[store_name]->write(it.second);
    for (auto& result : results) {
      ret.emplace(move(result.first), move(result.second));
    }
  }
  return ret;
}

unordered_map<string, FindResult> MultiStore::find(
    const vector<string>& patterns) {
  unordered_map<string, FindResult> ret;
  for (const auto it : this->stores) {
    for (auto& it2 : it.second->find(patterns)) {
      auto& pattern = it2.first;
      auto& result = it2.second;

      auto& ret_result = ret[pattern];
      ret_result.results.insert(ret_result.results.end(),
          make_move_iterator(result.results.begin()),
          make_move_iterator(result.results.end()));
      if (!result.error.empty()) {
        ret_result.error = move(result.error);
      }
    }
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
