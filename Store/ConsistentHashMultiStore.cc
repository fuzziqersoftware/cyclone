#include "ConsistentHashMultiStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <phosg/Filesystem.hh>
#include <phosg/Strings.hh>
#include <stdexcept>
#include <string>
#include <vector>

#include "CarbonConsistentHashRing.hh"

using namespace std;


ConsistentHashMultiStore::ConsistentHashMultiStore(
    const unordered_map<string, shared_ptr<Store>>& stores, int64_t precision) :
    MultiStore(stores) {
  vector<ConsistentHashRing::Host> hosts;
  for (const auto& it : this->stores) {
    hosts.emplace_back(it.first, "", 0); // host/port are ignored
  }
  if (precision > 0) {
    this->ring = shared_ptr<ConsistentHashRing>(
        new ConstantTimeConsistentHashRing(hosts, precision));
  } else if (precision < 0) {
    this->ring = shared_ptr<ConsistentHashRing>(
        new CarbonConsistentHashRing(hosts, -precision));
  } else {
    throw invalid_argument("precision must be nonzero");
  }
}

unordered_map<string, string> ConsistentHashMultiStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior) {
  unordered_map<uint8_t, unordered_map<string, SeriesMetadata>> partitioned_data;

  for (const auto& it : metadata) {
    uint8_t store_id = this->ring->host_id_for_key(it.first);
    partitioned_data[store_id].emplace(it.first, it.second);
  }

  unordered_map<string, string> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->update_metadata(it.second,
        create_new, update_behavior);
    this->merge_maps(ret, move(results), this->combine_error_strings);
  }
  return ret;
}

unordered_map<string, string> ConsistentHashMultiStore::delete_series(
    const vector<string>& key_names) {

  unordered_map<uint8_t, vector<string>> partitioned_data;
  for (size_t x = 0; x < key_names.size(); x++) {
    uint8_t store_id = this->ring->host_id_for_key(key_names[x]);
    partitioned_data[store_id].push_back(key_names[x]);
  }

  unordered_map<string, string> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->delete_series(it.second);
    this->merge_maps(ret, move(results), this->combine_error_strings);
  }
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> ConsistentHashMultiStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {

  unordered_map<size_t, vector<string>> partitioned_data;
  for (size_t x = 0; x < key_names.size(); x++) {
    if (this->token_is_pattern(key_names[x])) {
      size_t num_hosts = this->ring->all_hosts().size();
      for (size_t store_id = 0; store_id < num_hosts; store_id++) {
        partitioned_data[store_id].push_back(key_names[x]);
      }
    } else {
      size_t store_id = this->ring->host_id_for_key(key_names[x]);
      partitioned_data[store_id].push_back(key_names[x]);
    }
  }

  // TODO: we should find some way to make the reads happen in parallel
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->read(it.second, start_time, end_time);
    this->merge_maps(ret, move(results), this->combine_read_pattern_maps);
  }
  return ret;
}

unordered_map<string, string> ConsistentHashMultiStore::write(
    const unordered_map<string, Series>& data) {

  unordered_map<uint8_t, unordered_map<string, Series>> partitioned_data;
  for (const auto& it : data) {
    uint8_t store_id = this->ring->host_id_for_key(it.first);
    partitioned_data[store_id].emplace(it.first, it.second);
  }

  unordered_map<string, string> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->write(it.second);
    this->merge_maps(ret, move(results), this->combine_error_strings);
  }
  return ret;
}
