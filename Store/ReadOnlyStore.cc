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

using namespace std;


ReadOnlyStore::ReadOnlyStore(shared_ptr<Store> store) : Store(), store(store) { }

unordered_map<string, string> ReadOnlyStore::update_metadata(
      const SeriesMetadataMap& metadata_map, bool create_new,
      UpdateMetadataBehavior update_behavior) {
  unordered_map<string, string> ret;
  for (const auto& it : metadata_map) {
    ret.emplace(it.first, "writes not allowed");
  }
  return ret;
}

unordered_map<string, string> ReadOnlyStore::delete_series(
    const vector<string>& key_names) {
  unordered_map<string, string> ret;
  for (const auto& it : key_names) {
    ret.emplace(it, "writes not allowed");
  }
  return ret;
}

unordered_map<string, ReadResult> ReadOnlyStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {
  return this->store->read(key_names, start_time, end_time);
}

unordered_map<string, string> ReadOnlyStore::write(
    const unordered_map<string, Series>& data) {
  unordered_map<string, string> ret;
  for (const auto& it : data) {
    ret.emplace(it.first, "writes not allowed");
  }
  return ret;
}

unordered_map<string, FindResult> ReadOnlyStore::find(
    const vector<string>& patterns) {
  return this->store->find(patterns);
}

unordered_map<string, int64_t> ReadOnlyStore::get_stats(bool rotate) {
  return this->store->get_stats(rotate);
}

int64_t ReadOnlyStore::delete_from_cache(const std::string& path) {
  return this->store->delete_from_cache(path);
}
