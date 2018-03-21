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

using namespace std;


unordered_map<string, string> EmptyStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior) {

  unordered_map<string, string> ret;
  for (const auto& it : metadata) {
    ret.emplace(it.first, "");
  }
  return ret;
}

unordered_map<string, string> EmptyStore::delete_series(
    const vector<string>& key_names) {

  unordered_map<string, string> ret;
  for (const auto& it : key_names) {
    ret.emplace(it, "");
  }
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> EmptyStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {

  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : key_names) {
    ret.emplace(piecewise_construct, forward_as_tuple(it), forward_as_tuple());
  }
  return ret;
}

unordered_map<string, string> EmptyStore::write(
    const unordered_map<string, Series>& data) {

  unordered_map<string, string> ret;
  for (const auto& it : data) {
    ret.emplace(it.first, "");
  }
  return ret;
}

unordered_map<string, FindResult> EmptyStore::find(
    const vector<string>& patterns) {

  unordered_map<string, FindResult> ret;
  for (const auto& it : patterns) {
    ret.emplace(piecewise_construct, forward_as_tuple(it), forward_as_tuple());
  }
  return ret;
}

string EmptyStore::str() const {
  return "EmptyStore";
}
