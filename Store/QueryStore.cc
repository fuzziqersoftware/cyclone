#include "QueryStore.hh"

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

#include "QueryParser.hh"

using namespace std;


QueryStore::QueryStore(shared_ptr<Store> store) : Store(), store(store) { }

void QueryStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>> autocreate_rules) {
  this->store->set_autocreate_rules(autocreate_rules);
}

unordered_map<string, string> QueryStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior) {
  return this->store->update_metadata(metadata, create_new, update_behavior);
}

unordered_map<string, string> QueryStore::delete_series(
    const vector<string>& key_names) {
  return this->store->delete_series(key_names);
}

unordered_map<string, unordered_map<string, ReadResult>> QueryStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {

  for (const auto& query : key_names) {
    fprintf(stderr, "[QueryStore:%s] lexing query\n", query.c_str());
    auto tokens = tokenize_query(query);
    for (const auto& token : tokens) {
      string token_str = token.str();
      fprintf(stderr, "[QueryStore:%s] token: %s\n", query.c_str(), token_str.c_str());
    }
  }

  // TODO
  return this->store->read(key_names, start_time, end_time);
}

unordered_map<string, string> QueryStore::write(
    const unordered_map<string, Series>& data) {
  return this->store->write(data);
}

unordered_map<string, FindResult> QueryStore::find(
    const vector<string>& patterns) {
  return this->store->find(patterns);
}

unordered_map<string, int64_t> QueryStore::get_stats(bool rotate) {
  return this->store->get_stats();
}

int64_t QueryStore::delete_from_cache(const std::string& path) {
  return this->store->delete_from_cache(path);
}

int64_t QueryStore::delete_pending_writes(const std::string& pattern) {
  return this->store->delete_pending_writes(pattern);
}

string QueryStore::str() const {
  return "QueryStore(" + this->store->str() + ")";
}
