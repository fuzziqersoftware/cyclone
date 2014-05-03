#include "ConsistentHashRing.hh"

#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;


uint64_t fnv1a64(const void* data, size_t size, uint64_t hash) {
  const uint8_t *data_ptr = (const uint8_t*)data;
  const uint8_t *end_ptr = data_ptr + size;

  for (; data_ptr != end_ptr; data_ptr++) {
    hash = (hash ^ (uint64_t)*data_ptr) * 0x00000100000001B3;
  }
  return hash;
}


#define POINTS_PER_HOST 256 // must be divisible by 4

ConsistentHashRing::ConsistentHashRing(const vector<string>& hosts,
    uint8_t precision) {
  unordered_map<string, string> hosts_map;
  for (const auto& host : hosts) {
    hosts_map[host] = host;
  }
  this->populate(hosts_map, precision);
}

ConsistentHashRing::ConsistentHashRing(
    const unordered_map<string, string>& hosts, uint8_t precision) {
  this->populate(hosts, precision);
}

uint8_t ConsistentHashRing::server_id_for_key(const string& s) const {
  return this->server_id_for_key(s.data(), s.size());
}

uint8_t ConsistentHashRing::server_id_for_key(const void* key, int64_t size) const {
  uint16_t z = 0;
  uint64_t h = fnv1a64(key, size, fnv1a64(&z, sizeof(uint16_t)));
  return this->points[h & (this->points.size() - 1)];
}

const string& ConsistentHashRing::server_name_for_key(const string& s) const {
  return this->server_name_for_key(s.data(), s.size());
}

const string& ConsistentHashRing::server_name_for_key(const void* key, int64_t size) const {
  return this->server_name_for_id(this->server_id_for_key(key, size));
}

const string& ConsistentHashRing::server_name_for_id(uint8_t id) const {
  return this->hosts[id];
}

const vector<uint8_t>& ConsistentHashRing::all_points() const {
  return this->points;
}

const vector<string>& ConsistentHashRing::all_server_names() const {
  return this->hosts;
}

void ConsistentHashRing::populate(const unordered_map<string, string>& hosts,
    uint8_t precision) {

  if (hosts.size() > 254) {
    throw runtime_error("too many hosts");
  }

  this->points.clear();
  this->points.resize(1 << precision, 0xFF);

  size_t index_mask = this->points.size() - 1;
  set<pair<string, string>> hosts_ordered(hosts.begin(), hosts.end());
  for (const auto & it : hosts_ordered) {
    uint8_t host_id = this->hosts.size();
    for (uint16_t y = 0; y < POINTS_PER_HOST; y++) {
      uint64_t h = fnv1a64(it.first.data(), it.first.size(), fnv1a64(&y, sizeof(uint16_t)));
      this->points[h & index_mask] = host_id;
    }
    this->hosts.push_back(it.second);
  }

  uint8_t current_host = 0xFF;
  for (size_t index = this->points.size() - 1;
       (index > 0) && (current_host == 0xFF); index--) {
    if (this->points[index] != 0xFF) {
      current_host = this->points[index];
    }
  }
  for (auto& pt : this->points) {
    if (pt == 0xFF) {
      pt = current_host;
    } else {
      current_host = pt;
    }
  }
}
