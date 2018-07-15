#include "CarbonConsistentHashRing.hh"

#include <stdint.h>

#include <map>
#include <phosg/Encoding.hh>
#include <phosg/Hash.hh>
#include <phosg/Strings.hh>
#include <unordered_set>
#include <string>

using namespace std;


CarbonConsistentHashRing::CarbonConsistentHashRing(
    const vector<Host>& hosts, size_t replica_count) :
    ConsistentHashRing(hosts) {

  for (size_t x = 0; x < this->hosts.size(); x++) {
    const Host& host = this->hosts[x];
    string node_key = string_printf("('%s', '%s')", host.host.c_str(),
        host.name.c_str());

    for (size_t y = 0; y < replica_count; y++) {
      string replica_key = string_printf("%s:%zu", node_key.c_str(), y);
      uint16_t position = this->compute_ring_position(replica_key.data(),
          replica_key.size());
      this->ring.emplace(position, x);
    }
  }
}

uint16_t CarbonConsistentHashRing::compute_ring_position(const void* data,
    size_t size) {
  string h = md5(data, size);
  return bswap16(*reinterpret_cast<const uint16_t*>(h.data()));
}

uint64_t CarbonConsistentHashRing::host_id_for_key(const void* key,
    int64_t size) const {
  if (this->ring.empty()) {
    throw invalid_argument("ring contains no nodes");
  }

  uint16_t position = this->compute_ring_position(key, size);
  auto it = this->ring.lower_bound(position);
  if (it == this->ring.end()) {
    return this->ring.begin()->second;
  }
  return it->second;
}
