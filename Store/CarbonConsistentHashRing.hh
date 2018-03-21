#pragma once

#include <stdint.h>

#include <map>
#include <phosg/ConsistentHashRing.hh>
#include <unordered_set>
#include <string>


class CarbonConsistentHashRing : public ConsistentHashRing {
public:
  CarbonConsistentHashRing() = delete;
  explicit CarbonConsistentHashRing(const std::vector<Host>& hosts,
      size_t replica_count = 100);
  virtual ~CarbonConsistentHashRing() = default;

  virtual uint64_t host_id_for_key(const void* key, int64_t size) const;

private:
  static uint16_t compute_ring_position(const void* data, size_t size);

  std::map<uint16_t, uint64_t> ring;
};
