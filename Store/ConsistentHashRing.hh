#pragma once

#include <stdint.h>

#include <vector>
#include <unordered_map>



uint64_t fnv1a64(const void* data, size_t size,
    uint64_t hash = 0xCBF29CE484222325);


class ConsistentHashRing {
public:
  ConsistentHashRing() = delete;
  ConsistentHashRing(const ConsistentHashRing& other) = default;
  ConsistentHashRing(ConsistentHashRing&& other) = default;
  ConsistentHashRing(const std::vector<std::string>& hosts,
      uint8_t precision = 17);
  ConsistentHashRing(const std::unordered_map<std::string, std::string>& hosts,
      uint8_t precision = 17);
  virtual ~ConsistentHashRing() = default;

  uint8_t server_id_for_key(const std::string& key) const;
  uint8_t server_id_for_key(const void* key, int64_t size) const;
  const std::string& server_name_for_key(const std::string& key) const;
  const std::string& server_name_for_key(const void* key, int64_t size) const;
  const std::string& server_name_for_id(uint8_t id) const;

  const std::vector<uint8_t>& all_points() const;
  const std::vector<std::string>& all_server_names() const;

protected:
  void populate(const std::unordered_map<std::string, std::string>& hosts,
      uint8_t precision);

  std::vector<uint8_t> points;
  std::vector<std::string> hosts;
};
