#pragma once

#include <event2/buffer.h>
#include <stdint.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "../gen-cpp/Cyclone.h"


class Renderer {
public:
  Renderer(const Renderer& rhs) = delete;
  const Renderer& operator=(const Renderer& rhs) = delete;
  virtual ~Renderer() = default;

  virtual const char* content_type() const = 0;
  virtual void render_data(
        const std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>& data,
        int64_t start_time, int64_t end_time) const = 0;
  virtual void render_find_results(const std::unordered_map<std::string, FindResult>& data) const = 0;

protected:
  struct evbuffer* buf;

  Renderer(struct evbuffer* buf);
};
