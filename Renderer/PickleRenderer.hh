#pragma once

#include <event2/buffer.h>

#include <unordered_map>
#include <vector>

#include "../gen-cpp/Cyclone.h"
#include "Renderer.hh"


class PickleRenderer : public Renderer {
public:
  PickleRenderer(struct evbuffer* buf);
  PickleRenderer(const PickleRenderer& rhs) = delete;
  const PickleRenderer& operator=(const PickleRenderer& rhs) = delete;
  virtual ~PickleRenderer() = default;

  virtual const char* content_type() const;
  virtual void render_data(
      const std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>& data,
      int64_t start_time, int64_t end_time) const;
  virtual void render_find_results(const std::unordered_map<std::string, FindResult>& data) const;
};
