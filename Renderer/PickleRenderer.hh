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

  const char* content_type() const;
  virtual void render_data(const std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>& data) const;
  virtual void render_find_results(const std::unordered_map<std::string, FindResult>& data) const;
};
