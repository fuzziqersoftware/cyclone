#pragma once

#include <event2/buffer.h>

#include <unordered_map>
#include <vector>

#include "../gen-cpp/Cyclone.h"
#include "Renderer.hh"


class JSONRenderer : public Renderer {
public:
  JSONRenderer(struct evbuffer* buf);
  JSONRenderer(const JSONRenderer& rhs) = delete;
  const JSONRenderer& operator=(const JSONRenderer& rhs) = delete;
  virtual ~JSONRenderer() = default;

  const char* content_type() const;
  virtual void render_data(const std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>& data) const;
  virtual void render_find_results(const std::unordered_map<std::string, FindResult>& data) const;
};
