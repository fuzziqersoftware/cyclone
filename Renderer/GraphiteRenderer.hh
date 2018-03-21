#pragma once

#include <event2/buffer.h>

#include <unordered_map>
#include <vector>

#include "../gen-cpp/Cyclone.h"
#include "JSONRenderer.hh"


class GraphiteRenderer : public JSONRenderer {
public:
  GraphiteRenderer(struct evbuffer* buf);
  GraphiteRenderer(const GraphiteRenderer& rhs) = delete;
  const GraphiteRenderer& operator=(const GraphiteRenderer& rhs) = delete;
  virtual ~GraphiteRenderer() = default;

  virtual void render_find_results(const std::unordered_map<std::string, FindResult>& data) const;
};
