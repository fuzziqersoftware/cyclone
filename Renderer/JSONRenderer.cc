#define _STDC_FORMAT_MACROS

#include "JSONRenderer.hh"

#include <inttypes.h>
#include <math.h>

#include <phosg/Strings.hh>

using namespace std;


JSONRenderer::JSONRenderer(struct evbuffer* buf) : Renderer(buf) { }

const char* JSONRenderer::content_type() const {
  return "application/json";
}

void JSONRenderer::render_data(const unordered_map<string, ReadResult>& results) const {

  evbuffer_add(this->buf, "[", 1);

  int num_series = 0;
  for (const auto& it : results) {
    if (num_series) {
      evbuffer_add(this->buf, ", ", 2);
    }
    evbuffer_add_printf(this->buf, "{\"target\": \"%s\", \"datapoints\": [", it.first.c_str());

    int num_points = 0;
    for (const auto& pt : it.second.data) {
      if (isnan(pt.value)) {
        continue;
      }

      if (num_points) {
        evbuffer_add(this->buf, ", ", 2);
      }
      evbuffer_add_printf(this->buf, "[%g, %" PRId64 "]", pt.value, pt.timestamp);
      num_points++;
    }

    evbuffer_add(this->buf, "]}", 2);
    num_series++;
  }

  evbuffer_add(this->buf, "]", 1);
}

void JSONRenderer::render_find_results(const unordered_map<string, FindResult>& results) const {
  // the graphite interface supports only one query at once, but it also doesn't
  // support JSON... so we use a completely different format here.

  evbuffer_add(this->buf, "{", 1);

  size_t num_results = 0;
  for (const auto& it : results) {
    const auto& pattern = it.first;
    const auto& result = it.second;

    if (num_results) {
      evbuffer_add_printf(this->buf, ",\"%s\":[", pattern.c_str());
    } else {
      evbuffer_add_printf(this->buf, "\"%s\":[", pattern.c_str());
    }

    size_t num_items = 0;
    for (const auto& it : result.results) {
      if (num_items) {
        evbuffer_add_printf(this->buf, ",\"%s\"", it.c_str());
      } else {
        evbuffer_add_printf(this->buf, "\"%s\"", it.c_str());
      }
      num_items++;
    }
    evbuffer_add(this->buf, "]", 1);
    num_results++;
  }
  evbuffer_add(this->buf, "}", 1);
}
