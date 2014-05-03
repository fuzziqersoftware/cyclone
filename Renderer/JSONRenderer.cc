#include "JSONRenderer.hh"

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
      evbuffer_add_printf(this->buf, "[%g, %lld]", pt.value, pt.timestamp);
      num_points++;
    }

    evbuffer_add(this->buf, "]}", 2);
    num_series++;
  }

  evbuffer_add(this->buf, "]", 1);
}

void JSONRenderer::render_find_results(const unordered_map<string, FindResult>& results) const {
  // the graphite interface supports only one query at once
  if (results.size() != 1) {
    throw runtime_error("can\'t render multiple find results at once");
  }
  const auto& result = results.begin()->second;

  evbuffer_add(this->buf, "[", 1);
  size_t num_items = 0;
  for (const auto& it : result.results) {
    if (num_items) {
      evbuffer_add(this->buf, ",", 1);
    }
    if (ends_with(it, ".*")) {
      string dirname = it;
      dirname.resize(dirname.size() - 2); // trim off the .*
      evbuffer_add_printf(this->buf,
          "{\"intervals\":[],\"isLeaf\":false,\"metric_path\":\"%s\"}", dirname.c_str());
    } else {
      // TODO: is it ok for intervals to be blank? unclear if it's used by the
      // frontend
      evbuffer_add_printf(this->buf,
          "{\"intervals\":[],\"isLeaf\":true,\"metric_path\":\"%s\"}", it.c_str());
    }
    num_items++;
  }
  evbuffer_add(this->buf, "]", 1);
}
