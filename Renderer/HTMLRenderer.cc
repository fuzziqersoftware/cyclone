#include "HTMLRenderer.hh"

#include <math.h>

#include <algorithm>
#include <phosg/Strings.hh>
#include <unordered_map>

using namespace std;


HTMLRenderer::HTMLRenderer(struct evbuffer* buf) : Renderer(buf) { }

const char* HTMLRenderer::content_type() const {
  return "text/html";
}

void HTMLRenderer::render_data(
    const unordered_map<string, unordered_map<string, ReadResult>>& results) const {
  throw runtime_error("HTML renderer cannot render data");
}

void HTMLRenderer::render_find_results(
    const unordered_map<string, FindResult>& data) const {
  if (data.size() == 0) {
    evbuffer_add(this->buf, "<!DOCTYPE html><html><head><title>cyclone</title></head><body>no query given</body></html>", 90);
    return;

  } else if (data.size() == 1) {
    evbuffer_add_printf(this->buf, "<!DOCTYPE html><html><head><title>cyclone: %s</title></head>", data.begin()->first.c_str());
  } else {
    evbuffer_add(this->buf, "<!DOCTYPE html><html><head><title>cyclone: multiple queries</title></head>", 74);
  }

  evbuffer_add_printf(this->buf, "<style type=\"text/css\">body { background-color: #000000; color: #FFFFFF; } a.query_link { color: #00FF00; } a.series_link { color: #00FFFF }</style>");

  for (const auto& query_it : data) {
    if (!query_it.second.error.empty()) {
      evbuffer_add_printf(this->buf, "Query: %s (failed: %s)<br /><br />",
          query_it.first.c_str(), query_it.second.error.c_str());
    } else {
      evbuffer_add_printf(this->buf, "Query: %s (results: %zu)<br /><br />",
          query_it.first.c_str(), query_it.second.results.size());
      auto results = query_it.second.results;
      sort(results.begin(), results.end());
      for (const auto& result_it : results) {
        if (ends_with(result_it, ".*")) {
          evbuffer_add_printf(this->buf, "<a class=\"query_link\" href=\"/metrics/find?format=html&query=%s\">%s</a><br />",
              result_it.c_str(), result_it.c_str());
        } else {
          evbuffer_add_printf(this->buf, "<a class=\"series_link\" href=\"/render?target=%s\">%s</a><br />",
              result_it.c_str(), result_it.c_str());
        }
      }
      evbuffer_add(this->buf, "<br />", 6);
    }
  }
}
