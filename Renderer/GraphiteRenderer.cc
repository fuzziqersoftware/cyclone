#define _STDC_FORMAT_MACROS

#include "GraphiteRenderer.hh"

#include <inttypes.h>
#include <math.h>

#include <phosg/Strings.hh>

#include "../Store/Utils/Errors.hh"

using namespace std;


GraphiteRenderer::GraphiteRenderer(struct evbuffer* buf) : JSONRenderer(buf) { }

void GraphiteRenderer::render_find_results(
    const unordered_map<string, FindResult>& results) const {
  // the graphite interface supports only one query at once
  if (results.empty()) {
    evbuffer_add(this->buf, "[]", 2);
    return;
  }
  if (results.size() > 1) {
    throw runtime_error("can\'t return multiple find results through graphite interface");
  }

  const FindResult& result = results.begin()->second;

  if (!result.error.description.empty()) {
    throw runtime_error("find failed: " + string_for_error(result.error));
  }

  evbuffer_add(this->buf, "[", 1);

  size_t num_items = 0;
  for (const auto& it : result.results) {

    if (num_items) {
      evbuffer_add_printf(this->buf, ",");
    }

    // subdirectory
    if (ends_with(it, ".*")) {
      string full_path = it.substr(0, it.size() - 2);
      size_t dot_pos = full_path.rfind('.');

      string leaf_name;
      if (dot_pos == string::npos) {
        leaf_name = full_path;
      } else {
        leaf_name = full_path.substr(dot_pos + 1);
      }

      evbuffer_add_printf(this->buf,
          "{\"leaf\":0,\"context\":{},\"text\":\"%s\",\"expandable\":1,\"id\":\"%s\",\"allowChildren\":1}",
          leaf_name.c_str(), full_path.c_str());

    // key (not subdirectory)
    } else {
      size_t dot_pos = it.rfind('.');
      string leaf_name;
      if (dot_pos == string::npos) {
        leaf_name = it;
      } else {
        leaf_name = it.substr(dot_pos + 1);
      }

      evbuffer_add_printf(this->buf,
          "{\"leaf\":1,\"context\":{},\"text\":\"%s\",\"expandable\":0,\"id\":\"%s\",\"allowChildren\":0}",
          leaf_name.c_str(), it.c_str());
    }

    num_items++;
  }
  evbuffer_add(this->buf, "]", 1);
}
