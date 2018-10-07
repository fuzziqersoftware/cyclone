#include "PickleRenderer.hh"

#include <math.h>

#include <phosg/Encoding.hh>
#include <phosg/Strings.hh>

#include "../Store/Utils/Errors.hh"

using namespace std;


PickleRenderer::PickleRenderer(struct evbuffer* buf) : Renderer(buf) { }

const char* PickleRenderer::content_type() const {
  return "application/pickle";
}

static void write_pickle_string(struct evbuffer* buf, const string& s) {
  if (s.size() > 0xFF) {
    uint32_t size = s.size();
    evbuffer_add(buf, "T", 1);
    evbuffer_add(buf, &size, sizeof(size));
  } else {
    uint8_t size = s.size();
    evbuffer_add(buf, "U", 1);
    evbuffer_add(buf, &size, sizeof(size));
  }
  evbuffer_add(buf, s.data(), s.size());
}

void PickleRenderer::render_data(
    const unordered_map<string, unordered_map<string, ReadResult>>& data,
    int64_t start_time, int64_t end_time) const {

  evbuffer_add(this->buf, "\x80\x02(", 3);

  for (const auto& it : data) { // (pattern, key_to_result)
    const auto& pattern = it.first;
    const auto& key_to_result = it.second;

    for (const auto& it2 : key_to_result) {
      const auto& key = it2.first;
      const auto& result = it2.second;

      // if there was a read error or the series doesn't exist (step==0), don't
      // render it
      if (!result.error.description.empty() || !result.step) {
        continue;
      }

      // the fields in result are i64s, we need to write u32s instead
      uint32_t step = result.step;
      uint32_t start32 = (start_time / step) * step;
      uint32_t end32 = (end_time / step) * step;

      evbuffer_add(this->buf, "(U\x0EpathExpression", 17);
      write_pickle_string(this->buf, pattern);
      evbuffer_add(this->buf, "U\x04name", 6);
      write_pickle_string(this->buf, key);
      evbuffer_add(this->buf, "U\x05startJ", 8);
      evbuffer_add(this->buf, &start32, sizeof(start32));
      evbuffer_add(this->buf, "U\x03\x65ndJ", 6);
      evbuffer_add(this->buf, &end32, sizeof(end32));
      evbuffer_add(this->buf, "U\x04stepJ", 7);
      evbuffer_add(this->buf, &step, sizeof(step));
      evbuffer_add(this->buf, "U\x06values(", 9);

      size_t offset = 0;
      for (int64_t ts = start32; ts <= end32; ts += result.step) {
        while ((offset < result.data.size()) && (result.data[offset].timestamp < ts)) {
          offset++;
        }

        if ((offset >= result.data.size()) || (result.data[offset].timestamp != ts) || isnan(result.data[offset].value)) {
          evbuffer_add(this->buf, "N", 1); // None
        } else {
          uint64_t v = bswap64f(result.data[offset].value);
          evbuffer_add(this->buf, "G", 1);
          evbuffer_add(this->buf, &v, sizeof(v));
        }
      }
      evbuffer_add(this->buf, "ld", 2);
    }
  }

  evbuffer_add(this->buf, "l.", 2);
}

void PickleRenderer::render_find_results(
    const unordered_map<string, FindResult>& results) const {
  // the graphite interface supports only one query at once
  if (results.size() != 1) {
    throw runtime_error("can\'t render multiple find results at once");
  }
  const auto& result = results.begin()->second;

  evbuffer_add(this->buf, "\x80\x02(", 3);
  for (auto it : result.results) {
    // note: we copy the results because we modify them within the loop.
    // TODO: rewrite so we don't have to copy them
    bool is_directory = ends_with(it, ".*");
    if (is_directory) {
      it.resize(it.size() - 2); // trim off the .*
    }
    bool long_name = it.size() < 0x100;

    // TODO: is it ok for intervals to be blank? unclear if it's used by the
    // frontend
    evbuffer_add_printf(this->buf, "(U\x09intervals]U\x06isLeaf%cU\x0Bmetric_path%c",
        is_directory ? '\x89' : '\x88', long_name ? 'T' : 'U');
    if (long_name) {
      uint32_t size = it.size();
      evbuffer_add(this->buf, &size, sizeof(size));
    } else {
      uint8_t size = it.size();
      evbuffer_add(this->buf, &size, sizeof(size));
    }
    evbuffer_add(this->buf, it.data(), it.size());

    evbuffer_add(this->buf, "d", 1);
  }

  evbuffer_add(this->buf, "l.", 2);
}
