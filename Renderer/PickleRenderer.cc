#include "PickleRenderer.hh"

#include <math.h>

#include <phosg/Encoding.hh>
#include <phosg/Strings.hh>

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

void PickleRenderer::render_data(const unordered_map<string, ReadResult>& data) const {

  evbuffer_add(this->buf, "\x80\x02(", 3);

  for (const auto& it : data) {
    const auto& name = it.first;
    const auto& points = it.second.data;

    uint32_t start = 0;
    uint32_t end = 0;
    uint32_t step = 0;
    if (!points.empty()) {
      start = points.front().timestamp;
      end = points.back().timestamp;
      step = start - end;
      for (size_t x = 1; x < points.size(); x++) {
        uint32_t this_step = points[x].timestamp - points[x - 1].timestamp;
        if (step > this_step) {
          step = this_step;
        }
      }
    }

    evbuffer_add(this->buf, "(U\x0EpathExpression", 17);
    write_pickle_string(this->buf, name);
    evbuffer_add(this->buf, "U\x04name", 6);
    write_pickle_string(this->buf, name);
    evbuffer_add(this->buf, "U\x05startJ", 8);
    evbuffer_add(this->buf, &start, sizeof(start));
    evbuffer_add(this->buf, "U\x03\x65ndJ", 6);
    evbuffer_add(this->buf, &end, sizeof(end));
    evbuffer_add(this->buf, "U\x04stepJ", 7);
    evbuffer_add(this->buf, &step, sizeof(step));
    evbuffer_add(this->buf, "U\x06values(", 9);
    size_t offset = 0;
    for (uint32_t ts = start; end && (ts <= end); ts += step) {
      while (points[offset].timestamp < ts) {
        offset++;
      }
      if ((points[offset].timestamp != ts) || isnan(points[offset].value)) {
        evbuffer_add(this->buf, "N", 1);
      } else {
        uint64_t v = bswap64f(points[offset].value);
        evbuffer_add(this->buf, "G", 1);
        evbuffer_add(this->buf, &v, sizeof(v));
      }
    }
    evbuffer_add(this->buf, "ld", 2);
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
    evbuffer_add_printf(this->buf, "(T\x09intervals]T\x06isLeaf%cT\x0Bmetric_path%c",
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

  evbuffer_add(this->buf, "l.", 1);
}
