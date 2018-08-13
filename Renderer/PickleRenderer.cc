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

void PickleRenderer::render_data(
    const unordered_map<string, unordered_map<string, ReadResult>>& data) const {

  evbuffer_add(this->buf, "\x80\x02(", 3);

  // figure out the overall start and end times. we need to fill in nulls in the
  // returned series because graphite functions like divideSeries don't check
  // the timestamps! they just divide the datapoints in order, which is wrong.
  // but rendering doesn't work (and things will break anyway) if you do this
  // with series with different step values (because we could overshoot the
  // timestamps in the lower-resolution series), so find start/end pairs for
  // each unique step value instead of overall
  unordered_map<uint32_t, pair<uint32_t, uint32_t>> step_to_start_end;
  for (const auto& it : data) { // (pattern, key_to_result)
    const auto& key_to_result = it.second;

    for (const auto& it2 : key_to_result) {
      const auto& result = it2.second;

      auto emplace_ret = step_to_start_end.emplace(result.step, make_pair(
          result.start_time, result.end_time));
      if (emplace_ret.second) {
        continue; // no other series had this step yet
      }

      // some other series had this step. apply min/max appropriately
      pair<uint32_t, uint32_t>& start_end = emplace_ret.first->second;
      if (result.start_time < start_end.first) {
        start_end.first = result.start_time;
      }
      if (result.end_time > start_end.second) {
        start_end.second = result.end_time;
      }
    }
  }

  for (const auto& it : data) { // (pattern, key_to_result)
    const auto& pattern = it.first;
    const auto& key_to_result = it.second;

    for (const auto& it2 : key_to_result) {
      const auto& key = it2.first;
      const auto& result = it2.second;

      // if there was a read error or the series doesn't exist (step==0), don't
      // render it
      if (!result.error.empty() || !result.step) {
        continue;
      }

      // the fields in result are i64s, we need to write u32s instead
      uint32_t step = result.step;
      const auto& start_end = step_to_start_end.at(step);

      evbuffer_add(this->buf, "(U\x0EpathExpression", 17);
      write_pickle_string(this->buf, pattern);
      evbuffer_add(this->buf, "U\x04name", 6);
      write_pickle_string(this->buf, key);
      evbuffer_add(this->buf, "U\x05startJ", 8);
      evbuffer_add(this->buf, &start_end.first, sizeof(start_end.first));
      evbuffer_add(this->buf, "U\x03\x65ndJ", 6);
      evbuffer_add(this->buf, &start_end.second, sizeof(start_end.second));
      evbuffer_add(this->buf, "U\x04stepJ", 7);
      evbuffer_add(this->buf, &step, sizeof(step));
      evbuffer_add(this->buf, "U\x06values(", 9);

      size_t offset = 0;
      for (int64_t ts = start_end.first; ts <= start_end.second; ts += result.step) {
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
