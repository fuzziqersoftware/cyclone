#include "CycloneHTTPServer.hh"

#include <event2/buffer.h>
#include <event2/http.h>
#include <math.h>
#include <string.h>

#include <phosg/Filesystem.hh>
#include <phosg/Image.hh>
#include <phosg/Network.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>

#include "../Renderer/Renderer.hh"
#include "../Renderer/GraphiteRenderer.hh"
#include "../Renderer/HTMLRenderer.hh"
#include "../Renderer/ImageRenderer.hh"
#include "../Renderer/JSONRenderer.hh"
#include "../Renderer/PickleRenderer.hh"
#include "../Store/Utils/Errors.hh"

using namespace std;



CycloneHTTPServer::CycloneHTTPServer(shared_ptr<Store> store,
    size_t num_threads, const string& config_filename) :
    HTTPServer(num_threads), store(store), config_filename(config_filename),
    start_time(now()) {
  // generate the favicon
  Image img(32, 32);
  img.draw_horizontal_line(0, 31, 0, 0, 0, 0xFF, 0);
  img.draw_horizontal_line(0, 30, 1, 0, 0, 0xFF, 0);
  img.draw_horizontal_line(0, 29, 2, 0, 0, 0xFF, 0);
  img.draw_vertical_line(0, 3, 31, 0, 0, 0xFF, 0);
  img.draw_vertical_line(1, 3, 30, 0, 0, 0xFF, 0);
  img.draw_vertical_line(2, 3, 29, 0, 0, 0xFF, 0);
  img.draw_horizontal_line(1, 31, 31, 0, 0, 0x80, 0);
  img.draw_horizontal_line(2, 31, 30, 0, 0, 0x80, 0);
  img.draw_horizontal_line(3, 31, 29, 0, 0, 0x80, 0);
  img.draw_vertical_line(31, 1, 29, 0, 0, 0x80, 0);
  img.draw_vertical_line(30, 2, 29, 0, 0, 0x80, 0);
  img.draw_vertical_line(29, 3, 29, 0, 0, 0x80, 0);
  img.fill_rect(3, 3, 26, 26, 0, 0xC0, 0);
  img.fill_rect(5, 18, 9, 9, 0xFF, 0xFF, 0xFF);
  img.fill_rect(18, 5, 9, 9, 0xFF, 0xFF, 0xFF);
  this->favicon_data = img.save(Image::ImageFormat::WindowsBitmap);
}

void CycloneHTTPServer::handle_request(struct Thread& t, struct evhttp_request* req) {
  BusyThreadGuard g(&this->idle_thread_count);

  string content_type;
  unique_ptr<struct evbuffer, void(*)(struct evbuffer*)> out_buffer(
      evbuffer_new(), evbuffer_free);

  try {

    const char* uri = evhttp_request_get_uri(req);

    // default
    if (!strcmp(uri, "/") || !strncmp(uri, "/?", 2)) {
      content_type = this->handle_index_request(t, req, out_buffer.get());

    // graphite api
    } else if (!strncmp(uri, "/render/?", 9) || !strncmp(uri, "/render?", 8)) {
      content_type = this->handle_graphite_render_request(t, req, out_buffer.get());
    } else if (!strncmp(uri, "/metrics/find/?", 15) || !strncmp(uri, "/metrics/find?", 14)) {
      content_type = this->handle_graphite_find_request(t, req, out_buffer.get());

    // cyclone api
    } else if (!strncmp(uri, "/y/action", 9)) {
      content_type = this->handle_action_request(t, req, out_buffer.get());
    } else if (!strncmp(uri, "/y/stats", 8)) {
      content_type = this->handle_stats_request(t, req, out_buffer.get());
    } else if (!strncmp(uri, "/y/thread-status", 16)) {
      content_type = this->handle_thread_status_request(t, req, out_buffer.get());
    } else if (!strncmp(uri, "/y/config", 9)) {
      content_type = this->handle_config_request(t, req, out_buffer.get());
    } else if (!strncmp(uri, "/y/read-all", 11)) {
      content_type = this->handle_read_all_request(t, req, out_buffer.get());
    // } else if (!strcmp(uri, "/create")) {
    //   content_type = this->handle_create_request(t, req, out_buffer.get());
    // } else if (!strcmp(uri, "/delete")) {
    //   content_type = this->handle_delete_request(t, req, out_buffer.get());
    // } else if (!strncmp(uri, "/write", 6)) {
    //   content_type = this->handle_write_request(t, req, out_buffer.get());

    // other stuff
    } else if (!strcmp(uri, "/favicon.ico")) {
      content_type = this->handle_favicon_request(t, req, out_buffer.get());

    } else {
      throw http_error(404, "unknown action");
    }

  } catch (const http_error& e) {
    evbuffer_drain(out_buffer.get(), evbuffer_get_length(out_buffer.get()));
    evbuffer_add_printf(out_buffer.get(), "%s", e.what());
    this->send_response(req, e.code, "text/plain", out_buffer.get());
    return;

  } catch (const exception& e) {
    evbuffer_drain(out_buffer.get(), evbuffer_get_length(out_buffer.get()));
    evbuffer_add_printf(out_buffer.get(), "error during request: %s", e.what());
    this->send_response(req, 500, "text/plain", out_buffer.get());
    log(WARNING, "internal server error during http request: %s", e.what());
    return;
  }

  this->send_response(req, 200, content_type.c_str(), out_buffer.get());
}


unique_ptr<Renderer> CycloneHTTPServer::create_renderer(const string& format,
    struct evbuffer* buf) {
  if (format.empty()) {
    return unique_ptr<Renderer>(new GraphiteRenderer(buf));
  } else if (format == "json") {
    return unique_ptr<Renderer>(new JSONRenderer(buf));
  } else if (format == "pickle") {
    return unique_ptr<Renderer>(new PickleRenderer(buf));
  } else if (format == "html") {
    return unique_ptr<Renderer>(new HTMLRenderer(buf));
  } else {
    throw http_error(400, string_printf("unknown format: %s", format.c_str()));
  }
}


const string INDEX_HTML_HEADER("\
<!DOCTYPE html>\n\
<html><head><title>cyclone</title></head><body>\n\
<style type=\"text/css\">\n\
body {text-align:center; color: #000000; background-color: #FFFFFF}\n\
a {color: #0000CC}\n\
.small-note {color: #333333; font-size: 80%}\n\
</style>\n\
<h3>cyclone</h3>");

const string INDEX_HTML_FOOTER("\
<a href=\"/metrics/find/?query=*&format=html\">browse all metrics</a> or \n\
  find metrics matching <form style=\"display:inline\" method=\"GET\" action=\"/metrics/find/\">\n\
    <input name=\"query\" placeholder=\"pattern or series name\"/>\n\
    <input type=\"hidden\" name=\"format\" value=\"html\" />\n\
    <input type=\"submit\" value=\"find\" /></form><br />\n\
<form style=\"display:inline\" method=\"GET\" action=\"/render\">\n\
  read from <input name=\"target\" placeholder=\"pattern or series name\" />\n\
  from <input name=\"from\" value=\"-1h\"/>\n\
  until <input name=\"until\" value=\"now\"/>\n\
  <input type=\"hidden\" name=\"format\" value=\"json\" />\n\
  <input type=\"submit\" value=\"read\" /></form><br />\n\
<br />\n\
<form style=\"display:inline\" method=\"GET\" action=\"/y/action\">\n\
  rename <input name=\"from_key_name\" placeholder=\"original series name\" />\n\
  to <input name=\"to_key_name\" placeholder=\"new series name\">\n\
  <input name=\"merge\" type=\"checkbox\"> and merge with existing data\n\
  <input type=\"hidden\" name=\"action\" value=\"rename_series\" />\n\
  <input type=\"submit\" value=\"rename\" /></form><br />\n\
<form style=\"display:inline\" method=\"GET\" action=\"/y/action\">\n\
  delete <input name=\"pattern\" placeholder=\"pattern or series name\" />\n\
  <input type=\"checkbox\" name=\"deferred\"/> asynchronously\n\
  <input type=\"hidden\" name=\"action\" value=\"delete_series\" />\n\
  <input type=\"submit\" value=\"delete\" /></form><br />\n\
<br />\n\
<a href=\"/y/thread-status\">thread status</a><br />\n\
<a href=\"/y/stats\">server stats</a><br />\n\
<a href=\"/y/config\">server configuration</a><br />\n\
</body></html>");

string CycloneHTTPServer::handle_index_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {
  ProfilerGuard pg(create_profiler(t.thread_name, "(http) /"));

  evbuffer_add_reference(out_buffer, INDEX_HTML_HEADER.data(),
      INDEX_HTML_HEADER.size(), NULL, NULL);

  string hostname = gethostname();
  string start_time_str = format_time(this->start_time);
  evbuffer_add_printf(out_buffer, "\
<span class=\"small-note\">running on %s, started at %s</span><br />\n\
<br />\n", hostname.c_str(), start_time_str.c_str());

  evbuffer_add_reference(out_buffer, INDEX_HTML_FOOTER.data(),
      INDEX_HTML_FOOTER.size(), NULL, NULL);
  return "text/html";
}

string CycloneHTTPServer::handle_favicon_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {
  ProfilerGuard pg(create_profiler(t.thread_name, "(http) /favicon.ico"));

  evbuffer_add_reference(out_buffer, this->favicon_data.data(),
      this->favicon_data.size(), NULL, NULL);
  return "image/x-icon";
}

string CycloneHTTPServer::handle_stats_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {
  ProfilerGuard pg(create_profiler(t.thread_name, "(http) /y/stats"));

  auto stats = gather_stats(this->store, this->all_servers);

  map<string, int64_t> sorted_stats;
  for (const auto& it : stats) {
    sorted_stats.emplace(it);
  }
  for (const auto& it : sorted_stats) {
    evbuffer_add_printf(out_buffer, "%s = %" PRId64 "\n", it.first.c_str(),
        it.second);
  }

  return "text/plain";
}

string CycloneHTTPServer::handle_thread_status_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {
  ProfilerGuard pg(create_profiler(t.thread_name, "(http) /y/thread-status"));

  auto profilers = get_active_profilers();
  for (const auto& it : profilers) {
    if (it.second->done()) {
      evbuffer_add_printf(out_buffer, "[%s] idle\n", it.first.c_str());
    } else {
      string function_name = it.second->get_function_name();
      evbuffer_add_printf(out_buffer, "[%s] %s\n", it.first.c_str(),
          function_name.c_str());
    }
  }

  return "text/plain";
}

string CycloneHTTPServer::handle_action_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {

  const struct evhttp_uri* uri = evhttp_request_get_evhttp_uri(req);
  const char* query_string = evhttp_uri_get_query(uri);
  unordered_map<string, string> params;
  if (query_string) {
    params = this->parse_url_params_unique(query_string);
  }

  string action;
  try {
    action = params.at("action");
  } catch (const out_of_range&) {
    throw http_error(400, "action is required");
  }

  if (action == "rename_series") {
    string from_key_name, to_key_name;
    bool merge;
    try {
      from_key_name = params.at("from_key_name");
      to_key_name = params.at("to_key_name");
      merge = params.count("merge");
    } catch (const out_of_range&) {
      throw http_error(400, "from_key_name and to_key_name are required");
    }

    unordered_map<string, string> renames({{from_key_name, to_key_name}});

    ProfilerGuard pg(create_profiler(t.thread_name,
        Store::string_for_rename_series(renames, merge, false)));
    auto task = this->store->rename_series(NULL, renames, merge, false,
        pg.profiler.get());
    for (const auto& it : task->value()) {
      if (it.second.description.empty()) {
        evbuffer_add_printf(out_buffer, "[%s->%s] success\n",
            it.first.c_str(), renames.at(it.first).c_str());
      } else {
        string error_str = string_for_error(it.second);
        evbuffer_add_printf(out_buffer, "[%s->%s] FAILED: %s\n",
            it.first.c_str(), renames.at(it.first).c_str(), error_str.c_str());
      }
    }

  } else if (action == "delete_series") {
    string pattern;
    bool deferred;
    try {
      pattern = params.at("pattern");
      deferred = params.count("deferred");
    } catch (const out_of_range&) {
      throw http_error(400, "pattern is required");
    }

    vector<string> patterns({pattern});
    ProfilerGuard pg(create_profiler(t.thread_name,
        Store::string_for_delete_series(patterns, deferred, false)));
    auto task = this->store->delete_series(NULL, patterns, deferred, false,
        pg.profiler.get());
    for (const auto& it : task->value()) {
      if (it.second.error.description.empty()) {
        evbuffer_add_printf(out_buffer, "[%s] %" PRId64 " series deleted from disk, %" PRId64 " series deleted from buffer\n",
            it.first.c_str(), it.second.disk_series_deleted, it.second.buffer_series_deleted);
      } else {
        evbuffer_add_printf(out_buffer, "[%s] FAILED: %s (%" PRId64 " series deleted from disk, %" PRId64 " series deleted from buffer)\n",
            it.first.c_str(), it.second.error.description.c_str(),
            it.second.disk_series_deleted, it.second.buffer_series_deleted);
      }
    }

  } else {
    throw http_error(400, "unknown action");
  }

  return "text/plain";
}

string CycloneHTTPServer::handle_config_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {
  ProfilerGuard pg(create_profiler(t.thread_name, "(http) /y/config"));

  string contents = load_file(this->config_filename);
  evbuffer_add_printf(out_buffer,
      "// configuration filename: %s\n", this->config_filename.c_str());
  evbuffer_add(out_buffer, contents.data(), contents.size());
  return "application/json";
}

string CycloneHTTPServer::handle_graphite_render_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {

  const struct evhttp_uri* uri = evhttp_request_get_evhttp_uri(req);
  const char* query_string = evhttp_uri_get_query(uri);
  unordered_multimap<string, string> params;
  if (query_string) {
    params = this->parse_url_params(query_string);
  }

  int64_t start_time = 0, end_time = 0;
  vector<string> targets;
  string format;

  for (auto it : params) {
    if (it.first.compare("from") == 0) {
      start_time = parse_relative_time(it.second.c_str());
    } else if (it.first.compare("until") == 0) {
      end_time = parse_relative_time(it.second.c_str());
    } else if (it.first.compare("format") == 0) {
      format = it.second;
    } else if (it.first.compare("target") == 0) {
      targets.push_back(it.second);
    } else if ((it.first.compare("now") == 0) || (it.first.compare("noCache") == 0) || (it.first.compare("local") == 0)) {
      // ignore these; graphite sends them but cyclone doesn't use them
    } else {
      throw http_error(400, string_printf("unknown argument: %s", it.first.c_str()));
    }
  }

  // if end_time and start_time aren't given, use defaults
  if (end_time == 0) {
    end_time = time(NULL);
  }
  if (start_time == 0) {
    start_time = end_time - 60 * 60;
  }

  ProfilerGuard pg(create_profiler(t.thread_name, Store::string_for_read(targets,
      start_time, end_time, false)));
  auto r = this->create_renderer(format, out_buffer);

  auto task = this->store->read(NULL, targets, start_time, end_time,
      false, pg.profiler.get());

  pg.profiler->checkpoint("store_read");
  r->render_data(task->value(), start_time, end_time);
  pg.profiler->checkpoint("render");
  return r->content_type();
}

string CycloneHTTPServer::handle_graphite_find_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {
  // arguments: local, format, query
  // e.g. /metrics/find/?local=1&format=pickle&query=key.pattern.*

  const struct evhttp_uri* uri = evhttp_request_get_evhttp_uri(req);
  const char* query_string = evhttp_uri_get_query(uri);
  unordered_multimap<string, string> params;
  if (query_string) {
    params = this->parse_url_params(query_string);
  }

  string def = "";
  string format = this->get_url_param(params, "format", &def);

  vector<string> queries;
  for (auto its = params.equal_range("query"); its.first != its.second; its.first++) {
    queries.emplace_back(its.first->second);
  }

  ProfilerGuard pg(create_profiler(t.thread_name, Store::string_for_find(
      queries, false)));
  auto r = this->create_renderer(format, out_buffer);

  auto task = this->store->find(NULL, queries, false, pg.profiler.get());

  pg.profiler->checkpoint("store_find");
  r->render_find_results(task->value());
  pg.profiler->checkpoint("render");
  return r->content_type();
}

string CycloneHTTPServer::handle_read_all_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {

  const struct evhttp_uri* uri = evhttp_request_get_evhttp_uri(req);
  const char* query_string = evhttp_uri_get_query(uri);
  unordered_multimap<string, string> params;
  if (query_string) {
    params = this->parse_url_params(query_string);
  }

  string target;
  for (auto it : params) {
    if (it.first.compare("target") == 0) {
      if (!target.empty()) {
        throw http_error(400, "multiple targets given");
      }
      target = it.second;
    } else {
      throw http_error(400, string_printf("unknown argument: %s", it.first.c_str()));
    }
  }

  ProfilerGuard pg(create_profiler(t.thread_name, Store::string_for_read_all(
      target, false)));

  auto task = this->store->read_all(NULL, target, false, pg.profiler.get());
  pg.profiler->checkpoint("store_read_all");

  auto& result = task->value();
  if (result.error.description.empty()) {
    evbuffer_add(out_buffer, "[", 1);
    size_t num_points = 0;
    for (const auto& pt : result.data) {
      if (isnan(pt.value)) {
        continue;
      }

      if (num_points) {
        evbuffer_add(out_buffer, ",", 1);
      }
      evbuffer_add_printf(out_buffer, "[%g,%" PRId64 "]", pt.value, pt.timestamp);
      num_points++;
    }
    evbuffer_add(out_buffer, "]", 1);
  } else {
    string error_str = string_for_error(result.error);
    evbuffer_add_printf(out_buffer, "\"error: %s\"", error_str.c_str());
  }
  pg.profiler->checkpoint("render");

  return "application/json";
}
