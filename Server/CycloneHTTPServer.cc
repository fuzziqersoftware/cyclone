#include "CycloneHTTPServer.hh"

#include <event2/buffer.h>
#include <event2/http.h>
#include <math.h>
#include <string.h>

#include <phosg/Filesystem.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>

#include "../Renderer/Renderer.hh"
#include "../Renderer/GraphiteRenderer.hh"
#include "../Renderer/HTMLRenderer.hh"
#include "../Renderer/ImageRenderer.hh"
#include "../Renderer/JSONRenderer.hh"
#include "../Renderer/PickleRenderer.hh"

using namespace std;



CycloneHTTPServer::CycloneHTTPServer(shared_ptr<Store> store,
    size_t num_threads, const string& config_filename) :
    HTTPServer(num_threads), store(store), config_filename(config_filename) { }

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
    } else if (!strncmp(uri, "/y/stats", 8)) {
      content_type = this->handle_stats_request(t, req, out_buffer.get());
    } else if (!strncmp(uri, "/y/config", 9)) {
      content_type = this->handle_config_request(t, req, out_buffer.get());
    } else if (!strncmp(uri, "/y/read_all", 11)) {
      content_type = this->handle_read_all_request(t, req, out_buffer.get());
    // } else if (!strcmp(uri, "/create")) {
    //   content_type = this->handle_create_request(t, req, out_buffer.get());
    // } else if (!strcmp(uri, "/delete")) {
    //   content_type = this->handle_delete_request(t, req, out_buffer.get());
    // } else if (!strncmp(uri, "/read/?", 7) || !strncmp(uri, "/read?", 6)) {
    //   content_type = this->handle_read_request(t, req, out_buffer.get());
    // } else if (!strncmp(uri, "/write", 6)) {
    //   content_type = this->handle_write_request(t, req, out_buffer.get());
    // } else if (!strncmp(uri, "/find/?", 7) || !strncmp(uri, "/find?", 6)) {
    //   content_type = this->handle_find_request(t, req, out_buffer.get());

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


const string INDEX_HTML("\
<!DOCTYPE html>\n\
<html><head><title>cyclone</title></head><body>\n\
<h3>cyclone</h3>\n\
<a href=\"/metrics/find/?query=*&format=html\">browse</a> - <a href=\"/y/stats\">stats</a> - <a href=\"/y/config\">config</a>\n\
<form method=\"GET\" action=\"/metrics/find/\"><input name=\"query\" /><input type=\"hidden\" name=\"format\" value=\"html\" /><input type=\"submit\" value=\"find\" /></form><br />\n\
</body></html>");

string CycloneHTTPServer::handle_index_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {
  evbuffer_add_reference(out_buffer, INDEX_HTML.data(), INDEX_HTML.size(), NULL, NULL);
  return "text/html";
}

string CycloneHTTPServer::handle_stats_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {
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

string CycloneHTTPServer::handle_config_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {
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
  auto data = this->store->read(targets, start_time, end_time, false, pg.profiler.get());
  pg.profiler->checkpoint("store_read");
  r->render_data(data, start_time, end_time);
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
  auto ret = this->store->find(queries, false, pg.profiler.get());
  pg.profiler->checkpoint("store_find");
  r->render_find_results(ret);
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
  auto result = this->store->read_all(target, false, pg.profiler.get());
  pg.profiler->checkpoint("store_read_all");

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
    evbuffer_add_printf(out_buffer, "\"error: %s\"",
        result.error.description.c_str());
  }
  pg.profiler->checkpoint("render");

  return "application/json";
}
