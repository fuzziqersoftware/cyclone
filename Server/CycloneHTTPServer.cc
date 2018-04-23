#include "CycloneHTTPServer.hh"

#include <event2/buffer.h>
#include <event2/http.h>
#include <string.h>

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
    size_t num_threads, uint64_t exit_check_interval) :
    HTTPServer(num_threads, exit_check_interval), store(store) { }

void CycloneHTTPServer::handle_request(struct Thread& t, struct evhttp_request* req) {

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


static int64_t parse_relative_time(const string& s) {
  if (s == "now") {
    return now() / 1000000;
  }

  size_t bytes_parsed;
  int64_t int_part = stoll(s, &bytes_parsed, 0);
  if (bytes_parsed == s.size()) {
    return int_part; // it's an absolute timestamp
  }

  if (s[bytes_parsed] == 's') {
    return (now() / 1000000) + int_part;
  }
  if (s[bytes_parsed] == 'm') {
    return (now() / 1000000) + (int_part * 60);
  }
  if (s[bytes_parsed] == 'h') {
    return (now() / 1000000) + (int_part * 60 * 60);
  }
  if (s[bytes_parsed] == 'd') {
    return (now() / 1000000) + (int_part * 60 * 60 * 24);
  }
  if (s[bytes_parsed] == 'w') {
    return (now() / 1000000) + (int_part * 60 * 60 * 24 * 7);
  }
  if (s[bytes_parsed] == 'y') {
    return (now() / 1000000) + (int_part * 60 * 60 * 365);
  }

  throw invalid_argument("can\'t parse relative time: " + s);
}


string CycloneHTTPServer::handle_index_request(struct Thread& t,
    struct evhttp_request* req, struct evbuffer* out_buffer) {
  evbuffer_add(out_buffer, "stats:\n", 7);

  auto stats = this->store->get_stats();
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

  auto r = this->create_renderer(format, out_buffer);
  auto data = this->store->read(targets, start_time, end_time, false);
  r->render_data(data);
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

  auto r = this->create_renderer(format, out_buffer);
  auto ret = this->store->find(queries, false);
  r->render_find_results(ret);
  return r->content_type();
}
