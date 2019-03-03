#include "HTTPServer.hh"

#include <event2/buffer.h>
#include <event2/http.h>

#include <string>
#include <phosg/Network.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>

using namespace std;


HTTPServer::HTTPServer(size_t num_threads) :
    Server("http_server", num_threads), should_exit(false),
    num_threads(num_threads), threads() { }

void HTTPServer::listen(const string& path) {
  int fd = ::listen(path, 0, SOMAXCONN);
  log(INFO, "[HTTPServer] listening on unix socket %s on fd %d", path.c_str(),
      fd);
  this->add_socket(fd);
}

void HTTPServer::listen(const string& addr, int port) {
  int fd = ::listen(addr, port, SOMAXCONN);
  string netloc_str = render_netloc(addr, port);
  log(INFO, "[HTTPServer] listening on tcp interface %s on fd %d",
      netloc_str.c_str(), fd);
  this->add_socket(fd);
}

void HTTPServer::add_socket(int fd) {
  evutil_make_socket_nonblocking(fd);
  this->listen_fds.insert(fd);
}

void HTTPServer::start() {
  this->threads.reserve(this->num_threads);
  while (this->threads.size() < this->num_threads) {
    this->threads.emplace_back(this);
  }
}

void HTTPServer::schedule_stop() {
  log(INFO, "[HTTPServer] scheduling exit for all threads");
  this->should_exit = true;
}

void HTTPServer::wait_for_stop() {
  log(INFO, "[HTTPServer] waiting for workers to terminate");
  // this calls the Thread destructors, which calls join() on the threads
  this->threads.clear();
  log(INFO, "[HTTPServer] shutdown complete");
}

void HTTPServer::send_response(struct evhttp_request* req, int code,
    const char* content_type, struct evbuffer* b) {

  struct evkeyvalq* headers = evhttp_request_get_output_headers(req);
  evhttp_add_header(headers, "Content-Type", content_type);
  evhttp_add_header(headers, "Server", "cyclone");

  evhttp_send_reply(req, code, explanation_for_response_code.at(code), b);
}

void HTTPServer::send_response(struct evhttp_request* req, int code,
    const char* content_type, const char* fmt, ...) {

  unique_ptr<struct evbuffer, void(*)(struct evbuffer*)> out_buffer(
      evbuffer_new(), evbuffer_free);

  va_list va;
  va_start(va, fmt);
  evbuffer_add_vprintf(out_buffer.get(), fmt, va);
  va_end(va);

  HTTPServer::send_response(req, code, content_type, out_buffer.get());
}

unordered_multimap<string, string> HTTPServer::parse_url_params(
    const string& query) {

  unordered_multimap<string, string> params;
  if (query.empty()) {
    return params;
  }
  for (auto it : split(query, '&')) {
    size_t first_equals = it.find('=');
    if (first_equals != string::npos) {
      string value(it, first_equals + 1);

      size_t write_offset = 0, read_offset = 0;
      for (; read_offset < value.size(); write_offset++) {
        if ((value[read_offset] == '%') && (read_offset < value.size() - 2)) {
          value[write_offset] =
              static_cast<char>(value_for_hex_char(value[read_offset + 1]) << 4) |
              static_cast<char>(value_for_hex_char(value[read_offset + 2]));
              read_offset += 3;
        } else if (value[write_offset] == '+') {
          value[write_offset] = ' ';
          read_offset++;
        } else {
          value[write_offset] = value[read_offset];
          read_offset++;
        }
      }
      value.resize(write_offset);

      params.emplace(piecewise_construct, forward_as_tuple(it, 0, first_equals),
          forward_as_tuple(value));
    } else {
      params.emplace(it, "");
    }
  }
  return params;
}

unordered_map<string, string> HTTPServer::parse_url_params_unique(
    const string& query) {
  unordered_map<string, string> ret;
  for (const auto& it : HTTPServer::parse_url_params(query)) {
    ret.emplace(it.first, move(it.second));
  }
  return ret;
}

const string& HTTPServer::get_url_param(
    const unordered_multimap<string, string>& params, const string& key,
    const string* _default) {

  auto range = params.equal_range(key);
  if (range.first == range.second) {
    if (!_default) {
      throw out_of_range("URL parameter " + key + " not present");
    }
    return *_default;
  }

  return range.first->second;
}

const unordered_map<int, const char*> HTTPServer::explanation_for_response_code({
  {100, "Continue"},
  {101, "Switching Protocols"},
  {102, "Processing"},
  {200, "OK"},
  {201, "Created"},
  {202, "Accepted"},
  {203, "Non-Authoritative Information"},
  {204, "No Content"},
  {205, "Reset Content"},
  {206, "Partial Content"},
  {207, "Multi-Status"},
  {208, "Already Reported"},
  {226, "IM Used"},
  {300, "Multiple Choices"},
  {301, "Moved Permanently"},
  {302, "Found"},
  {303, "See Other"},
  {304, "Not Modified"},
  {305, "Use Proxy"},
  {307, "Temporary Redirect"},
  {308, "Permanent Redirect"},
  {400, "Bad Request"},
  {401, "Unathorized"},
  {402, "Payment Required"},
  {403, "Forbidden"},
  {404, "Not Found"},
  {405, "Method Not Allowed"},
  {406, "Not Acceptable"},
  {407, "Proxy Authentication Required"},
  {408, "Request Timeout"},
  {409, "Conflict"},
  {410, "Gone"},
  {411, "Length Required"},
  {412, "Precondition Failed"},
  {413, "Request Entity Too Large"},
  {414, "Request-URI Too Long"},
  {415, "Unsupported Media Type"},
  {416, "Requested Range Not Satisfiable"},
  {417, "Expectation Failed"},
  {418, "I\'m a Teapot"},
  {420, "Enhance Your Calm"},
  {422, "Unprocessable Entity"},
  {423, "Locked"},
  {424, "Failed Dependency"},
  {426, "Upgrade Required"},
  {428, "Precondition Required"},
  {429, "Too Many Requests"},
  {431, "Request Header Fields Too Large"},
  {444, "No Response"},
  {449, "Retry With"},
  {451, "Unavailable For Legal Reasons"},
  {500, "Internal Server Error"},
  {501, "Not Implemented"},
  {502, "Bad Gateway"},
  {503, "Service Unavailable"},
  {504, "Gateway Timeout"},
  {505, "HTTP Version Not Supported"},
  {506, "Variant Also Negotiates"},
  {507, "Insufficient Storage"},
  {508, "Loop Detected"},
  {509, "Bandwidth Limit Exceeded"},
  {510, "Not Extended"},
  {511, "Network Authentication Required"},
  {598, "Network Read Timeout Error"},
  {599, "Network Connect Timeout Error"},
});

HTTPServer::http_error::http_error(int code, const string& what) :
    runtime_error(what), code(code) { }

HTTPServer::Thread::Thread(HTTPServer* s) : server(s),
    base(event_base_new(), event_base_free),
    exit_check_event(event_new(this->base.get(), -1, EV_TIMEOUT | EV_PERSIST,
        &HTTPServer::Thread::dispatch_exit_check, this), event_free),
    http(evhttp_new(this->base.get()), evhttp_free), t() {

  this->thread_name = string_printf("HTTPServer::worker_thread_routine (base=0x%016" PRIX64 ")",
      reinterpret_cast<uint64_t>(base.get()));

  evhttp_set_gencb(this->http.get(), HTTPServer::dispatch_handle_request, this);

  for (int fd : this->server->listen_fds) {
    evhttp_accept_socket(this->http.get(), fd);
  }

  struct timeval exit_check_interval = usecs_to_timeval(2000000);
  event_add(this->exit_check_event.get(), &exit_check_interval);

  this->t = thread(&HTTPServer::worker_thread_routine, s, this);
}

HTTPServer::Thread::~Thread() {
  this->t.join();
}

void HTTPServer::Thread::dispatch_exit_check(evutil_socket_t fd, short what, void* ctx) {
  Thread* t = (Thread*)ctx;
  if (t->server->should_exit) {
    event_base_loopexit(t->base.get(), NULL);
  }
}

void HTTPServer::dispatch_handle_request(struct evhttp_request* req, void* ctx) {
  HTTPServer::Thread* t = reinterpret_cast<HTTPServer::Thread*>(ctx);
  t->server->handle_request(*t, req);
}

void HTTPServer::worker_thread_routine(HTTPServer::Thread* t) {
  event_base_dispatch(t->base.get());
}
