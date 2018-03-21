#pragma once

#include <memory>

#include "HTTPServer.hh"
#include "../Renderer/Renderer.hh"
#include "../Store/Store.hh"


class CycloneHTTPServer : public HTTPServer {
public:
  CycloneHTTPServer() = delete;
  CycloneHTTPServer(std::shared_ptr<Store> store, size_t num_threads,
      uint64_t exit_check_interval);
  virtual ~CycloneHTTPServer() = default;

protected:
  std::shared_ptr<Store> store;

  std::unique_ptr<Renderer> create_renderer(const std::string& format, struct evbuffer* buf);

  virtual void handle_request(struct Thread& t, struct evhttp_request* req);

  std::string handle_index_request(struct Thread& t, struct evhttp_request* req,
      struct evbuffer* out_buffer);

  std::string handle_graphite_render_request(struct Thread& t,
      struct evhttp_request* req, struct evbuffer* out_buffer);
  std::string handle_graphite_find_request(struct Thread& t,
      struct evhttp_request* req, struct evbuffer* out_buffer);
};
