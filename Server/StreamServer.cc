#include "StreamServer.hh"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <phosg/Encoding.hh>
#include <phosg/JSON.hh>
#include <phosg/JSONPickle.hh>
#include <phosg/Network.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>

#include <iostream>
#include <thread>

using namespace std;


StreamServer::Client::Client(int fd, const struct sockaddr& remote_addr,
    Protocol protocol) : remote_addr(remote_addr), fd(fd), protocol(protocol) { }

StreamServer::WorkerThread::WorkerThread(StreamServer* server, int worker_num) :
    server(server), worker_num(worker_num),
    base(event_base_new(), event_base_free), t() { }

void StreamServer::WorkerThread::disconnect_client(struct bufferevent* bev) {
  this->bev_to_client.erase(bev);
  bufferevent_free(bev);
  // don't have to explicitly close the client's fd because the bufferevent has
  // BEV_OPT_CLOSE_ON_FREE
}

void StreamServer::WorkerThread::dispatch_on_listen_accept(
    struct evconnlistener *listener, evutil_socket_t fd,
    struct sockaddr *address, int socklen, void *ctx) {
  WorkerThread* wt = (WorkerThread*)ctx;
  wt->server->on_listen_accept(*wt, listener, fd, address, socklen);
}

void StreamServer::WorkerThread::dispatch_on_listen_error(
    struct evconnlistener *listener, void *ctx) {
  WorkerThread* wt = (WorkerThread*)ctx;
  wt->server->on_listen_error(*wt, listener);
}

void StreamServer::WorkerThread::dispatch_on_client_input(
    struct bufferevent *bev, void *ctx) {
  WorkerThread* wt = (WorkerThread*)ctx;
  wt->server->on_client_input(*wt, bev);
}

void StreamServer::WorkerThread::dispatch_on_client_error(
    struct bufferevent *bev, short events, void *ctx) {
  WorkerThread* wt = (WorkerThread*)ctx;
  wt->server->on_client_error(*wt, bev, events);
}

void StreamServer::WorkerThread::dispatch_check_for_thread_exit(
    evutil_socket_t fd, short what, void* ctx) {
  WorkerThread* wt = (WorkerThread*)ctx;
  wt->server->check_for_thread_exit(*wt, fd, what);
}

void StreamServer::on_listen_accept(StreamServer::WorkerThread& wt,
    struct evconnlistener *listener, evutil_socket_t fd,
    struct sockaddr *address, int socklen) {

  int fd_flags = fcntl(fd, F_GETFD, 0);
  if (fd_flags >= 0) {
    fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC);
  }

  int listen_fd = evconnlistener_get_fd(listener);
  Protocol protocol = Protocol::Line;
  try {
    protocol = this->listen_fd_to_protocol.at(listen_fd);
  } catch (const out_of_range& e) {
    log(WARNING, "[StreamServer] can\'t determine protocol for socket %d; assuming line",
        listen_fd);
  }

  struct bufferevent *bev = bufferevent_socket_new(wt.base.get(), fd,
      BEV_OPT_CLOSE_ON_FREE);
  wt.bev_to_client.emplace(piecewise_construct, make_tuple(bev),
      make_tuple(fd, *address, protocol));

  bufferevent_setcb(bev, &WorkerThread::dispatch_on_client_input, NULL,
      &WorkerThread::dispatch_on_client_error, &wt);
  bufferevent_enable(bev, EV_READ | EV_WRITE);

  // if this is a shell client, send it the prompt string
  if (protocol == Protocol::Shell) {
    struct evbuffer* out_buffer = bufferevent_get_output(bev);
    evbuffer_add(out_buffer, "cyclone> ", 9);
  }
}

void StreamServer::on_listen_error(StreamServer::WorkerThread& wt,
    struct evconnlistener *listener) {
  int err = EVUTIL_SOCKET_ERROR();
  log(ERROR, "[StreamServer] failure on listening socket %d: %d (%s)\n",
      evconnlistener_get_fd(listener), err,
      evutil_socket_error_to_string(err));

  event_base_loopexit(wt.base.get(), NULL);
}

void StreamServer::on_client_input(StreamServer::WorkerThread& wt,
    struct bufferevent *bev) {

  struct evbuffer* in_buffer = bufferevent_get_input(bev);
  Client* c = NULL;
  try {
    c = &wt.bev_to_client.at(bev);
  } catch (const out_of_range& e) {
    log(WARNING, "[StreamServer] received message from client with no configuration");

    // ignore all the data
    evbuffer_drain(in_buffer, evbuffer_get_length(in_buffer));
    return;
  }

  unordered_map<string, Series> data;
  switch (c->protocol) {
    case Protocol::Pickle: {
      while (evbuffer_get_length(in_buffer)) {
        // the payload is preceded by a 32-bit size field
        uint32_t payload_size;
        ssize_t bytes = evbuffer_copyout(in_buffer, &payload_size, sizeof(uint32_t));
        if (bytes != sizeof(uint32_t)) {
          break; // we don't have the full size field yet
        }
        payload_size = bswap32(payload_size);

        // if there aren't (payload_size + sizeof(uint32_t)) bytes available, we
        // don't have a full payload, so we're done for now
        if (evbuffer_get_length(in_buffer) < payload_size + sizeof(uint32_t)) {
          break;
        }

        // extract the payload
        string payload(payload_size, 0);
        evbuffer_drain(in_buffer, sizeof(uint32_t));
        evbuffer_remove(in_buffer, const_cast<char*>(payload.data()), payload_size);

        // parse the payload and generate the data dict
        try {
          shared_ptr<JSONObject> o = parse_pickle(payload);

          // expected format: [(key, (timestamp, value))...]
          for (const auto& datapoint : o->as_list()) {
            const auto& datapoint_list = datapoint->as_list();
            if (datapoint_list.size() != 2) {
              continue;
            }
            const auto& value_list = datapoint_list[1]->as_list();
            if (value_list.size() != 2) {
              continue;
            }

            // data[key][timestamp] = value
            auto& series = data[datapoint_list[0]->as_string()];
            series.emplace_back();
            series.back().timestamp = value_list[0]->as_int();
            series.back().value = value_list[1]->as_float();
          }
        } catch (const exception& e) {
          log(INFO, "[StreamServer] received bad pickle object (%s)", e.what());
        }
      }
      break;
    }

    case Protocol::Line: {
      // keyname value [timestamp_secs]
      // if timestamp_secs is zero or missing, use the current time
      char* line_data = NULL;
      size_t line_length = 0;
      while ((line_data = evbuffer_readln(in_buffer, &line_length, EVBUFFER_EOL_CRLF))) {
        auto tokens = split(line_data, ' ');
        free(line_data);

        if (tokens.size() > 3) {
          log(INFO, "[StreamServer] received bad line: too many tokens");
          continue;
        }
        if (tokens.size() < 2) {
          log(INFO, "[StreamServer] received bad line: not enough tokens");
          continue;
        }

        try {
          auto& series = data[tokens[0]];
          double value = stod(tokens[1]);
          uint32_t t = (tokens.size() == 3) ? stoul(tokens[2]) : 0;
          if (!t) {
            t = time(NULL);
          }
          series.emplace_back();
          series.back().timestamp = t;
          series.back().value = value;
        } catch (const exception& e) {
          log(INFO, "[StreamServer] received bad line (%s)", e.what());
        }
      }
      break;
    }

    case Protocol::Shell: {
      struct evbuffer* out_buffer = bufferevent_get_output(bev);
      char* line_data = NULL;
      size_t line_length = 0;
      while ((line_data = evbuffer_readln(in_buffer, &line_length, EVBUFFER_EOL_CRLF))) {
        try {
          this->execute_shell_command(line_data, out_buffer);
          evbuffer_add_reference(out_buffer, "\n\ncyclone> ", 11, NULL, NULL);
        } catch (const exception& e) {
          evbuffer_add_printf(out_buffer, "failed: %s\n\ncyclone> ", e.what());
        }
        free(line_data);
      }
      break;
    }
  }

  if (!data.empty()) {
    for (const auto& it : this->store->write(data, false)) {
      if (it.second.empty()) {
        continue;
      }
      log(WARNING, "write failed: %s\n", it.second.c_str());
    }
  }
}

void StreamServer::on_client_error(StreamServer::WorkerThread& wt,
    struct bufferevent *bev, short events) {

  if (events & BEV_EVENT_ERROR) {
    int err = EVUTIL_SOCKET_ERROR();
    log(WARNING, "[StreamServer] client caused %d (%s)\n", err,
        evutil_socket_error_to_string(err));
  }
  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    wt.disconnect_client(bev);
  }
}

void StreamServer::check_for_thread_exit(StreamServer::WorkerThread& wt,
    evutil_socket_t fd, short what) {
  if (this->should_exit) {
    event_base_loopexit(wt.base.get(), NULL);
  }
}

const string HELP_STRING("\
commands:\n\
  help\n\
    you're reading it now\n\
\n\
  find <pattern> [<pattern> ...]\n\
    search for metrics matching the given pattern(s)\n\
\n\
  read <pattern> [+start=<timestamp>] [+end=<timestamp>]\n\
    read data from all series that match the given pattern\n\
    if given, start and end must be absolute epoch timestamps in seconds\n\
\n\
  write <series> <timestamp> <value> [<timestamp> <value> ...]\n\
    write one or more datapoints to the given series\n\
\n\
  delete <series> [<series> ...]\n\
    delete entire series\n\
\n\
  stats\n\
    get the current server stats\n\
");

void StreamServer::execute_shell_command(const char* line_data,
    struct evbuffer* out_buffer) {
  auto tokens = split(line_data, ' ');
  if (tokens.size() == 0) {
    evbuffer_add(out_buffer, "command is empty", 16);
    return;
  }

  string command_name = move(tokens[0]);
  tokens.erase(tokens.begin());

  if (command_name == "help") {
    evbuffer_add_reference(out_buffer, HELP_STRING.data(), HELP_STRING.size(), NULL, NULL);

  } else if (command_name == "stats") {
    auto stats = this->store->get_stats();
    for (const auto& it : stats) {
      evbuffer_add_printf(out_buffer, "%s = %" PRId64 "\n", it.first.c_str(), it.second);
    }

  } else if (command_name == "delete") {
    // all tokens after the command name are series names
    auto result = this->store->delete_series(tokens, false);

    for (const auto& it : result) {
      evbuffer_add_printf(out_buffer, "[%s] %" PRId64 " series deleted\n",
          it.first.c_str(), it.second);
    }

  } else if (command_name == "find") {
    // all tokens after the command name are patterns
    auto find_result = this->store->find(tokens, false);

    if (find_result.size() == 1) {
      const auto& result = find_result.begin()->second;
      if (!result.error.empty()) {
        evbuffer_add_printf(out_buffer, "FAILED: %s\n", result.error.c_str());
      } else {
        for (const auto& item : result.results) {
          evbuffer_add(out_buffer, item.data(), item.size());
          evbuffer_add(out_buffer, "\n", 1);
        }
      }

    } else {
      for (const auto& it : find_result) {
        if (!it.second.error.empty()) {
          evbuffer_add_printf(out_buffer, "[%s] FAILED: %s\n", it.first.c_str(), it.second.error.c_str());
          continue;
        }
        for (const auto& item : it.second.results) {
          evbuffer_add_printf(out_buffer, "[%s] %s\n", it.first.c_str(), item.c_str());
        }
      }
    }

  } else if (command_name == "write") {
    evbuffer_add(out_buffer, "write is not yet implemented\n", 29);

  } else if (command_name == "read") {
    int64_t end_time = 0;
    int64_t start_time = 0;
    vector<string> patterns;
    for (auto& tok : tokens) {
      if (starts_with(tok, "+start=")) {
        start_time = stoll(tok.substr(7));
      } else if (starts_with(tok, "+end=")) {
        end_time = stoll(tok.substr(5));
      } else {
        patterns.emplace_back(move(tok));
      }
    }

    if (!end_time) {
      end_time = now() / 1000000;
    }
    if (!start_time) {
      start_time = end_time - (60 * 60);
    }

    auto read_results = this->store->read(patterns, start_time, end_time, false);
    for (const auto& result_it : read_results) {
      const string& pattern = result_it.first;
      const auto& series_map = result_it.second;
      evbuffer_add_printf(out_buffer, "[%s]\n", pattern.c_str());

      for (const auto& series_it : series_map) {
        const string& series_name = series_it.first;
        const auto& result = series_it.second;

        if (!result.error.empty()) {
          evbuffer_add_printf(out_buffer, "FAILED: %s (%s)\n", series_name.c_str(), result.error.c_str());
          continue;
        }
        evbuffer_add_printf(out_buffer, "series: %s (start=%" PRId64 ", end=%" PRId64 ", step=%" PRId64 "\n",
            series_name.c_str(), result.start_time, result.end_time, result.step);
        for (const auto& dp : result.data) {
          evbuffer_add_printf(out_buffer, "%" PRId64 " -> %g\n", dp.timestamp, dp.value);
        }
      }
      evbuffer_add(out_buffer, "\n", 1);
    }

  } else {
    throw runtime_error(string_printf("invalid command: %s (try \'help\')", command_name.c_str()));
  }
}

void StreamServer::run_thread(int worker_num) {
  WorkerThread& wt = this->threads[worker_num];

  struct timeval tv = usecs_to_timeval(this->exit_check_usecs);

  struct event* ev = event_new(wt.base.get(), -1, EV_PERSIST,
      &WorkerThread::dispatch_check_for_thread_exit, &wt);
  event_add(ev, &tv);

  event_base_dispatch(wt.base.get());

  event_del(ev);
}

StreamServer::StreamServer(shared_ptr<Store> store, size_t num_threads,
    uint64_t exit_check_usecs) : Server(), should_exit(false),
    exit_check_usecs(exit_check_usecs), store(store) {
  for (size_t x = 0; x < num_threads; x++) {
    this->threads.emplace_back(this, x);
  }
}

void StreamServer::listen(const string& socket_path, Protocol protocol) {
  int fd = ::listen(socket_path, 0, SOMAXCONN);
  log(INFO, "[StreamServer] listening on unix socket %s (protocol %d) on fd %d",
      socket_path.c_str(), protocol, fd);
  this->add_socket(fd, protocol);
}

void StreamServer::listen(const string& addr, int port, Protocol protocol) {
  int fd = ::listen(addr, port, SOMAXCONN);
  string netloc_str = render_netloc(addr, port);
  log(INFO, "[StreamServer] listening on tcp interface %s (protocol %d) on fd %d",
      netloc_str.c_str(), protocol, fd);
  this->add_socket(fd, protocol);
}

void StreamServer::listen(int port, Protocol protocol) {
  this->listen("", port, protocol);
}

void StreamServer::add_socket(int fd, Protocol protocol) {
  this->listen_fd_to_protocol.emplace(fd, protocol);
}

void StreamServer::start() {
  for (auto& wt : this->threads) {
    for (const auto& it : this->listen_fd_to_protocol) {
      struct evconnlistener* listener = evconnlistener_new(wt.base.get(),
          WorkerThread::dispatch_on_listen_accept, &wt, LEV_OPT_REUSEABLE, 0,
          it.first);
      if (!listener) {
        throw runtime_error("can\'t create evconnlistener");
      }
      evconnlistener_set_error_cb(listener, WorkerThread::dispatch_on_listen_error);
      wt.listeners.emplace(listener, evconnlistener_free);
    }
    wt.t = thread(&StreamServer::run_thread, this, wt.worker_num);
  }
}

void StreamServer::schedule_stop() {
  log(INFO, "[StreamServer] scheduling exit for all threads");
  this->should_exit = true;

  for (const auto& it : listen_fd_to_protocol) {
    log(INFO, "[StreamServer] closing listening fd %d", it.first);
    close(it.first);
  }
}

void StreamServer::wait_for_stop() {
  for (auto& wt : this->threads) {
    if (!wt.t.joinable()) {
      continue;
    }
    log(INFO, "[StreamServer] waiting for worker %d to terminate", wt.worker_num);
    wt.t.join();
  }
  log(INFO, "[StreamServer] shutdown complete");
}
