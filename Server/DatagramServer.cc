#include "DatagramServer.hh"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <iostream>
#include <thread>

#include <phosg/Concurrency.hh>
#include <phosg/Network.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>

#include "../Store/Utils/Errors.hh"

using namespace std;


DatagramServer::WorkerThread::WorkerThread(DatagramServer* server,
    int worker_num) : server(server), worker_num(worker_num),
    base(event_base_new(), event_base_free), fd_to_event(), t() {
  this->thread_name = string_printf("DatagramServer::run_thread-%016" PRIX64,
      reinterpret_cast<uint64_t>(this->base.get()));
}

void DatagramServer::WorkerThread::check_for_thread_exit(evutil_socket_t fd,
    short what) {
  if (this->server->should_exit) {
    event_base_loopexit(this->base.get(), NULL);
  }
}

void DatagramServer::dispatch_on_client_input(int fd, short events, void *ctx) {
  auto* wt = reinterpret_cast<DatagramServer::WorkerThread*>(ctx);
  wt->server->on_client_input(wt, fd, events);
}

void DatagramServer::dispatch_check_for_thread_exit(
    evutil_socket_t fd, short what, void* ctx) {
  auto* wt = reinterpret_cast<DatagramServer::WorkerThread*>(ctx);
  wt->check_for_thread_exit(fd, what);
}

void DatagramServer::on_client_input(WorkerThread* wt, int fd, short events) {
  BusyThreadGuard g(&this->idle_thread_count);

  struct sockaddr_storage ss;
  socklen_t ss_len = sizeof(struct sockaddr_storage);

  for (;;) {
    string buffer(10240, 0);
    ss_len = sizeof(struct sockaddr_in6);
    ssize_t bytes_received = recvfrom(fd, const_cast<char*>(buffer.data()),
        buffer.size(), 0, (struct sockaddr*)&ss, &ss_len);

    if (bytes_received == -1) {
      if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
        string error_str = string_for_error(errno);
        log(WARNING, "[DatagramServer] failed to read message from udp socket %d: %s",
            fd, error_str.c_str());
      }
      break;
    }
    buffer.resize(bytes_received);
    if (buffer.empty()) {
      continue;
    }

    // data = {key: {timestamp: value}}
    unordered_map<string, Series> data;

    auto lines = split(buffer, '\n');
    for (const auto& line : lines) {
      if (line.empty()) {
        continue;
      }

      try {
        // keyname value timestamp_secs
        auto tokens = split(buffer, ' ');
        if (tokens.size() < 2 || tokens.size() > 3) {
          log(WARNING, "[DatagramServer] received bad message from udp socket %d: %s",
              fd, buffer.c_str());
          continue;
        }

        uint32_t t = (tokens.size() == 3) ? parse_relative_time(tokens[2]) : 0;
        if (t == 0) {
          t = time(NULL);
        }

        auto& series = data[tokens[0]];
        series.emplace_back();
        series.back().timestamp = t;
        series.back().value = stod(tokens[1]);

      } catch (const exception& e) {
        log(WARNING, "[DatagramServer] received bad message from udp socket %d: %s (%s)",
            fd, buffer.c_str(), e.what());
      }
    }

    // send it to the store
    ProfilerGuard pg(create_profiler(wt->thread_name, Store::string_for_write(
        data, false, false)));
    auto task = this->store->write(NULL, data, false, false, pg.profiler.get());
    for (const auto& it : task->value()) {
      if (it.second.description.empty()) {
        continue;
      }
      string error_str = string_for_error(it.second);
      log(WARNING, "write failed: %s\n", error_str.c_str());
    }
  }
}

void DatagramServer::run_thread(int worker_num) {
  WorkerThread& ti = this->threads[worker_num];

  struct timeval tv = usecs_to_timeval(2000000);
  struct event* ev = event_new(ti.base.get(), -1, EV_PERSIST,
      &DatagramServer::dispatch_check_for_thread_exit, &ti);
  event_add(ev, &tv);

  event_base_dispatch(ti.base.get());

  event_del(ev);
}

DatagramServer::DatagramServer(shared_ptr<Store> store, size_t num_threads) :
    Server("datagram_server", num_threads), should_exit(false), store(store) {
  for (size_t x = 0; x < num_threads; x++) {
    this->threads.emplace_back(this, x);
  }
}

void DatagramServer::listen(const string& socket_path) {
  int fd = ::listen(socket_path, 0, 0);
  log(INFO, "[DatagramServer] listening on unix socket %s on fd %d",
      socket_path.c_str(), fd);
  this->add_socket(fd);
}

void DatagramServer::listen(const string& addr, int port) {
  int fd = ::listen(addr, port, 0);
  string netloc_str = render_netloc(addr, port);
  log(INFO, "[DatagramServer] listening on udp interface %s on fd %d",
      netloc_str.c_str(), fd);
  this->add_socket(fd);
}

void DatagramServer::listen(int port) {
  this->listen("", port);
}

void DatagramServer::add_socket(int fd) {
  this->fds.emplace(fd);
}

void DatagramServer::start() {
  for (auto& ti : this->threads) {
    for (const auto& fd : this->fds) {
      auto ev = ti.fd_to_event.emplace(piecewise_construct,
          forward_as_tuple(fd),
          forward_as_tuple(event_new(ti.base.get(), fd,
              EV_READ | EV_PERSIST, DatagramServer::dispatch_on_client_input,
              &ti), event_free)).first->second.get();
      event_add(ev, NULL);
    }
    ti.t = thread(&DatagramServer::run_thread, this, ti.worker_num);
  }
}

void DatagramServer::schedule_stop() {
  log(INFO, "[DatagramServer] scheduling exit for all threads");
  this->should_exit = true;

  for (const auto& fd : fds) {
    log(INFO, "[DatagramServer] closing fd %d", fd);
    close(fd);
  }
}

void DatagramServer::wait_for_stop() {
  for (auto& ti : this->threads) {
    if (!ti.t.joinable()) {
      continue;
    }
    log(INFO, "[DatagramServer] waiting for worker %d to terminate", ti.worker_num);
    ti.t.join();
  }
}
