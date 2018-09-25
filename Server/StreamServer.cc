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

#include <algorithm>
#include <iostream>
#include <phosg/Encoding.hh>
#include <phosg/JSON.hh>
#include <phosg/JSONPickle.hh>
#include <phosg/Network.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <thread>

#include "../Store/Whisper.hh"

using namespace std;


StreamServer::Client::Client(int fd, const struct sockaddr& remote_addr,
    Protocol protocol) : remote_addr(remote_addr), fd(fd), protocol(protocol),
    profiler_enabled(false) { }

StreamServer::WorkerThread::WorkerThread(StreamServer* server, int worker_num) :
    server(server), worker_num(worker_num),
    base(event_base_new(), event_base_free), t() {
  this->thread_name = string_printf("StreamServer::run_thread (worker_num=%d)",
      worker_num);
}

void StreamServer::WorkerThread::disconnect_client(struct bufferevent* bev) {
  this->bev_to_client.erase(bev);
  bufferevent_free(bev);
  this->server->client_count--;
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
  BusyThreadGuard g(&this->idle_thread_count);

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
  this->client_count++;

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
  BusyThreadGuard g(&this->idle_thread_count);

  int err = EVUTIL_SOCKET_ERROR();
  log(ERROR, "[StreamServer] failure on listening socket %d: %d (%s)\n",
      evconnlistener_get_fd(listener), err,
      evutil_socket_error_to_string(err));

  event_base_loopexit(wt.base.get(), NULL);
}

void StreamServer::on_client_input(StreamServer::WorkerThread& wt,
    struct bufferevent *bev) {
  BusyThreadGuard g(&this->idle_thread_count);

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
          uint32_t t = (tokens.size() == 3) ? parse_relative_time(tokens[2]) : 0;
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
          this->execute_shell_command(line_data, out_buffer, c, wt.thread_name);
          evbuffer_add_reference(out_buffer, "\n\ncyclone> ", 11, NULL, NULL);
        } catch (const exception& e) {
          evbuffer_add_printf(out_buffer, "FAILED: %s\n\ncyclone> ", e.what());
        }
        free(line_data);
      }
      break;
    }
  }

  if (!data.empty()) {
    ProfilerGuard pg(create_profiler(wt.thread_name, Store::string_for_write(
        data, false, false)));
    for (const auto& it : this->store->write(data, false, false, pg.profiler.get())) {
      if (it.second.empty()) {
        continue;
      }
      log(WARNING, "write failed: %s\n", it.second.c_str());
    }
  }
}

void StreamServer::on_client_error(StreamServer::WorkerThread& wt,
    struct bufferevent *bev, short events) {
  BusyThreadGuard g(&this->idle_thread_count);

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

unordered_map<string, int64_t> StreamServer::get_stats() {
  auto stats = this->Server::get_stats();
  stats.emplace(this->stats_prefix + ".client_count", this->client_count.load());
  return stats;
}

const string HELP_STRING("\
Commands:\n\
  help\n\
    You\'re reading it now.\n\
\n\
  profiler [on|off]\n\
    Get or set the profiling state for this shell connection.\n\
    Profiling can be useful to diagnose performance problems. When profiling is\n\
    enabled, performance debugging information is printed after the result of\n\
    each query run from the current shell connection.\n\
\n\
  find [<pattern> ...]\n\
    Search for metrics matching the given pattern(s). If no patterns are given,\n\
    return the contents of the root directory.\n\
\n\
  read <pattern> [+start=<time>] [+end=<time>]\n\
    Read data from all series that match the given pattern.\n\
    If given, start and end must be absolute epoch timestamps in seconds.\n\
    If start and end are not given, the default is to read the past hour.\n\
    The start and end timestamps may also be relative, like -24h or -30d.\n\
\n\
  write <series> <timestamp> <value> [<timestamp> <value> ...]\n\
    Write one or more datapoints to the given series. If any timestamps are\n\
    zero, the server\'s current time is used. Timestamps may also be relative,\n\
    like -5m or -12h.\n\
\n\
  delete <series> [<series> ...]\n\
    Delete entire series.\n\
\n\
  create <series> <archives> <x-files-factor> <agg-method> [+skip-existing]\n\
      [+truncate]\n\
    Alias for `update-metadata +create`.\n\
\n\
  update-metadata <series> <archives> <x-files-factor> <agg-method> [+create]\n\
      [+skip-existing] [+truncate]\n\
    Create a new series or change metadata of an existing series.\n\
    If a pattern is given for the series name, multiple series may be affected;\n\
    in this case, the +skip-existing and +create options are not valid.\n\
    - <archives> is a comma-separated list of pairs, e.g. 60:90d,3600:5y to\n\
      store minutely data for 90 days and hourly data for 5 years.\n\
    - <x-files-factor> is the proportion of datapoints in each interval that\n\
      must exist for an aggregation to occur to a lower-resolution archive.\n\
    - <agg-method> is the aggregation method to use when updating lower-\n\
      resolution archives. Valid values are average, sum, min, max, and last.\n\
    - +create creates a new series if it doesn\'t exist. (By default, series\n\
      are not created if they don't exist during update-metadata calls.)\n\
    - +skip-existing skips the operation if the series already exists.\n\
    - +truncate recreates the series if it already exists.\n\
\n\
  stats\n\
    Get the current store and server stats.\n\
\n\
  thread-status\n\
    Show what all the worker threads are doing. This list only contains threads\n\
    that have executed at least one query; threads that exist but have done no\n\
    work are omitted.\n\
\n\
  verify start [repair]\n\
    Start a verify procedure. This procedure checks that all keys are in the\n\
    correct substores in all hash multistores. This procedure does not block\n\
    other queries, but causes a performance penalty until it completes. If the\n\
    repair option is given, keys that exist in incorrect stores will be moved\n\
    to the correct store and combined with any existing data in that store.\n\
\n\
  verify cancel\n\
    Cancel the running verify procedure.\n\
\n\
  verify status\n\
    Show the progress of the running verify procedure.\n\
\n\
  read-from-all [on|off]\n\
    Get or set the read-from-all state of the server. This state determines\n\
    whether reads from hash stores go to all substores or only the substore\n\
    which is responsible for the key in question. The read-from-all state is\n\
    automatically enabled during a verify+repair procedure, since it's likely\n\
    that keys exist in the wrong substores if a verify+repair is running.\n\
");

struct ShellProfilerGuard {
  ShellProfilerGuard(struct evbuffer* out_buffer, bool enabled) : profiler(),
      out_buffer(out_buffer), enabled(enabled) { }

  ~ShellProfilerGuard() {
    if (this->enabled) {
      auto result = profiler->output();
      evbuffer_add_printf(out_buffer, "Profiler result: %s\n\n", result.c_str());
    }
  }

  shared_ptr<BaseFunctionProfiler> profiler;
  struct evbuffer* out_buffer;
  bool enabled;
};

void StreamServer::execute_shell_command(const char* line_data,
    struct evbuffer* out_buffer, Client* client, const string& thread_name) {
  auto tokens = split(line_data, ' ');
  if (tokens.size() == 0) {
    throw runtime_error("command is empty; try \'help\'");
  }

  string command_name = move(tokens[0]);
  tokens.erase(tokens.begin());

  ShellProfilerGuard pg(out_buffer, client->profiler_enabled);
  {
    string function_name("(shell) ");
    function_name += line_data;
    if (client->profiler_enabled) {
      pg.profiler = create_profiler(thread_name, function_name, 0);
    } else {
      pg.profiler = create_profiler(thread_name, function_name);
    }
  }

  if (command_name == "help") {
    evbuffer_add_reference(out_buffer, HELP_STRING.data(), HELP_STRING.size(), NULL, NULL);

  } else if (command_name == "stats") {
    if (!tokens.empty()) {
      throw runtime_error("incorrect argument count");
    }

    auto stats = gather_stats(this->store, this->all_servers);

    vector<string> lines;
    for (const auto& it : stats) {
      lines.emplace_back(string_printf("%s = %" PRId64 "\n", it.first.c_str(), it.second));
    }
    sort(lines.begin(), lines.end());

    for (const auto& line : lines) {
      evbuffer_add(out_buffer, line.data(), line.size());
    }

  } else if (command_name == "thread-status") {
    if (!tokens.empty()) {
      throw runtime_error("incorrect argument count");
    }

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

  } else if ((command_name == "update-metadata") || (command_name == "create")) {
    if ((tokens.size() < 4) || (tokens.size() > 7)) {
      throw runtime_error("incorrect argument count");
    }

    bool is_pattern = Store::token_is_pattern(tokens[0]);

    unordered_map<string, SeriesMetadata> metadata_map;
    SeriesMetadata& m = metadata_map[tokens[0]];
    m.archive_args = WhisperArchive::parse_archive_args(tokens[1]);
    m.x_files_factor = stod(tokens[2]);
    if (tokens[3] == "average") {
      m.agg_method = AggregationMethod::Average;
    } else if (tokens[3] == "sum") {
      m.agg_method = AggregationMethod::Sum;
    } else if (tokens[3] == "last") {
      m.agg_method = AggregationMethod::Last;
    } else if (tokens[3] == "min") {
      m.agg_method = AggregationMethod::Min;
    } else if (tokens[3] == "max") {
      m.agg_method = AggregationMethod::Max;
    } else {
      throw runtime_error("unknown aggregation method");
    }

    bool create_new = (command_name == "create");
    Store::UpdateMetadataBehavior update_behavior =
        Store::UpdateMetadataBehavior::Update;
    for (size_t x = 4; x < tokens.size(); x++) {
      if (tokens[x] == "+create") {
        create_new = true;
      } else if (tokens[x] == "+ignore-existing") {
        update_behavior = Store::UpdateMetadataBehavior::Ignore;
      } else if (tokens[x] == "+truncate") {
        update_behavior = Store::UpdateMetadataBehavior::Recreate;
      } else {
        throw runtime_error("unknown argument");
      }
    }

    if (is_pattern && create_new) {
      throw runtime_error("+create may not be used when a pattern is given");
    }
    if (is_pattern && (update_behavior == Store::UpdateMetadataBehavior::Ignore)) {
      throw runtime_error("+skip-existing may not be used when a pattern is given");
    }

    if (is_pattern) {
      auto find_result_map = this->store->find({tokens[0]}, false, pg.profiler.get());
      const auto& find_result = find_result_map.at(tokens[0]);
      if (!find_result.error.empty()) {
        throw runtime_error("find failed: " + find_result.error);
      }
      pg.profiler->checkpoint("store_find");

      // copy the metadata to all the found keys
      auto source_metadata_it = metadata_map.find(tokens[0]);
      const auto& source_metadata = source_metadata_it->second;
      for (const auto& result : find_result.results) {
        if (ends_with(result, ".*")) {
          continue;
        }
        metadata_map.emplace(result, source_metadata);
      }
      metadata_map.erase(source_metadata_it);
      pg.profiler->checkpoint("generate_metadata_map");
    }

    auto result = this->store->update_metadata(metadata_map, create_new,
        update_behavior, false, false, pg.profiler.get());

    for (const auto& result_it : result) {
      if (result_it.second.empty()) {
        evbuffer_add_printf(out_buffer, "[%s] success\n",
            result_it.first.c_str());
      } else {
        evbuffer_add_printf(out_buffer, "[%s] FAILED: %s\n",
            result_it.first.c_str(), result_it.second.c_str());
      }
    }

  } else if (command_name == "delete") {
    if (tokens.empty()) {
      throw runtime_error("no series given");
    }

    auto result = this->store->delete_series(tokens, false, pg.profiler.get());

    for (const auto& it : result) {
      evbuffer_add_printf(out_buffer, "[%s] %" PRId64 " series deleted\n",
          it.first.c_str(), it.second);
    }

  } else if (command_name == "find") {
    if (tokens.empty()) {
      tokens.emplace_back("*");
    }

    auto find_result = this->store->find(tokens, false, pg.profiler.get());

    if (find_result.size() == 1) {
      auto& result = find_result.begin()->second;
      if (!result.error.empty()) {
        evbuffer_add_printf(out_buffer, "FAILED: %s\n", result.error.c_str());
      } else {
        sort(result.results.begin(), result.results.end());
        for (const auto& item : result.results) {
          evbuffer_add(out_buffer, item.data(), item.size());
          evbuffer_add(out_buffer, "\n", 1);
        }
      }

    } else {
      for (auto& it : find_result) {
        if (!it.second.error.empty()) {
          evbuffer_add_printf(out_buffer, "[%s] FAILED: %s\n", it.first.c_str(), it.second.error.c_str());
          continue;
        }
        sort(it.second.results.begin(), it.second.results.end());
        for (const auto& item : it.second.results) {
          evbuffer_add_printf(out_buffer, "[%s] %s\n", it.first.c_str(), item.c_str());
        }
      }
    }

  } else if (command_name == "write") {
    if (tokens.size() < 3) {
      throw runtime_error("not enough arguments");
    }
    if (!(tokens.size() & 1)) {
      throw runtime_error("incorrect number of arguments");
    }

    unordered_map<string, vector<Datapoint>> write_map;
    vector<Datapoint>& data = write_map[tokens[0]];
    int64_t now_timestamp = 0;
    for (size_t x = 1; x < tokens.size(); x += 2) {
      data.emplace_back();
      auto& dp = data.back();
      dp.timestamp = parse_relative_time(tokens[x]);
      dp.value = stod(tokens[x + 1]);
      if (dp.timestamp == 0) {
        if (now_timestamp == 0) {
          now_timestamp = now() / 1000000;
        }
        dp.timestamp = now_timestamp;
      }
    }

    auto result = this->store->write(write_map, false, false, pg.profiler.get());

    for (const auto& result_it : result) {
      if (result_it.second.empty()) {
        evbuffer_add_printf(out_buffer, "[%s] success\n",
            result_it.first.c_str());
      } else {
        evbuffer_add_printf(out_buffer, "[%s] FAILED: %s\n",
            result_it.first.c_str(), result_it.second.c_str());
      }
    }

  } else if (command_name == "read") {
    if (tokens.empty()) {
      throw runtime_error("no series given");
    }

    int64_t end_time = 0;
    int64_t start_time = 0;
    vector<string> patterns;
    for (auto& tok : tokens) {
      if (starts_with(tok, "+start=")) {
        start_time = parse_relative_time(tok.substr(7));
      } else if (starts_with(tok, "+end=")) {
        end_time = parse_relative_time(tok.substr(5));
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

    auto read_results = this->store->read(patterns, start_time, end_time, false,
        pg.profiler.get());

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
        if (result.step == 0) {
          evbuffer_add_printf(out_buffer, "series: %s (no data)\n",
              series_name.c_str());
        } else {
          evbuffer_add_printf(out_buffer, "series: %s (start=%" PRId64 ", end=%" PRId64 ", step=%" PRId64 ")\n",
              series_name.c_str(), result.start_time, result.end_time, result.step);
          for (const auto& dp : result.data) {
            string time_str = format_time(dp.timestamp * 1000000);
            evbuffer_add_printf(out_buffer, "%" PRId64 " (%s) -> %g\n",
                dp.timestamp, time_str.c_str(), dp.value);
          }
        }
      }
      evbuffer_add(out_buffer, "\n", 1);
    }

  } else if (command_name == "verify") {
    if (this->hash_stores.empty()) {
      throw runtime_error("there are no hash stores");
    }
    if (this->hash_stores.size() > 1) {
      throw runtime_error("there are multiple hash stores");
    }

    if (tokens.size() > 2) {
      throw runtime_error("too many arguments");
    }
    if ((tokens.size() == 2) && ((tokens[1] != "repair") || (tokens[0] != "start"))) {
      throw runtime_error("invalid arguments");
    }
    if (tokens.size() < 1) {
      throw runtime_error("not enough arguments");
    }

    auto hash_store = this->hash_stores[0];

    if (tokens[0] == "start") {
      bool repair = (tokens.size() == 2);
      bool ret = hash_store->start_verify(repair);
      evbuffer_add_printf(out_buffer, "%s %s\n",
          repair ? "verify+repair" : "verify", ret ? "started" : "not started");

    } else if (tokens[0] == "cancel") {
      bool ret = hash_store->cancel_verify();
      evbuffer_add_printf(out_buffer, "verify %s\n",
          ret ? "cancelled" : "not cancelled");

    } else if (tokens[0] == "status") {
      const auto& progress = hash_store->get_verify_progress();
      int64_t start_time = progress.start_time;
      int64_t end_time = progress.end_time;

      if (start_time == end_time) {
        evbuffer_add_printf(out_buffer, "verify was never run\n");

      } else {
        string start_time_str = format_time(start_time);
        evbuffer_add_printf(out_buffer, "%s %s\n",
            progress.repair ? "verify+repair" : "verify",
            progress.in_progress() ? "in progress" : "completed");
        evbuffer_add_printf(out_buffer, "started at %" PRId64 " (%s)\n",
            start_time, start_time_str.c_str());
        if (end_time) {
          string end_time_str = format_time(progress.end_time);
          evbuffer_add_printf(out_buffer, "completed at %" PRId64 " (%s)\n",
              end_time, end_time_str.c_str());
        }
        evbuffer_add_printf(out_buffer, "%" PRId64 " of %" PRId64 " keys moved\n",
            progress.keys_moved.load(), progress.keys_examined.load());
        evbuffer_add_printf(out_buffer, "%" PRId64 " read_all errors\n",
            progress.read_all_errors.load());
        evbuffer_add_printf(out_buffer, "%" PRId64 " update_metadata errors\n",
            progress.update_metadata_errors.load());
        evbuffer_add_printf(out_buffer, "%" PRId64 " write errors\n",
            progress.write_errors.load());
        evbuffer_add_printf(out_buffer, "%" PRId64 " delete errors\n",
            progress.delete_errors.load());
        evbuffer_add_printf(out_buffer, "%" PRId64 " find queries executed\n",
            progress.find_queries_executed.load());
      }

    } else {
      throw runtime_error("invalid subcommand");
    }

  } else if (command_name == "read-from-all") {
    if (this->hash_stores.empty()) {
      throw runtime_error("there are no hash stores");
    }
    if (this->hash_stores.size() > 1) {
      throw runtime_error("there are multiple hash stores");
    }

    if (tokens.size() > 1) {
      throw runtime_error("too many arguments");
    }
    if ((tokens.size() == 1) && (tokens[0] != "on") && (tokens[0] != "off")) {
      throw runtime_error("invalid argument");
    }

    auto hash_store = this->hash_stores[0];

    if (tokens.size() == 0) {
      evbuffer_add_printf(out_buffer, "read-from-all is %s\n",
          hash_store->get_read_from_all() ? "enabled" : "disabled");

    } else {
      bool enable = (tokens[0] == "on");
      bool prev_read_from_all = hash_store->set_read_from_all(enable);
      evbuffer_add_printf(out_buffer, "read-from-all is %s (was %s)\n",
          enable ? "enabled" : "disabled",
          prev_read_from_all ? "enabled" : "disabled");
    }

  } else if (command_name == "profiler") {
    if (tokens.size() > 1) {
      throw runtime_error("too many arguments");
    }
    if ((tokens.size() == 1) && (tokens[0] != "on") && (tokens[0] != "off")) {
      throw runtime_error("invalid argument");
    }

    if (tokens.size() == 0) {
      evbuffer_add_printf(out_buffer, "profiler is %s\n",
          client->profiler_enabled ? "enabled" : "disabled");

    } else {
      bool enable = (tokens[0] == "on");
      evbuffer_add_printf(out_buffer, "profiler is %s (was %s)\n",
          enable ? "enabled" : "disabled",
          client->profiler_enabled ? "enabled" : "disabled");
      client->profiler_enabled = enable;
    }

  } else {
    throw runtime_error(string_printf("invalid command: %s (try \'help\')", command_name.c_str()));
  }
}

void StreamServer::run_thread(int worker_num) {
  WorkerThread& wt = this->threads[worker_num];

  struct timeval tv = usecs_to_timeval(2000000);

  struct event* ev = event_new(wt.base.get(), -1, EV_PERSIST,
      &WorkerThread::dispatch_check_for_thread_exit, &wt);
  event_add(ev, &tv);

  event_base_dispatch(wt.base.get());

  event_del(ev);
}

StreamServer::StreamServer(shared_ptr<Store> store,
    const vector<shared_ptr<ConsistentHashMultiStore>>& hash_stores,
    size_t num_threads) :
    Server("stream_server", num_threads), should_exit(false),
    client_count(0), store(store), hash_stores(hash_stores) {
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
