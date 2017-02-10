#include <signal.h>

#include <phosg/JSON.hh>
#include <phosg/Network.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>

#include "Store/Store.hh"
#include "Store/CachedDiskStore.hh"
#include "Store/DiskStore.hh"
#include "Store/RemoteStore.hh"
#include "Store/ConsistentHashMultiStore.hh"
#include "Store/MultiStore.hh"
#include "Store/WriteBufferStore.hh"
#include "Store/EmptyStore.hh"
#include "Store/ReadOnlyStore.hh"

#include "Server/CycloneHTTPServer.hh"
#include "Server/ThriftServer.hh"
#include "Server/StreamServer.hh"

using namespace std;


struct Options {
  string filename;

  vector<pair<string, int>> http_listen_addrs;
  vector<pair<string, int>> line_listen_addrs;
  vector<pair<string, int>> pickle_listen_addrs;
  int thrift_port;
  size_t http_threads;
  size_t stream_threads;
  size_t thrift_threads;
  uint64_t exit_check_usecs;
  uint64_t stats_report_usecs;
  size_t open_file_cache_size;

  shared_ptr<Store> store;

  vector<pair<string, SeriesMetadata>> autocreate_rules;

  Options(const string& filename, bool is_reload = false) : filename(filename) {
    auto json = JSONObject::load(this->filename);

    // load mutable configuration
    try {
      this->stats_report_usecs = (*json)["stats_report_usecs"]->as_int();
    } catch (const JSONObject::key_error& e) {
      this->stats_report_usecs = 60000000; // 1 minute
    }
    try {
      this->open_file_cache_size = (*json)["open_file_cache_size"]->as_int();
    } catch (const JSONObject::key_error& e) {
      this->open_file_cache_size = 1024;
    }

    this->autocreate_rules = this->parse_autocreate_rules((*json)["autocreate_rules"]);

    // load immutable configuration
    if (!is_reload) {
      this->http_listen_addrs = this->parse_listen_addrs_list(
          (*json)["http_listen"]);
      this->line_listen_addrs = this->parse_listen_addrs_list(
          (*json)["line_listen"]);
      this->pickle_listen_addrs = this->parse_listen_addrs_list(
          (*json)["pickle_listen"]);
      this->thrift_port = (*json)["thrift_port"]->as_int();
      this->http_threads = (*json)["http_threads"]->as_int();
      this->stream_threads = (*json)["stream_threads"]->as_int();
      this->thrift_threads = (*json)["thrift_threads"]->as_int();
      try {
        this->exit_check_usecs = (*json)["exit_check_usecs"]->as_int();
      } catch (const JSONObject::key_error& e) {
        this->exit_check_usecs = 2000000; // 2 seconds
      }

      this->store = this->parse_store_config((*json)["store_config"]);
      this->store->set_autocreate_rules(this->autocreate_rules);
    }
  }

  static vector<pair<string, int>> parse_listen_addrs_list(
      shared_ptr<JSONObject> list) {
    vector<pair<string, int>> ret;
    for (const auto& item : list->as_list()) {
      try {
        int port = item->as_int();
        ret.emplace_back(make_pair("", port));
      } catch (const JSONObject::type_error& e) {
        string netloc = item->as_string();
        size_t colon_offset = netloc.find(':');
        if (colon_offset == string::npos) {
          ret.emplace_back(make_pair(netloc, 0));
        } else {
          int port = stoi(netloc.substr(colon_offset + 1));
          ret.emplace_back(make_pair(netloc.substr(0, colon_offset), port));
        }
      }
    }
    return ret;
  }

  static shared_ptr<Store> parse_store_config(
      shared_ptr<JSONObject> store_config) {

    string type = (*store_config)["type"]->as_string();

    if (!type.compare("hash")) {
      unordered_map<string, shared_ptr<Store>> stores;
      for (auto& it : (*store_config)["stores"]->as_dict()) {
        stores.emplace(it.first, parse_store_config(it.second));
      }
      return shared_ptr<Store>(new ConsistentHashMultiStore(stores));
    }

    if (!type.compare("multi")) {
      unordered_map<string, shared_ptr<Store>> stores;
      for (auto& it : (*store_config)["stores"]->as_dict()) {
        stores.emplace(it.first, parse_store_config(it.second));
      }
      return shared_ptr<Store>(new MultiStore(stores));
    }

    if (!type.compare("remote")) {
      string hostname = (*store_config)["hostname"]->as_string();
      int port = (*store_config)["port"]->as_int();
      int64_t connection_cache_count = 0;
      try {
        connection_cache_count = (*store_config)["connection_cache_count"]->as_int();
      } catch (const JSONObject::key_error& e) { }
      return shared_ptr<Store>(new RemoteStore(hostname, port, connection_cache_count));
    }

    if (!type.compare("disk")) {
      string directory = (*store_config)["directory"]->as_string();
      return shared_ptr<Store>(new DiskStore(directory));
    }

    if (!type.compare("cached_disk")) {
      string directory = (*store_config)["directory"]->as_string();
      return shared_ptr<Store>(new CachedDiskStore(directory));
    }

    if (!type.compare("write_buffer")) {
      size_t num_write_threads = (*store_config)["num_write_threads"]->as_int();
      size_t batch_size = (*store_config)["batch_size"]->as_int();
      shared_ptr<Store> substore = parse_store_config((*store_config)["substore"]);
      return shared_ptr<Store>(new WriteBufferStore(substore, num_write_threads,
          batch_size));
    }

    if (!type.compare("read_only")) {
      shared_ptr<Store> substore = parse_store_config((*store_config)["substore"]);
      return shared_ptr<Store>(new ReadOnlyStore(substore));
    }

    if (!type.compare("empty")) {
      return shared_ptr<Store>(new EmptyStore());
    }

    throw runtime_error("invalid store config");
  }

  static vector<pair<string, SeriesMetadata>> parse_autocreate_rules(
      shared_ptr<const JSONObject> json) {
    vector<pair<string, SeriesMetadata>> ret;
    for (const auto& item : json->as_list()) {
      const auto& item_list = item->as_list();
      if (item_list.size() != 4) {
        auto item_ser = item->serialize();
        log(WARNING, "autocreate rules contain bad entry: %s", item_ser.c_str());
        continue;
      }

      SeriesMetadata m;
      m.archive_args = WhisperArchive::parse_archive_args(item_list[1]->as_string());
      m.x_files_factor = item_list[2]->as_float();
      try {
        m.agg_method = item_list[3]->as_int();
      } catch (const JSONObject::type_error& e) {
        string s = item_list[3]->as_string();
        if (s == "average") {
          m.agg_method = AggregationMethod::Average;
        } else if (s == "sum") {
          m.agg_method = AggregationMethod::Sum;
        } else if (s == "last") {
          m.agg_method = AggregationMethod::Last;
        } else if (s == "min") {
          m.agg_method = AggregationMethod::Min;
        } else if (s == "max") {
          m.agg_method = AggregationMethod::Max;
        } else {
          auto item_ser = item->serialize();
          log(WARNING, "autocreate rules contain bad entry: %s", item_ser.c_str());
          continue;
        }
      }
      ret.emplace_back(make_pair(item_list[0]->as_string(), m));
    }
    return ret;

  }
};


bool should_exit = false;
bool should_flush = false;

void signal_handler(int signum) {
  if ((signum == SIGINT) || (signum == SIGTERM)) {
    should_exit = true;
  }
  if (signum == SIGUSR1) {
    should_flush = true;
  }
}


int main(int argc, char **argv) {

  log(INFO, "fuzziqer software cyclone");

  string config_filename = (argc < 2) ? "cyclone.conf.json" : argv[1];
  uint64_t config_modification_time = stat(config_filename).st_mtime;
  Options opt(config_filename);

  WhisperArchive::set_files_lru_max_size(opt.open_file_cache_size);

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  signal(SIGUSR1, signal_handler);

  vector<shared_ptr<Server>> servers;
  if (!opt.http_listen_addrs.empty()) {
    shared_ptr<HTTPServer> s(new CycloneHTTPServer(opt.store, opt.http_threads,
        opt.exit_check_usecs));
    for (const auto& addr : opt.http_listen_addrs) {
      if (addr.second == 0) {
        s->listen(addr.first);
      } else {
        s->listen(addr.first, addr.second);
      }
    }
    servers.emplace_back(s);
  }

  if (!opt.line_listen_addrs.empty() || !opt.pickle_listen_addrs.empty()) {
    shared_ptr<StreamServer> s(new StreamServer(opt.store, opt.stream_threads,
        opt.exit_check_usecs));
    for (const auto& addr : opt.line_listen_addrs) {
      if (addr.second == 0) {
        s->listen(addr.first, false);
      } else {
        s->listen(addr.first, addr.second, false);
      }
    }
    for (const auto& addr : opt.pickle_listen_addrs) {
      if (addr.second == 0) {
        s->listen(addr.first, true);
      } else {
        s->listen(addr.first, addr.second, true);
      }
    }
    servers.emplace_back(s);
  }

  if (opt.thrift_port) {
    servers.emplace_back(new ThriftServer(opt.store, opt.thrift_port, opt.thrift_threads));
  }

  for (auto& it : servers) {
    it->start();
  }

  // TODO: also check the config file for updates periodically

  // collect stats periodically and report them to the store
  uint64_t next_stats_report_time = opt.stats_report_usecs ?
      (now() + opt.stats_report_usecs) : 0;
  uint64_t next_config_check_time = now() + 10000000;
  while (!should_exit) {

    if (now() >= next_config_check_time) {
      uint64_t t = stat(config_filename).st_mtime;
      if (t > config_modification_time) {
        log(INFO, "configuration file was modified; reloading");

        Options new_opt(config_filename);
        if (new_opt.open_file_cache_size != opt.open_file_cache_size) {
          log(INFO, "open_file_cache_size changed from %lu to %lu",
              opt.open_file_cache_size, new_opt.open_file_cache_size);
          opt.open_file_cache_size = new_opt.open_file_cache_size;
          WhisperArchive::set_files_lru_max_size(opt.open_file_cache_size);
        }

        if (new_opt.stats_report_usecs != opt.stats_report_usecs) {
          log(INFO, "stats_report_usecs changed from %lu to %lu",
              opt.stats_report_usecs, new_opt.stats_report_usecs);
          opt.stats_report_usecs = new_opt.stats_report_usecs;
          next_stats_report_time = opt.stats_report_usecs ? now() : 0;
        }

        if (new_opt.autocreate_rules != opt.autocreate_rules) {
          log(INFO, "autocreate rules changed");
          opt.autocreate_rules = new_opt.autocreate_rules;
          opt.store->set_autocreate_rules(opt.autocreate_rules);
        }

        config_modification_time = t;
      }

      next_config_check_time = now() + 10000000;
    }

    if (next_stats_report_time && (now() >= next_stats_report_time)) {
      string hostname = gethostname();
      auto stats = opt.store->get_stats(true);

      unordered_map<string, Series> data_to_write;

      uint64_t n = now() / 1000000;
      for (const auto& stat : stats) {
        string key = string_printf("cyclone.%s.%s", hostname.c_str(),
            stat.first.c_str());
        auto& series = data_to_write[key];
        series.emplace_back();
        series.back().timestamp = n;
        series.back().value = stat.second;
      }
      string key = string_printf("cyclone.%s.open_file_cache_size",
          hostname.c_str(), WhisperArchive::get_files_lru_size());

      opt.store->write(data_to_write);

      next_stats_report_time += opt.stats_report_usecs;
    }

    if (should_flush) {
      opt.store->flush();
      should_flush = false;
    }

    uint64_t wakeup_time = next_config_check_time;
    if (next_stats_report_time && (wakeup_time > next_stats_report_time)) {
      wakeup_time = next_stats_report_time;
    }
    usleep(wakeup_time - now());
  }

  signal(SIGINT, SIG_DFL);
  signal(SIGTERM, SIG_DFL);
  signal(SIGUSR1, SIG_DFL);

  for (auto& it : servers) {
    it->schedule_stop();
  }

  for (auto& it : servers) {
    it->wait_for_stop();
  }

  opt.store->flush();

  return 0;
}
