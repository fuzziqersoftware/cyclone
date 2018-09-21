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
#include "Store/QueryStore.hh"

#include "Server/CycloneHTTPServer.hh"
#include "Server/ThriftServer.hh"
#include "Server/StreamServer.hh"
#include "Server/DatagramServer.hh"

using namespace std;


struct Options {
  string filename;

  vector<pair<string, int>> http_listen_addrs;
  vector<pair<string, int>> line_stream_listen_addrs;
  vector<pair<string, int>> line_datagram_listen_addrs;
  vector<pair<string, int>> pickle_listen_addrs;
  vector<pair<string, int>> shell_listen_addrs;
  int thrift_port;
  size_t http_threads;
  size_t stream_threads;
  size_t datagram_threads;
  size_t thrift_threads;
  uint64_t stats_report_usecs;
  size_t open_file_cache_size;
  size_t prepopulate_depth;
  int64_t profiler_threshold_usecs;
  int64_t internal_profiler_threshold_usecs;
  int log_level;

  shared_ptr<JSONObject> store_config;
  shared_ptr<Store> store;
  vector<shared_ptr<ConsistentHashMultiStore>> hash_stores;

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
    try {
      this->prepopulate_depth = (*json)["prepopulate_depth"]->as_int();
    } catch (const JSONObject::key_error& e) {
      this->prepopulate_depth = 0;
    }
    try {
      this->profiler_threshold_usecs = (*json)["profiler_threshold_usecs"]->as_int();
    } catch (const JSONObject::key_error& e) {
      this->profiler_threshold_usecs = -1;
    }
    try {
      this->internal_profiler_threshold_usecs = (*json)["internal_profiler_threshold_usecs"]->as_int();
    } catch (const JSONObject::key_error& e) {
      this->internal_profiler_threshold_usecs = -1;
    }
    try {
      this->log_level = (*json)["log_level"]->as_int();
    } catch (const JSONObject::key_error& e) {
      this->log_level = WARNING;
    }
    this->autocreate_rules = this->parse_autocreate_rules((*json)["autocreate_rules"]);
    this->store_config = (*json)["store_config"];

    // load immutable configuration
    if (!is_reload) {
      this->http_listen_addrs = this->parse_listen_addrs_list(
          (*json)["http_listen"]);
      this->line_stream_listen_addrs = this->parse_listen_addrs_list(
          (*json)["line_stream_listen"]);
      this->line_datagram_listen_addrs = this->parse_listen_addrs_list(
          (*json)["line_datagram_listen"]);
      this->pickle_listen_addrs = this->parse_listen_addrs_list(
          (*json)["pickle_listen"]);
      this->shell_listen_addrs = this->parse_listen_addrs_list(
          (*json)["shell_listen"]);
      this->thrift_port = (*json)["thrift_port"]->as_int();
      this->http_threads = (*json)["http_threads"]->as_int();
      this->stream_threads = (*json)["stream_threads"]->as_int();
      this->datagram_threads = (*json)["datagram_threads"]->as_int();
      this->thrift_threads = (*json)["thrift_threads"]->as_int();

      auto parse_ret = this->parse_store_config(this->store_config);
      this->store = parse_ret.first;
      this->hash_stores = move(parse_ret.second);
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

  pair<shared_ptr<Store>, vector<shared_ptr<ConsistentHashMultiStore>>> parse_store_config(
      shared_ptr<JSONObject> store_config) {

    string type = (*store_config)["type"]->as_string();

    if (!type.compare("query")) {
      auto parse_ret = this->parse_store_config((*store_config)["substore"]);
      return make_pair(shared_ptr<Store>(new QueryStore(parse_ret.first)), parse_ret.second);
    }

    if (!type.compare("hash")) {
      vector<shared_ptr<ConsistentHashMultiStore>> hash_stores;
      unordered_map<string, shared_ptr<Store>> stores;

      for (auto& it : (*store_config)["stores"]->as_dict()) {
        auto parse_ret = this->parse_store_config(it.second);
        stores.emplace(it.first, parse_ret.first);
        hash_stores.insert(hash_stores.end(), parse_ret.second.begin(), parse_ret.second.end());
      }

      int64_t precision = -100;
      try {
        precision = (*store_config)["precision"]->as_int();
      } catch (const JSONObject::key_error& e) { }

      shared_ptr<ConsistentHashMultiStore> store(new ConsistentHashMultiStore(stores, precision));
      hash_stores.emplace_back(store);
      return make_pair(store, hash_stores);
    }

    if (!type.compare("multi")) {
      vector<shared_ptr<ConsistentHashMultiStore>> hash_stores;
      unordered_map<string, shared_ptr<Store>> stores;

      for (auto& it : (*store_config)["stores"]->as_dict()) {
        auto parse_ret = this->parse_store_config(it.second);
        stores.emplace(it.first, parse_ret.first);
        hash_stores.insert(hash_stores.end(), parse_ret.second.begin(), parse_ret.second.end());
      }

      return make_pair(shared_ptr<Store>(new MultiStore(stores)), hash_stores);
    }

    if (!type.compare("remote")) {
      string hostname = (*store_config)["hostname"]->as_string();
      int port = (*store_config)["port"]->as_int();
      int64_t connection_limit = (*store_config)["connection_limit"]->as_int();
      return make_pair(
          shared_ptr<Store>(new RemoteStore(hostname, port, connection_limit)),
          vector<shared_ptr<ConsistentHashMultiStore>>());
    }

    if (!type.compare("disk")) {
      string directory = (*store_config)["directory"]->as_string();
      return make_pair(shared_ptr<Store>(new DiskStore(directory)),
          vector<shared_ptr<ConsistentHashMultiStore>>());
    }

    if (!type.compare("cached_disk")) {
      string directory = (*store_config)["directory"]->as_string();
      int64_t directory_limit = (*store_config)["directory_limit"]->as_int();
      int64_t file_limit = (*store_config)["file_limit"]->as_int();
      return make_pair(shared_ptr<Store>(new CachedDiskStore(directory, directory_limit, file_limit)),
          vector<shared_ptr<ConsistentHashMultiStore>>());
    }

    if (!type.compare("write_buffer")) {
      size_t num_write_threads = (*store_config)["num_write_threads"]->as_int();
      size_t batch_size = (*store_config)["batch_size"]->as_int();
      size_t max_update_metadatas_per_second = (*store_config)["max_update_metadatas_per_second"]->as_int();
      size_t max_write_batches_per_second = (*store_config)["max_write_batches_per_second"]->as_int();
      size_t disable_rate_limit_for_queue_length = (*store_config)["disable_rate_limit_for_queue_length"]->as_int();
      bool merge_find_patterns = (*store_config)["merge_find_patterns"]->as_bool();
      auto parse_ret = this->parse_store_config((*store_config)["substore"]);
      return make_pair(
          shared_ptr<Store>(new WriteBufferStore(parse_ret.first,
            num_write_threads, batch_size, max_update_metadatas_per_second,
            max_write_batches_per_second, disable_rate_limit_for_queue_length,
            merge_find_patterns)),
          parse_ret.second);
    }

    if (!type.compare("read_only")) {
      auto parse_ret = this->parse_store_config((*store_config)["substore"]);
      return make_pair(shared_ptr<Store>(new ReadOnlyStore(parse_ret.first)), parse_ret.second);
    }

    if (!type.compare("empty")) {
      return make_pair(shared_ptr<Store>(new EmptyStore()),
          vector<shared_ptr<ConsistentHashMultiStore>>());
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

void apply_store_config(shared_ptr<const JSONObject> orig_store_config,
    shared_ptr<const JSONObject> new_store_config,
    shared_ptr<Store> base, std::string prefix = "base") {
  try {
    string orig_type = (*orig_store_config)["type"]->as_string();
    string new_type = (*new_store_config)["type"]->as_string();
    if (orig_type != new_type) {
      log(ERROR, "store type in configuration has changed from %s to %s; ignoring",
          orig_type.c_str(), new_type.c_str());
      return;
    }

    if ((new_type == "disk") || (new_type == "cached_disk")) {
      DiskStore* d = reinterpret_cast<DiskStore*>(base.get());

      if (new_type == "cached_disk") {
        CachedDiskStore* cd = reinterpret_cast<CachedDiskStore*>(d);

        size_t new_directory_limit = (*new_store_config)["directory_limit"]->as_int();
        if (new_directory_limit != cd->get_directory_limit()) {
          log(INFO, "%s.cached_disk.directory_limit changed from %" PRId64 " to %" PRId64,
              prefix.c_str(), cd->get_directory_limit(), new_directory_limit);
          cd->set_directory_limit(new_directory_limit);
        }

        size_t new_file_limit = (*new_store_config)["file_limit"]->as_int();
        if (new_file_limit != cd->get_file_limit()) {
          log(INFO, "%s.cached_disk.file_limit changed from %" PRId64 " to %" PRId64,
              prefix.c_str(), cd->get_file_limit(), new_file_limit);
          cd->set_file_limit(new_file_limit);
        }
      }

      string new_directory = (*new_store_config)["directory"]->as_string();
      if (new_directory != d->get_directory()) {
        log(INFO, "%s.%s.directory changed from %s to %s", prefix.c_str(),
            new_type.c_str(), d->get_directory().c_str(),
            new_directory.c_str());
        d->set_directory(new_directory);
      }

    } else if (new_type == "write_buffer") {
      WriteBufferStore* wb = reinterpret_cast<WriteBufferStore*>(base.get());

      {
        size_t new_batch_size = (*new_store_config)["batch_size"]->as_int();
        if (new_batch_size != wb->get_batch_size()) {
          log(INFO, "%s.write_buffer.batch_size changed from %zu to %zu",
              prefix.c_str(), wb->get_batch_size(), new_batch_size);
          wb->set_batch_size(new_batch_size);
        }
      }

      {
        size_t new_value = (*new_store_config)["max_update_metadatas_per_second"]->as_int();
        if (new_value != wb->get_max_update_metadatas_per_second()) {
          log(INFO, "%s.write_buffer.max_update_metadatas_per_second changed from %zu to %zu",
              prefix.c_str(), wb->get_max_update_metadatas_per_second(), new_value);
          wb->set_max_update_metadatas_per_second(new_value);
        }
      }

      {
        size_t new_value = (*new_store_config)["max_write_batches_per_second"]->as_int();
        if (new_value != wb->get_max_write_batches_per_second()) {
          log(INFO, "%s.write_buffer.max_write_batches_per_second changed from %zu to %zu",
              prefix.c_str(), wb->get_max_write_batches_per_second(), new_value);
          wb->set_max_write_batches_per_second(new_value);
        }
      }

      {
        ssize_t new_value = (*new_store_config)["disable_rate_limit_for_queue_length"]->as_int();
        if (new_value != wb->get_disable_rate_limit_for_queue_length()) {
          log(INFO, "%s.write_buffer.disable_rate_limit_for_queue_length changed from %zd to %zd",
              prefix.c_str(), wb->get_disable_rate_limit_for_queue_length(), new_value);
          wb->set_disable_rate_limit_for_queue_length(new_value);
        }
      }

      {
        bool new_value = (*new_store_config)["merge_find_patterns"]->as_int();
        if (new_value != wb->get_merge_find_patterns()) {
          log(INFO, "%s.write_buffer.merge_find_patterns changed from %s to %s",
              prefix.c_str(), wb->get_merge_find_patterns() ? "true" : "false",
              new_value ? "true" : "false");
          wb->set_merge_find_patterns(new_value);
        }
      }

      auto orig_substore_config = (*orig_store_config)["substore"];
      auto new_substore_config = (*new_store_config)["substore"];
      apply_store_config(orig_substore_config, new_substore_config,
          wb->get_substore(), prefix + ".write_buffer");

    } else if (new_type == "remote") {
      RemoteStore* r = reinterpret_cast<RemoteStore*>(base.get());

      size_t new_connection_limit = (*new_store_config)["connection_limit"]->as_int();
      if (new_connection_limit != r->get_connection_limit()) {
        log(INFO, "%s.remote.connection_limit changed from %zu to %zu",
            prefix.c_str(), r->get_connection_limit(),
            new_connection_limit);
        r->set_connection_limit(new_connection_limit);
      }

      string new_hostname = (*new_store_config)["hostname"]->as_string();
      int new_port = (*new_store_config)["port"]->as_int();
      if (new_hostname != r->get_hostname() || new_port != r->get_port()) {
        log(INFO, "%s.remote.netloc changed from %s:%d to %s:%d",
            prefix.c_str(), r->get_hostname().c_str(), r->get_port(),
            new_hostname.c_str(), new_port);
        r->set_netloc(new_hostname, new_port);
      }

    } else if (new_type == "read_only") {
      ReadOnlyStore* ro = reinterpret_cast<ReadOnlyStore*>(base.get());
      auto orig_substore_config = (*orig_store_config)["substore"];
      auto new_substore_config = (*new_store_config)["substore"];
      apply_store_config(orig_substore_config, new_substore_config,
          ro->get_substore(), prefix + ".read_only");

    } else if (new_type == "query") {
      QueryStore* q = reinterpret_cast<QueryStore*>(base.get());
      auto orig_substore_config = (*orig_store_config)["substore"];
      auto new_substore_config = (*new_store_config)["substore"];
      apply_store_config(orig_substore_config, new_substore_config,
          q->get_substore(), prefix + ".query");

    } else if ((new_type == "multi") || (new_type == "hash")) {
      MultiStore* m = reinterpret_cast<MultiStore*>(base.get());

      if (new_type == "hash") {
        ConsistentHashMultiStore* hm = reinterpret_cast<ConsistentHashMultiStore*>(m);

        int64_t new_precision = (*new_store_config)["precision"]->as_int();
        if (new_precision != hm->get_precision()) {
          log(INFO, "%s.hash.precision changed from %zu to %zu",
              prefix.c_str(), hm->get_precision(), new_precision);
          hm->set_precision(new_precision);
        }
      }

      const auto& new_substore_configs = (*new_store_config)["stores"]->as_dict();
      for (const auto& it : m->get_substores()) {
        if (!new_substore_configs.count(it.first)) {
          log(ERROR, "substores cannot be removed from multi stores without restarting");
          return;
        }
      }
      for (const auto& it : new_substore_configs) {
        if (!m->get_substores().count(it.first)) {
          log(ERROR, "substores cannot be added to multi stores without restarting");
          return;
        }
      }

      const auto& orig_substore_configs = (*new_store_config)["stores"]->as_dict();
      for (auto& it : m->get_substores()) {
        apply_store_config(orig_substore_configs.at(it.first),
            new_substore_configs.at(it.first), it.second,
            string_printf("%s.%s[%s]", prefix.c_str(), new_type.c_str(), it.first.c_str()));
      }

    } else if (new_type == "empty") {
      // this store type has no options

    } else {
      log(ERROR, "unknown store type \"%s\" in configuration", new_type.c_str());
    }

  } catch (const exception& e) {
    log(WARNING, "failed to apply store config changes: %s", e.what());
  }
}


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
  set_log_level(opt.log_level);

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  signal(SIGUSR1, signal_handler);

  set_profiler_threshold(false, opt.profiler_threshold_usecs);
  set_profiler_threshold(true, opt.internal_profiler_threshold_usecs);

  if (opt.prepopulate_depth) {
    log(INFO, "populating cache directories to depth %zu", opt.prepopulate_depth);

    deque<pair<string, size_t>> pending_patterns;
    pending_patterns.emplace_back(make_pair("*", 1));
    while (!pending_patterns.empty()) {

      auto item = pending_patterns.front();
      const string& pattern = item.first;
      size_t level = item.second;
      pending_patterns.pop_front();

      log(INFO, "[prepopulate] find %s (level %zu)", pattern.c_str(), level);
      auto profiler = create_internal_profiler("prepopulate_find");
      auto find_result_map = opt.store->find({pattern}, true, profiler.get());
      profiler.reset();

      if (level >= opt.prepopulate_depth) {
        continue;
      }

      auto& result = find_result_map.at(pattern);
      for (const string& item : result.results) {
        if (ends_with(item, ".*")) {
          pending_patterns.emplace_back(make_pair(item, level + 1));
        }
      }
    }
  }

  vector<shared_ptr<Server>> servers;
  if (!opt.http_listen_addrs.empty()) {
    shared_ptr<HTTPServer> s(new CycloneHTTPServer(opt.store, opt.http_threads,
        opt.filename));
    for (const auto& addr : opt.http_listen_addrs) {
      if (addr.second == 0) {
        s->listen(addr.first);
      } else {
        s->listen(addr.first, addr.second);
      }
    }
    servers.emplace_back(s);
  }

  if (!opt.line_stream_listen_addrs.empty() || !opt.pickle_listen_addrs.empty() ||
      !opt.shell_listen_addrs.empty()) {
    shared_ptr<StreamServer> s(new StreamServer(opt.store, opt.hash_stores,
        opt.stream_threads));
    for (const auto& addr : opt.line_stream_listen_addrs) {
      if (addr.second == 0) {
        s->listen(addr.first, StreamServer::Protocol::Line);
      } else {
        s->listen(addr.first, addr.second, StreamServer::Protocol::Line);
      }
    }
    for (const auto& addr : opt.pickle_listen_addrs) {
      if (addr.second == 0) {
        s->listen(addr.first, StreamServer::Protocol::Pickle);
      } else {
        s->listen(addr.first, addr.second, StreamServer::Protocol::Pickle);
      }
    }
    for (const auto& addr : opt.shell_listen_addrs) {
      if (addr.second == 0) {
        s->listen(addr.first, StreamServer::Protocol::Shell);
      } else {
        s->listen(addr.first, addr.second, StreamServer::Protocol::Shell);
      }
    }
    servers.emplace_back(s);
  }

  if (!opt.line_datagram_listen_addrs.empty()) {
    shared_ptr<DatagramServer> s(new DatagramServer(opt.store,
        opt.datagram_threads));
    for (const auto& addr : opt.line_datagram_listen_addrs) {
      if (addr.second == 0) {
        s->listen(addr.first);
      } else {
        s->listen(addr.first, addr.second);
      }
    }
    servers.emplace_back(s);
  }

  if (opt.thrift_port) {
    servers.emplace_back(new ThriftServer(opt.store, opt.hash_stores,
        opt.thrift_port, opt.thrift_threads));
  }

  for (auto& it : servers) {
    it->set_servers_list(servers);
    it->start();
  }

  // collect stats periodically and report them to the store
  uint64_t next_stats_report_time = opt.stats_report_usecs ?
      ((now() / opt.stats_report_usecs) * opt.stats_report_usecs) : 0;
  uint64_t next_config_check_time = now() + 10000000;
  while (!should_exit) {

    if (now() >= next_config_check_time) {
      uint64_t t = stat(config_filename).st_mtime;
      if (t > config_modification_time) {
        log(INFO, "configuration file was modified; reloading");

        Options new_opt(config_filename, true);
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

        if (new_opt.log_level != opt.log_level) {
          log(INFO, "log_level changed from %d to %d", opt.log_level,
              new_opt.log_level);
          opt.log_level = new_opt.log_level;
          set_log_level(opt.log_level);
        }

        if (new_opt.profiler_threshold_usecs != opt.profiler_threshold_usecs) {
          log(INFO, "profiler_threshold_usecs changed from %" PRId64 " to %" PRId64,
              opt.profiler_threshold_usecs, new_opt.profiler_threshold_usecs);
          opt.profiler_threshold_usecs = new_opt.profiler_threshold_usecs;
          set_profiler_threshold(false, opt.profiler_threshold_usecs);
        }

        if (new_opt.internal_profiler_threshold_usecs != opt.internal_profiler_threshold_usecs) {
          log(INFO, "internal_profiler_threshold_usecs changed from %" PRId64 " to %" PRId64,
              opt.internal_profiler_threshold_usecs, new_opt.internal_profiler_threshold_usecs);
          opt.internal_profiler_threshold_usecs = new_opt.internal_profiler_threshold_usecs;
          set_profiler_threshold(true, opt.internal_profiler_threshold_usecs);
        }

        apply_store_config(opt.store_config, new_opt.store_config, opt.store);
        opt.store_config = new_opt.store_config;

        config_modification_time = t;
      }

      next_config_check_time = now() + 10000000;
    }

    if (next_stats_report_time && (now() >= next_stats_report_time)) {
      string hostname = gethostname();
      auto stats = gather_stats(opt.store, servers);

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

      try {
        auto profiler = create_internal_profiler("stats_write");
        opt.store->write(data_to_write, false, false, profiler.get());
      } catch (const exception& e) {
        log(INFO, "failed to report stats: %s\n", e.what());
      }

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
    int64_t sleep_time = wakeup_time - now();
    if (sleep_time > 0) {
      usleep(wakeup_time - now());
    }
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
