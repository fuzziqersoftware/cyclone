#include "Server.hh"

#include <phosg/Time.hh>

using namespace std;



Server::Server(const string& stats_prefix, size_t thread_count) :
    stats_prefix(stats_prefix), thread_count(thread_count),
    idle_thread_count(thread_count) { }

unordered_map<string, int64_t> Server::get_stats() {
  unordered_map<string, int64_t> ret;
  ret.emplace(this->stats_prefix + ".thread_count", this->thread_count.load());
  ret.emplace(this->stats_prefix + ".idle_thread_count",
      this->idle_thread_count.load());
  return ret;
}

void Server::set_servers_list(const vector<shared_ptr<Server>>& all_servers) {
  this->all_servers = all_servers;
}



BusyThreadGuard::BusyThreadGuard(std::atomic<size_t>* idle_thread_count) :
    idle_thread_count(idle_thread_count) {
  (*this->idle_thread_count)--;
}

BusyThreadGuard::~BusyThreadGuard() {
  (*this->idle_thread_count)++;
}



int64_t parse_relative_time(const string& s) {
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

unordered_map<string, int64_t> gather_stats(shared_ptr<Store> store,
    const vector<shared_ptr<Server>>& all_servers) {
  auto stats = store->get_stats(true);
  for (const auto& server : all_servers) {
    auto server_stats = server->get_stats();
    for (const auto& it : server_stats) {
      stats.emplace(it.first, it.second);
    }
  }
  return stats;
}
