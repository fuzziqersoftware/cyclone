#pragma once

class Server {
public:
  Server() = default;
  Server(const Server&) = delete;
  Server(Server&&) = delete;
  virtual ~Server() = default;

  virtual void start() = 0;
  virtual void schedule_stop() = 0;
  virtual void wait_for_stop() = 0;
};
