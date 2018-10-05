#include <string.h>
#include <unistd.h>

#include <iostream>

#include "Whisper.hh"

int main(int argc, char* argv[]) {

  if (argc < 3) {
    cout << "Usage: " << argv[0] << " file.wsp action [args ...]" << endl;
    return 1;
  }

  const char* filename = argv[1];
  const char* action = argv[2];

  if (!strcmp(action, "create")) {
    WhisperData database(filename, argv[3], atof(argv[4]), atoi(argv[5]));
    return 0;
  }

  WhisperData database(filename);

  if (!strcmp(action, "dump")) {
    database.PrintHeaders();

  } else if (!strcmp(action, "dumpdata")) {
    database.PrintData();

  } else if (!strcmp(action, "count")) {
    cout << database.CountValidPoints() << endl;

  } else if (!strcmp(action, "read")) {
    vector<pair<uint64_t, double>> data;
    database.Read(atoi(argv[3]), atoi(argv[4]), data);
    for (auto it : data)
      cout << it.first << " " << it.second << endl;

  } else if (!strcmp(action, "write")) {
    vector<pair<uint64_t, double>> data;
    for (int x = 3; x < argc; x += 2)
      data.push_back(make_pair(atoi(argv[x]), atof(argv[x + 1])));
    cout << "writing " << data.size() << " points" << endl;
    database.Write(data);

  } else if (!strcmp(action, "gen_random")) {

    srand(time(NULL) ^ getpid());

    vector<pair<uint64_t, double>> data;
    uint64_t end_time = time(NULL);
    uint64_t start_time = end_time + ParseTimeLength(argv[3]);
    uint64_t interval = ParseTimeLength(argv[4]);

    for (uint64_t now = start_time; now <= end_time; now += interval)
      data.push_back(make_pair(now, (float)(rand()) / RAND_MAX * 90 + 10));

    cout << "writing " << data.size() << " points" << endl;
    database.Write(data);
  }

  return 0;
}
