#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <phosg/UnitTest.hh>
#include <string>
#include <vector>

#include "ConsistentHashRing.hh"

using namespace std;


int main(int argc, char* argv[]) {

  {
    printf("-- create ring with one host\n");
    vector<string> in_hosts({"host1:8080"});
    ConsistentHashRing r(in_hosts);

    const auto& points = r.all_points();
    expect_eq(0x20000, points.size());
    for (const auto pt : points) {
      expect_eq(0, pt);
    }
  }

  {
    printf("-- create ring with three hosts; make sure all points are set\n");
    vector<string> in_hosts({"host1:8080", "host2:8080", "host3:8080"});
    ConsistentHashRing r(in_hosts);

    const auto& points = r.all_points();
    const auto& hosts = r.all_server_names();
    expect_eq(3, hosts.size());
    expect_eq(0x20000, points.size());
    for (const auto pt : points) {
      expect_le(pt, 2);
    }
  }

  {
    printf("-- create ring with two hosts; check point balance\n");
    vector<string> in_hosts({"host1:8080", "host2:8080"});
    ConsistentHashRing r(in_hosts);

    const auto& points = r.all_points();
    const auto& hosts = r.all_server_names();
    expect_eq(2, hosts.size());
    expect_eq(0x20000, points.size());

    int host_counts[2] = {0, 0};
    for (const auto pt : points) {
      expect_le(pt, 1);
      host_counts[pt]++;
    }

    expect_eq(0x20000, host_counts[0] + host_counts[1]);
    expect_lt(abs(host_counts[0] - host_counts[1]), 0x2000);
  }

  {
    printf("-- check host removal affecting other hosts\n");
    vector<string> in_hosts1({"host1:8080", "host2:8080", "host3:8080"});
    vector<string> in_hosts2({"host1:8080", "host2:8080"});
    ConsistentHashRing r1(in_hosts1);
    ConsistentHashRing r2(in_hosts2);

    expect_eq(in_hosts1.size(), r1.all_server_names().size());
    expect_eq(in_hosts2.size(), r2.all_server_names().size());

    const auto& pts1 = r1.all_points();
    const auto& pts2 = r2.all_points();
    expect_eq(pts1.size(), pts2.size());
    for (size_t x = 0; x < pts1.size(); x++) {
      if (pts1[x] == 2) {
        expect_le(pts2[x], 1);
      } else {
        expect_eq(pts2[x], pts1[x]);
      }
    }
  }

  {
    printf("-- check that host order doesn't matter\n");
    vector<string> in_hosts1({"host1:80", "host2:80", "host3:80", "host4:80"});
    vector<string> in_hosts2({"host4:80", "host3:80", "host2:80", "host1:80"});
    ConsistentHashRing r1(in_hosts1);
    ConsistentHashRing r2(in_hosts2);

    expect_eq(in_hosts1.size(), r1.all_server_names().size());
    expect_eq(in_hosts1.size(), r2.all_server_names().size());

    const auto& pts1 = r1.all_points();
    const auto& pts2 = r2.all_points();
    expect_eq(pts1.size(), pts2.size());
    for (size_t x = 0; x < pts1.size(); x++) {
      expect_eq(r1.server_name_for_id(pts1[x]), r2.server_name_for_id(pts2[x]));
    }
  }

  printf("all tests passed\n");
  return 0;
}
