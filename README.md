# Cyclone

Cyclone is a timeseries data storage engine designed to be a drop-in replacement for the Carbon daemons and Graphite backend webservers. It has preliminary function support, but currently cannot be used effectively as a frontend webserver - it's recommended to run the Graphite webapp in front of Cyclone for now. Cyclone is a multithreaded event-driven server, allowing the use of all CPU cores while sharing a single instance of cached data (directory contents and file headers).

Like most of my projects, this is only tested at a small scale (so far), so there may be unfound bugs or inefficiencies. Use at your own risk.

## Building

- Build and install phosg (https://github.com/fuzziqersoftware/phosg)
- Run `make`.

If it doesn't work on your system, let me know. I've built and tested it on Mac OS X 10.13 and Ubuntu 16.04.

## Running

Cyclone is configured via a small JSON file. The comments in the example file (cyclone.conf.json) explain the details. Once that's all set up, just run `cyclone config_filename.json` (or use an appropriate daemonizer).

## Clients

Cyclone supports several protocols with varying capabilities:

| Protocol | Graphite-compatible? | Python client class                 |   Read commands   |    Write commands     |
| -------- |:--------------------:| ----------------------------------- |:-----------------:|:---------------------:|
| Line     |         Yes          | `stream_client.CycloneLineClient`   |                   |    write, create*     |
| Datagram |         Yes          |                                     |                   |    write, create*     |
| Pickle   |         Yes          | `stream_client.CyclonePickleClient` |                   |    write, create*     |
| Shell    |         No           |                                     |    find, stats    |        delete         |
| HTTP     |         Yes          | `http_client.CycloneHTTPClient`     | read, find, stats |                       |
| Thrift   |         No           | `thrift_client.CycloneThriftClient` | read, find, stats | write, create, delete |

Note: the ability to create series through the line, datagram, and pickle protocols is limited to autocreates (according to predefined rules in the server configuration). For more fine-grained control over series schema, create series through the Thrift interface instead.

## Protocol descriptions

### Line protocol

Connect to the server using TCP on any of the line ports specified in the configuration. Send lines of text of the format `<key> <value> [timestamp]\n`. For example, sending the line `test.cyclone.key1 700 1527014460\n` writes a datapoint with value 700 at time 1527014460. The timestamp is optional; if omitted; the server will use its current time for the written datapoint.

### Datagram protocol

Send UDP datagrams to the server on any of the datagram ports specified in the configuration. The data format is the same as for the line protocol. Multiple datapoints can be sent in the same datagram, as lines separated by newline bytes, as long as they fit in the datagram.

### Pickle protocol

Connect to the server using TCP on any of the pickle ports specified in the configuration. Send frames of the format `<size><data>`, where `<size>` is a 32-bit big-endian integer specifying the number of bytes in the data field. The data field is a pickle-encoded Python object of the form `[(key, (timestamp, value)), ...]`. Lists and tuples may be used interchangeably; the server decodes them identically. All current pickle protocols (0-4) are supported.

### Shell protocol

Connect to the server using TCP on any of the shell ports specified in the configuration. This is intended to be an interactive protocol for manually inspecting and editing data and server state. Use `telnet <host> <port>` or `nc <host> <port>` to use the shell interface interactively. Run the `help` command within the shell to see a list of available commands.

### HTTP protocol

The HTTP server provides the following endpoints:
- `/`: Returns HTML containing links to the other endpoints.
- `/render`: Reads and renders data from one or more series. The query parameters (as GET params) are:
  - `from` and `until`: Specify a time range to query data over. If omitted, defaults to the past hour.
  - `format`: Specifies the format to return data in. Valid values are `json` and `pickle`.
  - `target`: Specifies the series or pattern to read data from. May be specified multiple times to read multiple series.
- `/metrics/find`: Searches for metrics matching the given query patterns.
  - `format`: Specifies the format to return data in. Valid values are `json`, `pickle`, and `html`.
  - `query`: Specifies the pattern to search for. May be given multiple times.
- `/y/stats`: Returns current server stats in plain text.
- `/y/config`: Returns the server configuration in commented JSON.

### Thrift protocol

Use TFramedTransport with this service. Most of these functions take a `local_only` argument, which is used internally by Cyclone and should be `false` when called by an external client.

The Thrift server provides the following functions:

- `update_metadata`: Creates series or modifies the storage format of existing series. If write buffering is used, this call returns before the changes are committed to disk.
- `delete_series`: Deletes one or more series, as well as all buffered writes in memory for those series. This call does not return until the changes are committed to disk.
- `read`: Reads datapoints from one or more series.
- `write`: Writes datapoints to one or more series. If write buffering is used, this call returns before the changes are committed to disk.
- `find`: Searches for series and directories matching the given patterns. In the returned list, items that end with `.*` are subdirectories; all others are individual series.
- `stats`: Returns current server stats.
- `delete_from_cache`: Deletes cached reads for the given series from the server's memory without modifying the underlying series on disk. Used in debugging.
- `delete_pending_writes`: Deletes all buffered writes in memory for the given series without modifying the underlying series on disk. Used in debugging.

See cyclone_if.thrift for complete descriptions of the parameters and return values for these functions.

## Graphite interoperability

The stream, datagram, and pickle protocols are compatible with Carbon's analogous protocols. This allows a Cyclone server to be used in place of carbon-relay and carbon-cache daemons. The HTTP protocol is also compatible with the Graphite webapp; you can run the Graphite webapp directly in front of Cyclone by setting Graphite's `CLUSTER_SERVERS = ['localhost:5050']` (or any port in `http_listen` from Cyclone's configuration). Cyclone will handle the clustering logic if applicable; the Graphite webapp only needs to talk to one of the Cyclone instances in the cluster.

A Cyclone cluster can be set up by creating configuration files for each instance (see the comments in the configuration file for an example), then setting up the Graphite webapp on each instance to read from the local Cyclone instance. The entire cluster is then homogenous; all machines run the same software with slightly different configurations. Writes can go to any Cyclone instance and will be forwarded appropriately, and reads can go to any Graphite webapp.

## Future work

There's a lot to do here.

- Add a `flush_series` call to the Thrift interface to support blocking on buffered writes.
- Support new storage formats.
- Build out query execution functionality.
- Support rendering graphs as images (perhaps even as SVGs).
