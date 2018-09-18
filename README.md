# Cyclone

Cyclone is a timeseries data storage engine designed to be a drop-in replacement for the Carbon daemons and Graphite backend webservers. It has preliminary function support, but currently cannot be used effectively as a frontend webserver - it's recommended to run the Graphite webapp in front of Cyclone for now. Cyclone is a multithreaded event-driven server, allowing the use of all CPU cores while sharing a single instance of cached data (directory contents and file headers).

Like most of my projects, this is only tested at a small scale (so far), so there may be unfound bugs or inefficiencies. Use at your own risk.

## Building

- Build and install phosg (https://github.com/fuzziqersoftware/phosg).
- Install any version of libevent at or after 2.0 and any version of Thrift at or after 0.10.0.
- Run `make`.

If it doesn't work on your system, let me know. I've built and tested it on Mac OS X 10.13 and Ubuntu 16.04.

## Running

Cyclone is configured via a small JSON file. The comments in the example file (cyclone.conf.json) explain the details. Once that's all set up, just run `cyclone config_filename.json` (or use an appropriate daemonizer).

## Clients

Cyclone supports several protocols with varying capabilities:

| Protocol | Graphite-compatible? | Python client class                 |   Read commands   |         Write commands         |    Admin commands     |
| -------- |:--------------------:| ----------------------------------- |:-----------------:|:------------------------------:|:---------------------:|
| Line     |         Yes          | `stream_client.CycloneLineClient`   |                   |         write, create*         |                       |
| Datagram |         Yes          |                                     |                   |         write, create*         |                       |
| Pickle   |         Yes          | `stream_client.CyclonePickleClient` |                   |         write, create*         |                       |
| Shell    |         No           |                                     | read, find, stats | write, update_metadata, delete | verify, read-from-all |
| HTTP     |         Yes          | `http_client.CycloneHTTPClient`     | read, find, stats |                                |                       |
| Thrift   |         No           | `thrift_client.CycloneThriftClient` | read, find, stats | write, update_metadata, delete | verify, read-from-all |

Note: the ability to create series through the line, datagram, and pickle protocols is limited to autocreates (according to predefined rules in the server configuration). For more fine-grained control over series schema, create series by using update_metadata through the Thrift or shell interfaces instead.

### Patterns

Many functions that accept key names also accept patterns as well. A pattern is a key name containing wildcards or other special tokens. The special tokens in key patterns are:
- `*`: matches a string of any length containing any characters except `.`
- `[abc]`: matches any of the characters `a`, `b`, or `c`
- `{abc,def,ghi}`: matches any of the strings `abc`, `def`, or `ghi`

For example, the pattern `test.*.{dir,file,}name.key[13]` matches all of the following keys:
- `test.whatever.dirname.key1`
- `test.anything.dirname.key3`
- `test.this_field_doesnt_matter.filename.key1`
- `test.it_can_be_anything.filename.key3`
- `test.as_long_as_it_doesnt.name.key1`
- `test.have_a_dot.name.key3`

This example pattern does not match any of the following keys:
- `test.whatever.dirname.key2`
- `test.whatever.devicename.key1`
- `test.whatever.anything.filename.key1`

In autocreate rules (see the example configuration file for more information), a fourth special token can be used:
- `**` matches any string of any length, including `.` characters

`**` may also be used with the `delete_series` and `find` commands, but only as the last token of a pattern. See the descriptions of those commands for more information.

### Commands

Cyclone provides the following functions.

#### update_metadata

This command creates series or modifies the storage format of existing series. The caller provides a map of series name to metadata (which includes the archive sizes and resolutions, and propagation/aggregation options), as well as flags to define the behavior (whether existing series should be overwritten or resampled, and whether missing series should be created).

If the series does not exist and create_new is true, it is created. If the series exists and skip_existing_series is true, no operation is performed. If the series exists and truncate_existing_series is true, all datapoints are deleted from the series and it is recreated with the specified storage format.

If the series exists and skip_existing_series and truncate_existing_series are both false, then Cyclone resamples the existing data into the new storage format. This is implemented by reading all non-null datapoints from the existing archive, creating a new storage file, then writing these datapoints back to it. This entire process happens under a write lock and may be considered atomic, with one caveat: the new resolution may not be visible to clients until the changes are actually committed to disk, which may take some time if write buffering is used. If the new storage format has a lower resolution or a shorter retention window than the original storage format, then some datapoints may be lost. Furthermore, if multiple non-null datapoints map to the same interval in the new storage format, they are not aggregated - only one of them will be chosen as the datapoint for that interval.

If write buffering is used and skip_buffering is false (the default), this command returns before the changes are committed to disk.

#### delete_series

This command deletes one or more series, as well as all buffered writes in memory for those series.

Patterns may be given here; all series that match the pattern will be deleted. Entire directory trees can also be deleted by providing a pattern ending in `.**` (this is a special case and does not work in other places or other commands).

This command does not return until the changes are committed to disk, even if write buffering is used.

#### read

This command reads datapoints from one or more series. Patterns may be given for the series names; the read will be performed on all series matching the pattern.

There are currently some unfixed edge cases with write buffering, which are:
- If there are buffered writes for a series that doesn't exist and a read is performed with a pattern that would match this series, the series is not returned.
- If there are writes in progress for a series that is returned, the values written by the in-progress writes may or may not be returned.

The second case is expected to be fixed soon.

#### read_all

This command reads all datapoints and metadata from a single series. The series name must be a complete name; it cannot be a pattern.

This command does not respect buffered writes; if there are uncommitted changes to a series, they will not be returned. This command is mainly for internal use during the verification and repair procedure.

#### write

This command writes datapoints to one or more series. Patterns cannot be given for the series names.

If the series doesn't exist but its name matches an autocreate rule in the server's configuration, it will be created and the data will be written to the newly-created seties.

If write buffering is used and skip_buffering is false (the default), this command returns before the changes are committed to disk.

#### find

This command searches for series and directories matching the given patterns. In the returned list, items that end with `.*` are subdirectories; all others are individual series.

Entire directories can be enumerated recursively by specifying a pattern ending in `.**` (this is a special case and does not work in other places or other commands). For example, `find(test.dir1.**)` returns all series that exist in `test.dir1` and all of its subdirectories.

#### stats

This command returns current server statistics.

## Protocol descriptions

### Line protocol

Connect to the server using TCP on any of the line ports specified in the configuration. Send lines of text of the format `<key> <value> [timestamp]\n`. For example, sending the line `test.cyclone.key1 700 1527014460\n` writes a datapoint with value 700 at time 1527014460.

The timestamp is optional; if omitted; the server will use its current time for the written datapoint. For example, sending the line `test.cyclone.key1 700\n` writes a datapoint with value 700 at the current time.

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

The Thrift server provides functions for all of the commands listed in the Commands section. See cyclone_if.thrift for complete descriptions of the parameters and return values for these functions.

## Graphite interoperability

The stream, datagram, and pickle protocols are compatible with Carbon's analogous protocols. This allows a Cyclone server to be used in place of carbon-relay and carbon-cache daemons. The HTTP protocol is also compatible with the Graphite webapp; you can run the Graphite webapp directly in front of Cyclone by setting Graphite's `CLUSTER_SERVERS = ['localhost:5050']` (or any port in `http_listen` from Cyclone's configuration). Cyclone will handle the clustering logic if applicable; the Graphite webapp only needs to talk to one of the Cyclone instances in the cluster.

A Cyclone cluster can be set up by creating configuration files for each instance (see the comments in the configuration file for an example), then setting up the Graphite webapp on each instance to read from the local Cyclone instance. The entire cluster is then mostly homogenous; all machines run the same software with slightly different but analogous configurations. Writes can go to any Cyclone instance and will be forwarded appropriately, and reads can go to any Graphite webapp.

## Scaling

Adding a new node to an existing cluster can be done as follows:
- Rewrite the configuration files on all nodes to reflect the new cluster definition (e.g. by adding another substore to a hash store).
- Restart Cyclone on all nodes and start it on the new node.
- Run `verify start repair` in a Cyclone shell (`telnet localhost $SHELL_PORT`) on all nodes in the cluster.

The verification procedure runs in the background. It examines all keys and moves any that are on the wrong nodes to the correct nodes. During the verify procedure, all writes will go to the correct nodes (even if the existing data hasn't been moved yet) and all reads will go to all nodes to account for the incomplete migration. If the verification procedure finishes at different times on different nodes, some nodes might stop reading from all other nodes before all keys have been moved. To correct for this, use `read-from-all on` in a Cyclone shell to force the node to read from all nodes even if no verify procedure is running.

The verification procedure exports data from the series which are on incorrect nodes, and writes the data over any existing data that exists on the correct node. For example, if a series is autocreated and exists on both nodes (with old data on node1 and new data on node2), then after the migration, there will be a single data file on node2 containing all the data from both files. If datapoints with the same timestamp exist on each node, the datapoint from node1 will be used. If the schemas are different, the schema existing on node2 will be used.

To monitor the verify procedure, run `verify status` in a Cyclone shell to get the instantaneous status, or look at the `cyclone.<hostname>.verify_*` keys for the historical status.

## Future work

There's a lot to do here.

- Add a `flush_series` call to the Thrift interface to support blocking on buffered writes.
- Make write buffer merging work with in-progress writes.
- Support new storage formats.
- Build out query execution functionality.
- Support rendering graphs as images (perhaps even as SVGs).
- Fix the graceful shutdown procedure. Currently the servers shut down before the store is (synchronously) flushed, which doesn't work if there are pending writes to remote stores in a cluster configuration.
- Make an easy way to remove nodes from a cluster. Probably this could be easily done by adding weights to the hash ring; a store with a weight of zero could exist but have no assigned keys, so verify would move them all to other nodes.
