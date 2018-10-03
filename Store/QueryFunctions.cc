#include "QueryFunctions.hh"

#include <dirent.h>
#include <errno.h>
#include <stdarg.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <phosg/Strings.hh>
#include <stdexcept>
#include <string>
#include <vector>

#include "QueryParser.hh"

using namespace std;


static string function_call_str(const string& name, const vector<Query>& args) {
  string s = name + "(";
  bool after_first = false;
  for (const auto& arg : args) {
    if (after_first) {
      s += ',';
    } else {
      after_first = true;
    }
    s += arg.str();
  }
  s += ')';
  return s;
}

static Error check_arg_types(const vector<Query>& args, size_t min_arg_count,
    ssize_t max_arg_count, ...) {
  if (args.size() < min_arg_count) {
    return make_error("not enough arguments to function");
  }
  if ((max_arg_count > 0) && (static_cast<ssize_t>(args.size()) > max_arg_count)) {
    return make_error("too many arguments to function");
  }

  // variadic functions: apply the last type mask to all the excess arguments
  size_t num_types_expected = (max_arg_count < 0) ? min_arg_count + 1 : max_arg_count;

  va_list va;
  va_start(va, max_arg_count);
  uint64_t type_mask;
  for (size_t x = 0; x < args.size(); x++) {
    const auto& arg = args[x];
    if (x < num_types_expected) {
      type_mask = va_arg(va, uint64_t);
    }
    if (!(arg.type & type_mask)) {
      return make_error("incorrect argument type in function call");
    }
    if ((arg.type & Query::Type::SeriesOrCall) && !arg.computed) {
      if (!arg.computed) {
        return make_error("uncomputed function call given as argument");
      }
      for (const auto& it : arg.series_data) {
        if (!it.second.error.description.empty()) {
          return it.second.error;
        }
      }
    }
  }
  va_end(va);

  for (const auto& arg : args) {
    if (!arg.computed) {
      return make_error("argument value is not computed");
    }
    for (const auto& it : arg.series_data) {
      if (!it.second.error.description.empty()) {
        return it.second.error;
      }
    }
  }

  return make_success();
}

struct NormalizedSeries {
  int64_t start_time;
  int64_t end_time;
  int64_t step;
  unordered_multimap<string, vector<Datapoint>> series;

  NormalizedSeries() : start_time(0), end_time(0), step(0) { }
};

int64_t gcd(int64_t a, int64_t b) {
  // Euclidean algorithm
  while (b) {
    int64_t t = b;
    b = a % b;
    a = t;
  }
  return a;
}

int64_t lcm(int64_t a, int64_t b) {
  return (a * b) / gcd(a, b);
}

NormalizedSeries normalize_series_collections(vector<Query>& args) {
  NormalizedSeries ret;

  for (const auto& arg : args) {
    for (const auto& series_it : arg.series_data) {
      const auto& result = series_it.second;
      if (!result.step) {
        continue; // series is empty
      }
      if (!ret.start_time || (ret.start_time < result.start_time)) {
        ret.start_time = result.start_time;
      }
      if (!ret.end_time || (ret.end_time < result.end_time)) {
        ret.end_time = result.end_time;
      }
      if (!ret.step) {
        ret.step = result.step;
      } else if (ret.step != result.step) {
        ret.step = gcd(ret.step, result.step);
      }
    }
  }

  for (auto& arg : args) {
    if (!(arg.type & Query::Type::SeriesOrCall)) {
      continue;
    }
    for (auto& series_it : arg.series_data) {
      ret.series.emplace(move(series_it.first), move(series_it.second.data));
    }
    arg.series_data.clear();
  }

  return ret;
}


static unordered_map<string, ReadResult> make_error_map(const Error& e,
    const char* function_name, const vector<Query>& args) {
  unordered_map<string, ReadResult> ret_map;
  ReadResult& ret = ret_map[function_call_str(function_name, args)];
  ret.error = e;
  return ret_map;
}


static unordered_map<string, ReadResult> fn_sum_series(vector<Query>& args) {
  auto error = check_arg_types(args, 0, -1, Query::Type::SeriesOrCall);
  if (!error.description.empty()) {
    return make_error_map(error, "sumSeries", args);
  }

  auto normalized = normalize_series_collections(args);

  unordered_map<string, ReadResult> ret_map;
  ReadResult& ret = ret_map[function_call_str("sumSeries", args)];
  ret.start_time = normalized.start_time;
  ret.end_time = normalized.end_time;
  ret.step = normalized.step;

  map<int64_t, double> result_series_map;
  for (const auto& normalized_series_it : normalized.series) {
    for (const auto& datapoint : normalized_series_it.second) {
      result_series_map[datapoint.timestamp] += datapoint.value;
    }
  }

  auto& result_series = ret.data;
  for (const auto& result_dp : result_series_map) {
    result_series.emplace_back();
    Datapoint& dp = result_series.back();
    dp.timestamp = result_dp.first;
    dp.value = result_dp.second;
  }

  return ret_map;
}

static unordered_map<string, ReadResult> fn_average_series(vector<Query>& args) {
  auto error = check_arg_types(args, 0, -1, Query::Type::SeriesOrCall);
  if (!error.description.empty()) {
    return make_error_map(error, "averageSeries", args);
  }

  auto normalized = normalize_series_collections(args);

  unordered_map<string, ReadResult> ret_map;
  ReadResult& ret = ret_map[function_call_str("averageSeries", args)];
  ret.start_time = normalized.start_time;
  ret.end_time = normalized.end_time;
  ret.step = normalized.step;

  // {timestamp: (value, count)}
  map<int64_t, pair<double, int64_t>> result_series_map;
  for (const auto& normalized_series_it : normalized.series) {
    for (const auto& datapoint : normalized_series_it.second) {
      auto value_pair = result_series_map[datapoint.timestamp];
      value_pair.first += datapoint.value;
      value_pair.second += 1;
    }
  }

  auto& result_series = ret.data;
  for (const auto& result_dp : result_series_map) {
    result_series.emplace_back();
    Datapoint& dp = result_series.back();
    dp.timestamp = result_dp.first;
    dp.value = result_dp.second.first / result_dp.second.second;
  }

  return ret_map;
}

#define CYCLONE_FUNCTION(name) #name, +[](vector<Query>& args) -> unordered_map<string, ReadResult>

static const unordered_map<string, unordered_map<string, ReadResult> (*)(vector<Query>&)> functions({
  {"sumSeries", fn_sum_series},
  {"sum", fn_sum_series},
  {"avgSeries", fn_average_series},
  {"avg", fn_average_series},

  {CYCLONE_FUNCTION(scale) {
    auto error = check_arg_types(args, 2, 2, Query::Type::SeriesOrCall, Query::Type::Numeric);
    if (!error.description.empty()) {
      return make_error_map(error, "scale", args);
    }

    double scale_value = (args[1].type == Query::Type::Integer) ?
        args[1].int_data : args[1].float_data;
    unordered_map<string, ReadResult> ret;
    for (auto& arg : args) {
      for (auto& series_it : arg.series_data) {
        for (auto& dp : series_it.second.data) {
          dp.value *= scale_value;
        }
        ret.emplace(move(series_it.first), move(series_it.second));
      }
    }
    return ret;
  }},

  /* unimplemented functions:

  // alias group
  "alias": {
    "description": "Takes one metric or a wildcard seriesList and a string in quotes.\nPrints the string instead of the metric name in the legend.\n\n.. code-block:: none\n\n  &target=alias(Sales.widgets.largeBlue,\"Large Blue Widgets\")",
    "function": "alias(seriesList, newName)",
  },
  "aliasByMetric": {
    "description": "Takes a seriesList and applies an alias derived from the base metric name.\n\n.. code-block:: none\n\n  &target=aliasByMetric(carbon.agents.graphite.creates)",
    "function": "aliasByMetric(seriesList)",
  },
  "aliasByNode": {
    "description": "Takes a seriesList and applies an alias derived from one or more \"node\"\nportion/s of the target name or tags. Node indices are 0 indexed.\n\n.. code-block:: none\n\n  &target=aliasByNode(ganglia.*.cpu.load5,1)\n\nEach node may be an integer referencing a node in the series name or a string identifying a tag.\n\n.. code-block:: none\n\n  &target=seriesByTag(\"name=~cpu.load.*\", \"server=~server[1-9]+\", \"datacenter=dc1\")|aliasByNode(\"datacenter\", \"server\", 1)\n\n  # will produce output series like\n  # dc1.server1.load5, dc1.server2.load5, dc1.server1.load10, dc1.server2.load10",
    "function": "aliasByNode(seriesList, *nodes)",
  },
  "aliasByTags": {
    "description": "Takes a seriesList and applies an alias derived from one or more tags and/or nodes\n\n.. code-block:: none\n\n  &target=seriesByTag(\"name=cpu\")|aliasByTags(\"server\",\"name\")\n\nThis is an alias for :py:func:`aliasByNode <aliasByNode>`.",
    "function": "aliasByTags(seriesList, *tags)",
  },
  "aliasQuery": {
    "description": "Performs a query to alias the metrics in seriesList.\n\n.. code-block:: none\n\n  &target=aliasQuery(channel.power.*,\"channel\\.power\\.([0-9]+)\",\"channel.frequency.\\1\", \"Channel %d MHz\")\n\nThe series in seriesList will be aliased by first translating the series names using\nthe search & replace parameters, then using the last value of the resulting series\nto construct the alias using sprintf-style syntax.",
    "function": "aliasQuery(seriesList, search, replace, newName)",
  },
  "aliasSub": {
    "description": "Runs series names through a regex search/replace.\n\n.. code-block:: none\n\n  &target=aliasSub(ip.*TCP*,\"^.*TCP(\\d+)\",\"\\1\")",
    "function": "aliasSub(seriesList, search, replace)",
  },
  "legendValue": {
    "description": "Takes one metric or a wildcard seriesList and a string in quotes.\nAppends a value to the metric name in the legend.  Currently one or several of: `last`, `avg`,\n`total`, `min`, `max`.\nThe last argument can be `si` (default) or `binary`, in that case values will be formatted in the\ncorresponding system.\n\n.. code-block:: none\n\n  &target=legendValue(Sales.widgets.largeBlue, 'avg', 'max', 'si')",
    "function": "legendValue(seriesList, *valueTypes)",
    "value_options": ["average", "count", "diff", "last", "max", "median", "min", "multiply", "range", "stddev", "sum", "si", "binary"],
  }

  // calculate group
  "aggregateLine": {
    "description": "Takes a metric or wildcard seriesList and draws a horizontal line\nbased on the function applied to each series.\n\nNote: By default, the graphite renderer consolidates data points by\naveraging data points over time. If you are using the 'min' or 'max'\nfunction for aggregateLine, this can cause an unusual gap in the\nline drawn by this function and the data itself. To fix this, you\nshould use the consolidateBy() function with the same function\nargument you are using for aggregateLine. This will ensure that the\nproper data points are retained and the graph should line up\ncorrectly.\n\nExample:\n\n.. code-block:: none\n\n  &target=aggregateLine(server01.connections.total, 'avg')\n  &target=aggregateLine(server*.connections.total, 'avg')",
    "function": "aggregateLine(seriesList, func='average')",
    "function_options": ["average", "count", "diff", "last", "max", "median", "min", "multiply", "range", "stddev", "sum"],
  },
  "exponentialMovingAverage": {
    "description": "Takes a series of values and a window size and produces an exponential moving\naverage utilizing the following formula:\n\n.. code-block:: none\n\n  ema(current) = constant * (Current Value) + (1 - constant) * ema(previous)\n\nThe Constant is calculated as:\n\n.. code-block:: none\n\n  constant = 2 / (windowSize + 1)\n\nThe first period EMA uses a simple moving average for its value.\n\nExample:\n\n.. code-block:: none\n\n  &target=exponentialMovingAverage(*.transactions.count, 10)\n  &target=exponentialMovingAverage(*.transactions.count, '-10s')",
    "function": "exponentialMovingAverage(seriesList, windowSize)",
  },
  "holtWintersAberration": {
    "description": "Performs a Holt-Winters forecast using the series as input data and plots the\npositive or negative deviation of the series data from the forecast.",
    "function": "holtWintersAberration(seriesList, delta=3, bootstrapInterval='7d', seasonality='1d')",
  },
  "holtWintersConfidenceArea": {
    "description": "Performs a Holt-Winters forecast using the series as input data and plots the\narea between the upper and lower bands of the predicted forecast deviations.",
    "function": "holtWintersConfidenceArea(seriesList, delta=3, bootstrapInterval='7d', seasonality='1d')",
  },
  "holtWintersConfidenceBands": {
    "description": "Performs a Holt-Winters forecast using the series as input data and plots\nupper and lower bands with the predicted forecast deviations.",
    "function": "holtWintersConfidenceBands(seriesList, delta=3, bootstrapInterval='7d', seasonality='1d')",
  },
  "holtWintersForecast": {
    "description": "Performs a Holt-Winters forecast using the series as input data. Data from\n`bootstrapInterval` (one week by default) previous to the series is used to bootstrap the initial forecast.",
    "function": "holtWintersForecast(seriesList, bootstrapInterval='7d', seasonality='1d')",
  },
  "identity": {
    "description": "Identity function:\nReturns datapoints where the value equals the timestamp of the datapoint.\nUseful when you have another series where the value is a timestamp, and\nyou want to compare it to the time of the datapoint, to render an age\n\nExample:\n\n.. code-block:: none\n\n  &target=identity(\"The.time.series\")\n\nThis would create a series named \"The.time.series\" that contains points where\nx(t) == t.",
    "function": "identity(name)",
  },
  "linearRegression": {
    "description": "Graphs the linear regression function by least squares method.\n\nTakes one metric or a wildcard seriesList, followed by a quoted string with the\ntime to start the line and another quoted string with the time to end the line.\nThe start and end times are inclusive (default range is from to until). See\n``from / until`` in the render\\_api_ for examples of time formats. Datapoints\nin the range is used to regression.\n\nExample:\n\n.. code-block:: none\n\n  &target=linearRegression(Server.instance01.threads.busy, '-1d')\n  &target=linearRegression(Server.instance*.threads.busy, \"00:00 20140101\",\"11:59 20140630\")",
    "function": "linearRegression(seriesList, startSourceAt=None, endSourceAt=None)",
  },
  "movingAverage": {
    "description": "Graphs the moving average of a metric (or metrics) over a fixed number of\npast points, or a time interval.\n\nTakes one metric or a wildcard seriesList followed by a number N of datapoints\nor a quoted string with a length of time like '1hour' or '5min' (See ``from /\nuntil`` in the render\\_api_ for examples of time formats), and an xFilesFactor value to specify\nhow many points in the window must be non-null for the output to be considered valid. Graphs the\naverage of the preceeding datapoints for each point on the graph.\n\nExample:\n\n.. code-block:: none\n\n  &target=movingAverage(Server.instance01.threads.busy,10)\n  &target=movingAverage(Server.instance*.threads.idle,'5min')",
    "function": "movingAverage(seriesList, windowSize, xFilesFactor=None)",
  },
  "movingMax": {
    "description": "Graphs the moving maximum of a metric (or metrics) over a fixed number of\npast points, or a time interval.\n\nTakes one metric or a wildcard seriesList followed by a number N of datapoints\nor a quoted string with a length of time like '1hour' or '5min' (See ``from /\nuntil`` in the render\\_api_ for examples of time formats), and an xFilesFactor value to specify\nhow many points in the window must be non-null for the output to be considered valid. Graphs the\nmaximum of the preceeding datapoints for each point on the graph.\n\nExample:\n\n.. code-block:: none\n\n  &target=movingMax(Server.instance01.requests,10)\n  &target=movingMax(Server.instance*.errors,'5min')",
    "function": "movingMax(seriesList, windowSize, xFilesFactor=None)",
  },
  "movingMedian": {
    "description": "Graphs the moving median of a metric (or metrics) over a fixed number of\npast points, or a time interval.\n\nTakes one metric or a wildcard seriesList followed by a number N of datapoints\nor a quoted string with a length of time like '1hour' or '5min' (See ``from /\nuntil`` in the render\\_api_ for examples of time formats), and an xFilesFactor value to specify\nhow many points in the window must be non-null for the output to be considered valid. Graphs the\nmedian of the preceeding datapoints for each point on the graph.\n\nExample:\n\n.. code-block:: none\n\n  &target=movingMedian(Server.instance01.threads.busy,10)\n  &target=movingMedian(Server.instance*.threads.idle,'5min')",
    "function": "movingMedian(seriesList, windowSize, xFilesFactor=None)",
  },
  "movingMin": {
    "description": "Graphs the moving minimum of a metric (or metrics) over a fixed number of\npast points, or a time interval.\n\nTakes one metric or a wildcard seriesList followed by a number N of datapoints\nor a quoted string with a length of time like '1hour' or '5min' (See ``from /\nuntil`` in the render\\_api_ for examples of time formats), and an xFilesFactor value to specify\nhow many points in the window must be non-null for the output to be considered valid. Graphs the\nminimum of the preceeding datapoints for each point on the graph.\n\nExample:\n\n.. code-block:: none\n\n  &target=movingMin(Server.instance01.requests,10)\n  &target=movingMin(Server.instance*.errors,'5min')",
    "function": "movingMin(seriesList, windowSize, xFilesFactor=None)",
  },
  "movingSum": {
    "description": "Graphs the moving sum of a metric (or metrics) over a fixed number of\npast points, or a time interval.\n\nTakes one metric or a wildcard seriesList followed by a number N of datapoints\nor a quoted string with a length of time like '1hour' or '5min' (See ``from /\nuntil`` in the render\\_api_ for examples of time formats), and an xFilesFactor value to specify\nhow many points in the window must be non-null for the output to be considered valid. Graphs the\nsum of the preceeding datapoints for each point on the graph.\n\nExample:\n\n.. code-block:: none\n\n  &target=movingSum(Server.instance01.requests,10)\n  &target=movingSum(Server.instance*.errors,'5min')",
    "function": "movingSum(seriesList, windowSize, xFilesFactor=None)",
  },
  "movingWindow": {
    "description": "Graphs a moving window function of a metric (or metrics) over a fixed number of\npast points, or a time interval.\n\nTakes one metric or a wildcard seriesList, a number N of datapoints\nor a quoted string with a length of time like '1hour' or '5min' (See ``from /\nuntil`` in the render\\_api_ for examples of time formats), a function to apply to the points\nin the window to produce the output, and an xFilesFactor value to specify how many points in the\nwindow must be non-null for the output to be considered valid. Graphs the\noutput of the function for the preceeding datapoints for each point on the graph.\n\nExample:\n\n.. code-block:: none\n\n  &target=movingWindow(Server.instance01.threads.busy,10)\n  &target=movingWindow(Server.instance*.threads.idle,'5min','median',0.5)\n\n.. note::\n\n  `xFilesFactor` follows the same semantics as in Whisper storage schemas.  Setting it to 0 (the\n  default) means that only a single value in a given interval needs to be non-null, setting it to\n  1 means that all values in the interval must be non-null.  A setting of 0.5 means that at least\n  half the values in the interval must be non-null.",
    "function": "movingWindow(seriesList, windowSize, func='average', xFilesFactor=None)",
    "function_options": ["average", "count", "diff", "last", "max", "median", "min", "multiply", "range", "stddev", "sum"],
  },
  "nPercentile": {
    "description": "Returns n-percent of each series in the seriesList.",
    "function": "nPercentile(seriesList, n)",
  },
  "stdev": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\nDraw the Standard Deviation of all metrics passed for the past N datapoints.\nIf the ratio of null points in the window is greater than windowTolerance,\nskip the calculation. The default for windowTolerance is 0.1 (up to 10% of points\nin the window can be missing). Note that if this is set to 0.0, it will cause large\ngaps in the output anywhere a single point is missing.\n\nExample:\n\n.. code-block:: none\n\n  &target=stdev(server*.instance*.threads.busy,30)\n  &target=stdev(server*.instance*.cpu.system,30,0.0)",
    "function": "stdev(seriesList, points, windowTolerance=0.1)",
  }

  // combine group
  "aggregate": {
    "description": "Aggregate series using the specified function.\n\nExample:\n\n.. code-block:: none\n\n  &target=aggregate(host.cpu-[0-7].cpu-{user,system}.value, \"sum\")\n\nThis would be the equivalent of\n\n.. code-block:: none\n\n  &target=sumSeries(host.cpu-[0-7].cpu-{user,system}.value)\n\nThis function can be used with aggregation functions ``average``, ``median``, ``sum``, ``min``,\n``max``, ``diff``, ``stddev``, ``count``, ``range``, ``multiply`` & ``last``.",
    "function": "aggregate(seriesList, func, xFilesFactor=None)",
    "function_options": ["average", "count", "diff", "last", "max", "median", "min", "multiply", "range", "stddev", "sum"],
  },
  "aggregateWithWildcards": {
    "description": "Call aggregator after inserting wildcards at the given position(s).\n\nExample:\n\n.. code-block:: none\n\n  &target=aggregateWithWildcards(host.cpu-[0-7].cpu-{user,system}.value, \"sum\", 1)\n\nThis would be the equivalent of\n\n.. code-block:: none\n\n  &target=sumSeries(host.cpu-[0-7].cpu-user.value)&target=sumSeries(host.cpu-[0-7].cpu-system.value)\n  # or\n  &target=aggregate(host.cpu-[0-7].cpu-user.value,\"sum\")&target=aggregate(host.cpu-[0-7].cpu-system.value,\"sum\")\n\nThis function can be used with all aggregation functions supported by\n:py:func:`aggregate <aggregate>`: ``average``, ``median``, ``sum``, ``min``, ``max``, ``diff``,\n``stddev``, ``range`` & ``multiply``.\n\nThis complements :py:func:`groupByNodes <groupByNodes>` which takes a list of nodes that must match in each group.",
    "function": "aggregateWithWildcards(seriesList, func, *positions)",
    "function_options": ["average", "count", "diff", "last", "max", "median", "min", "multiply", "range", "stddev", "sum"],
  },
  "applyByNode": {
    "description": "Takes a seriesList and applies some complicated function (described by a string), replacing templates with unique\nprefixes of keys from the seriesList (the key is all nodes up to the index given as `nodeNum`).\n\nIf the `newName` parameter is provided, the name of the resulting series will be given by that parameter, with any\n\"%\" characters replaced by the unique prefix.\n\nExample:\n\n.. code-block:: none\n\n  &target=applyByNode(servers.*.disk.bytes_free,1,\"divideSeries(%.disk.bytes_free,sumSeries(%.disk.bytes_*))\")\n\nWould find all series which match `servers.*.disk.bytes_free`, then trim them down to unique series up to the node\ngiven by nodeNum, then fill them into the template function provided (replacing % by the prefixes).\n\nAdditional Examples:\n\nGiven keys of\n\n- `stats.counts.haproxy.web.2XX`\n- `stats.counts.haproxy.web.3XX`\n- `stats.counts.haproxy.web.5XX`\n- `stats.counts.haproxy.microservice.2XX`\n- `stats.counts.haproxy.microservice.3XX`\n- `stats.counts.haproxy.microservice.5XX`\n\nThe following will return the rate of 5XX's per service:\n\n.. code-block:: none\n\n  applyByNode(stats.counts.haproxy.*.*XX, 3, \"asPercent(%.5XX, sumSeries(%.*XX))\", \"%.pct_5XX\")\n\nThe output series would have keys `stats.counts.haproxy.web.pct_5XX` and `stats.counts.haproxy.microservice.pct_5XX`.",
    "function": "applyByNode(seriesList, nodeNum, templateFunction, newName=None)",
  },
  "asPercent": {
    "description": "Calculates a percentage of the total of a wildcard series. If `total` is specified,\neach series will be calculated as a percentage of that total. If `total` is not specified,\nthe sum of all points in the wildcard series will be used instead.\n\nA list of nodes can optionally be provided, if so they will be used to match series with their\ncorresponding totals following the same logic as :py:func:`groupByNodes <groupByNodes>`.\n\nWhen passing `nodes` the `total` parameter may be a series list or `None`.  If it is `None` then\nfor each series in `seriesList` the percentage of the sum of series in that group will be returned.\n\nWhen not passing `nodes`, the `total` parameter may be a single series, reference the same number\nof series as `seriesList` or be a numeric value.\n\nExample:\n\n.. code-block:: none\n\n  # Server01 connections failed and succeeded as a percentage of Server01 connections attempted\n  &target=asPercent(Server01.connections.{failed,succeeded}, Server01.connections.attempted)\n\n  # For each server, its connections failed as a percentage of its connections attempted\n  &target=asPercent(Server*.connections.failed, Server*.connections.attempted)\n\n  # For each server, its connections failed and succeeded as a percentage of its connections attemped\n  &target=asPercent(Server*.connections.{failed,succeeded}, Server*.connections.attempted, 0)\n\n  # apache01.threads.busy as a percentage of 1500\n  &target=asPercent(apache01.threads.busy,1500)\n\n  # Server01 cpu stats as a percentage of its total\n  &target=asPercent(Server01.cpu.*.jiffies)\n\n  # cpu stats for each server as a percentage of its total\n  &target=asPercent(Server*.cpu.*.jiffies, None, 0)\n\nWhen using `nodes`, any series or totals that can't be matched will create output series with\nnames like ``asPercent(someSeries,MISSING)`` or ``asPercent(MISSING,someTotalSeries)`` and all\nvalues set to None. If desired these series can be filtered out by piping the result through\n``|exclude(\"MISSING\")`` as shown below:\n\n.. code-block:: none\n\n  &target=asPercent(Server{1,2}.memory.used,Server{1,3}.memory.total,0)\n\n  # will produce 3 output series:\n  # asPercent(Server1.memory.used,Server1.memory.total) [values will be as expected]\n  # asPercent(Server2.memory.used,MISSING) [all values will be None]\n  # asPercent(MISSING,Server3.memory.total) [all values will be None]\n\n  &target=asPercent(Server{1,2}.memory.used,Server{1,3}.memory.total,0)|exclude(\"MISSING\")\n\n  # will produce 1 output series:\n  # asPercent(Server1.memory.used,Server1.memory.total) [values will be as expected]\n\nEach node may be an integer referencing a node in the series name or a string identifying a tag.\n\n.. note::\n\n  When `total` is a seriesList, specifying `nodes` to match series with the corresponding total\n  series will increase reliability.",
    "function": "asPercent(seriesList, total=None, *nodes)",
  },
  "averageSeriesWithWildcards": {
    "description": "Call averageSeries after inserting wildcards at the given position(s).\n\nExample:\n\n.. code-block:: none\n\n  &target=averageSeriesWithWildcards(host.cpu-[0-7].cpu-{user,system}.value, 1)\n\nThis would be the equivalent of\n\n.. code-block:: none\n\n  &target=averageSeries(host.*.cpu-user.value)&target=averageSeries(host.*.cpu-system.value)\n\nThis is an alias for :py:func:`aggregateWithWildcards <aggregateWithWildcards>` with aggregation ``average``.",
    "function": "averageSeriesWithWildcards(seriesList, *position)",
  },
  "countSeries": {
    "description": "Draws a horizontal line representing the number of nodes found in the seriesList.\n\n.. code-block:: none\n\n  &target=countSeries(carbon.agents.*.*)",
    "function": "countSeries(*seriesLists)",
  },
  "diffSeries": {
    "description": "Subtracts series 2 through n from series 1.\n\nExample:\n\n.. code-block:: none\n\n  &target=diffSeries(service.connections.total,service.connections.failed)\n\nTo diff a series and a constant, one should use offset instead of (or in\naddition to) diffSeries\n\nExample:\n\n.. code-block:: none\n\n  &target=offset(service.connections.total,-5)\n\n  &target=offset(diffSeries(service.connections.total,service.connections.failed),-4)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``diff``.",
    "function": "diffSeries(*seriesLists)",
  },
  "divideSeries": {
    "description": "Takes a dividend metric and a divisor metric and draws the division result.\nA constant may *not* be passed. To divide by a constant, use the scale()\nfunction (which is essentially a multiplication operation) and use the inverse\nof the dividend. (Division by 8 = multiplication by 1/8 or 0.125)\n\nExample:\n\n.. code-block:: none\n\n  &target=divideSeries(Series.dividends,Series.divisors)",
    "function": "divideSeries(dividendSeriesList, divisorSeries)",
  },
  "divideSeriesLists": {
    "description": "Iterates over a two lists and divides list1[0] by list2[0], list1[1] by list2[1] and so on.\nThe lists need to be the same length",
    "function": "divideSeriesLists(dividendSeriesList, divisorSeriesList)",
  },
  "group": {
    "description": "Takes an arbitrary number of seriesLists and adds them to a single seriesList. This is used\nto pass multiple seriesLists to a function which only takes one",
    "function": "group(*seriesLists)",
  },
  "groupByNode": {
    "description": "Takes a serieslist and maps a callback to subgroups within as defined by a common node\n\n.. code-block:: none\n\n  &target=groupByNode(ganglia.by-function.*.*.cpu.load5,2,\"sumSeries\")\n\nWould return multiple series which are each the result of applying the \"sumSeries\" function\nto groups joined on the second node (0 indexed) resulting in a list of targets like\n\n.. code-block:: none\n\n  sumSeries(ganglia.by-function.server1.*.cpu.load5),sumSeries(ganglia.by-function.server2.*.cpu.load5),...\n\nNode may be an integer referencing a node in the series name or a string identifying a tag.\n\nThis is an alias for using :py:func:`groupByNodes <groupByNodes>` with a single node.",
    "function": "groupByNode(seriesList, nodeNum, callback='average')",
  },
  "groupByNodes": {
    "description": "Takes a serieslist and maps a callback to subgroups within as defined by multiple nodes\n\n.. code-block:: none\n\n  &target=groupByNodes(ganglia.server*.*.cpu.load*,\"sum\",1,4)\n\nWould return multiple series which are each the result of applying the \"sum\" aggregation\nto groups joined on the nodes' list (0 indexed) resulting in a list of targets like\n\n.. code-block:: none\n\n  sumSeries(ganglia.server1.*.cpu.load5),sumSeries(ganglia.server1.*.cpu.load10),sumSeries(ganglia.server1.*.cpu.load15),sumSeries(ganglia.server2.*.cpu.load5),sumSeries(ganglia.server2.*.cpu.load10),sumSeries(ganglia.server2.*.cpu.load15),...\n\nThis function can be used with all aggregation functions supported by\n:py:func:`aggregate <aggregate>`: ``average``, ``median``, ``sum``, ``min``, ``max``, ``diff``,\n``stddev``, ``range`` & ``multiply``.\n\nEach node may be an integer referencing a node in the series name or a string identifying a tag.\n\n.. code-block:: none\n\n  &target=seriesByTag(\"name=~cpu.load.*\", \"server=~server[1-9]+\", \"datacenter=~dc[1-9]+\")|groupByNodes(\"average\", \"datacenter\", 1)\n\n  # will produce output series like\n  # dc1.load5, dc2.load5, dc1.load10, dc2.load10\n\nThis complements :py:func:`aggregateWithWildcards <aggregateWithWildcards>` which takes a list of wildcard nodes.",
    "function": "groupByNodes(seriesList, callback, *nodes)",
  },
  "groupByTags": {
    "description": "Takes a serieslist and maps a callback to subgroups within as defined by multiple tags\n\n.. code-block:: none\n\n  &target=seriesByTag(\"name=cpu\")|groupByTags(\"average\",\"dc\")\n\nWould return multiple series which are each the result of applying the \"averageSeries\" function\nto groups joined on the specified tags resulting in a list of targets like\n\n.. code-block :: none\n\n  averageSeries(seriesByTag(\"name=cpu\",\"dc=dc1\")),averageSeries(seriesByTag(\"name=cpu\",\"dc=dc2\")),...\n\nThis function can be used with all aggregation functions supported by\n:py:func:`aggregate <aggregate>`: ``average``, ``median``, ``sum``, ``min``, ``max``, ``diff``,\n``stddev``, ``range`` & ``multiply``.",
    "function": "groupByTags(seriesList, callback, *tags)",
    "function_options": ["average", "count", "diff", "last", "max", "median", "min", "multiply", "range", "stddev", "sum"],
  },
  "isNonNull": {
    "description": "Takes a metric or wildcard seriesList and counts up the number of non-null\nvalues.  This is useful for understanding the number of metrics that have data\nat a given point in time (i.e. to count which servers are alive).\n\nExample:\n\n.. code-block:: none\n\n  &target=isNonNull(webapp.pages.*.views)\n\nReturns a seriesList where 1 is specified for non-null values, and\n0 is specified for null values.",
    "function": "isNonNull(seriesList)",
  },
  "map": {
    "description": "Short form: ``map()``\n\nTakes a seriesList and maps it to a list of seriesList. Each seriesList has the\ngiven mapNodes in common.\n\n.. note:: This function is not very useful alone. It should be used with :py:func:`reduceSeries`\n\n.. code-block:: none\n\n  mapSeries(servers.*.cpu.*,1) =>\n\n    [\n      servers.server1.cpu.*,\n      servers.server2.cpu.*,\n      ...\n      servers.serverN.cpu.*\n    ]\n\nEach node may be an integer referencing a node in the series name or a string identifying a tag.",
    "function": "map(seriesList, *mapNodes)",
  },
  "mapSeries": {
    "description": "Short form: ``map()``\n\nTakes a seriesList and maps it to a list of seriesList. Each seriesList has the\ngiven mapNodes in common.\n\n.. note:: This function is not very useful alone. It should be used with :py:func:`reduceSeries`\n\n.. code-block:: none\n\n  mapSeries(servers.*.cpu.*,1) =>\n\n    [\n      servers.server1.cpu.*,\n      servers.server2.cpu.*,\n      ...\n      servers.serverN.cpu.*\n    ]\n\nEach node may be an integer referencing a node in the series name or a string identifying a tag.",
    "function": "mapSeries(seriesList, *mapNodes)",
  },
  "maxSeries": {
    "description": "Takes one metric or a wildcard seriesList.\nFor each datapoint from each metric passed in, pick the maximum value and graph it.\n\nExample:\n\n.. code-block:: none\n\n  &target=maxSeries(Server*.connections.total)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``max``.",
    "function": "maxSeries(*seriesLists)",
  },
  "minSeries": {
    "description": "Takes one metric or a wildcard seriesList.\nFor each datapoint from each metric passed in, pick the minimum value and graph it.\n\nExample:\n\n.. code-block:: none\n\n  &target=minSeries(Server*.connections.total)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``min``.",
    "function": "minSeries(*seriesLists)",
  },
  "multiplySeries": {
    "description": "Takes two or more series and multiplies their points. A constant may not be\nused. To multiply by a constant, use the scale() function.\n\nExample:\n\n.. code-block:: none\n\n  &target=multiplySeries(Series.dividends,Series.divisors)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``multiply``.",
    "function": "multiplySeries(*seriesLists)",
  },
  "multiplySeriesWithWildcards": {
    "description": "Call multiplySeries after inserting wildcards at the given position(s).\n\nExample:\n\n.. code-block:: none\n\n  &target=multiplySeriesWithWildcards(web.host-[0-7].{avg-response,total-request}.value, 2)\n\nThis would be the equivalent of\n\n.. code-block:: none\n\n  &target=multiplySeries(web.host-0.{avg-response,total-request}.value)&target=multiplySeries(web.host-1.{avg-response,total-request}.value)...\n\nThis is an alias for :py:func:`aggregateWithWildcards <aggregateWithWildcards>` with aggregation ``multiply``.",
    "function": "multiplySeriesWithWildcards(seriesList, *position)",
  },
  "pct": {
    "description": "Calculates a percentage of the total of a wildcard series. If `total` is specified,\neach series will be calculated as a percentage of that total. If `total` is not specified,\nthe sum of all points in the wildcard series will be used instead.\n\nA list of nodes can optionally be provided, if so they will be used to match series with their\ncorresponding totals following the same logic as :py:func:`groupByNodes <groupByNodes>`.\n\nWhen passing `nodes` the `total` parameter may be a series list or `None`.  If it is `None` then\nfor each series in `seriesList` the percentage of the sum of series in that group will be returned.\n\nWhen not passing `nodes`, the `total` parameter may be a single series, reference the same number\nof series as `seriesList` or be a numeric value.\n\nExample:\n\n.. code-block:: none\n\n  # Server01 connections failed and succeeded as a percentage of Server01 connections attempted\n  &target=asPercent(Server01.connections.{failed,succeeded}, Server01.connections.attempted)\n\n  # For each server, its connections failed as a percentage of its connections attempted\n  &target=asPercent(Server*.connections.failed, Server*.connections.attempted)\n\n  # For each server, its connections failed and succeeded as a percentage of its connections attemped\n  &target=asPercent(Server*.connections.{failed,succeeded}, Server*.connections.attempted, 0)\n\n  # apache01.threads.busy as a percentage of 1500\n  &target=asPercent(apache01.threads.busy,1500)\n\n  # Server01 cpu stats as a percentage of its total\n  &target=asPercent(Server01.cpu.*.jiffies)\n\n  # cpu stats for each server as a percentage of its total\n  &target=asPercent(Server*.cpu.*.jiffies, None, 0)\n\nWhen using `nodes`, any series or totals that can't be matched will create output series with\nnames like ``asPercent(someSeries,MISSING)`` or ``asPercent(MISSING,someTotalSeries)`` and all\nvalues set to None. If desired these series can be filtered out by piping the result through\n``|exclude(\"MISSING\")`` as shown below:\n\n.. code-block:: none\n\n  &target=asPercent(Server{1,2}.memory.used,Server{1,3}.memory.total,0)\n\n  # will produce 3 output series:\n  # asPercent(Server1.memory.used,Server1.memory.total) [values will be as expected]\n  # asPercent(Server2.memory.used,MISSING) [all values will be None]\n  # asPercent(MISSING,Server3.memory.total) [all values will be None]\n\n  &target=asPercent(Server{1,2}.memory.used,Server{1,3}.memory.total,0)|exclude(\"MISSING\")\n\n  # will produce 1 output series:\n  # asPercent(Server1.memory.used,Server1.memory.total) [values will be as expected]\n\nEach node may be an integer referencing a node in the series name or a string identifying a tag.\n\n.. note::\n\n  When `total` is a seriesList, specifying `nodes` to match series with the corresponding total\n  series will increase reliability.",
    "function": "pct(seriesList, total=None, *nodes)",
  },
  "percentileOfSeries": {
    "description": "percentileOfSeries returns a single series which is composed of the n-percentile\nvalues taken across a wildcard series at each point. Unless `interpolate` is\nset to True, percentile values are actual values contained in one of the\nsupplied series.",
    "function": "percentileOfSeries(seriesList, n, interpolate=False)",
  },
  "rangeOfSeries": {
    "description": "Takes a wildcard seriesList.\nDistills down a set of inputs into the range of the series\n\nExample:\n\n.. code-block:: none\n\n    &target=rangeOfSeries(Server*.connections.total)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``rangeOf``.",
    "function": "rangeOfSeries(*seriesLists)",
  },
  "reduce": {
    "description": "Short form: ``reduce()``\n\nTakes a list of seriesLists and reduces it to a list of series by means of the reduceFunction.\n\nReduction is performed by matching the reduceNode in each series against the list of\nreduceMatchers. Then each series is passed to the reduceFunction as arguments in the order\ngiven by reduceMatchers. The reduceFunction should yield a single series.\n\nThe resulting list of series are aliased so that they can easily be nested in other functions.\n\n**Example**: Map/Reduce asPercent(bytes_used,total_bytes) for each server\n\nAssume that metrics in the form below exist:\n\n.. code-block:: none\n\n     servers.server1.disk.bytes_used\n     servers.server1.disk.total_bytes\n     servers.server2.disk.bytes_used\n     servers.server2.disk.total_bytes\n     servers.server3.disk.bytes_used\n     servers.server3.disk.total_bytes\n     ...\n     servers.serverN.disk.bytes_used\n     servers.serverN.disk.total_bytes\n\nTo get the percentage of disk used for each server:\n\n.. code-block:: none\n\n    reduceSeries(mapSeries(servers.*.disk.*,1),\"asPercent\",3,\"bytes_used\",\"total_bytes\") =>\n\n      alias(asPercent(servers.server1.disk.bytes_used,servers.server1.disk.total_bytes),\"servers.server1.disk.reduce.asPercent\"),\n      alias(asPercent(servers.server2.disk.bytes_used,servers.server2.disk.total_bytes),\"servers.server2.disk.reduce.asPercent\"),\n      alias(asPercent(servers.server3.disk.bytes_used,servers.server3.disk.total_bytes),\"servers.server3.disk.reduce.asPercent\"),\n      ...\n      alias(asPercent(servers.serverN.disk.bytes_used,servers.serverN.disk.total_bytes),\"servers.serverN.disk.reduce.asPercent\")\n\nIn other words, we will get back the following metrics::\n\n    servers.server1.disk.reduce.asPercent\n    servers.server2.disk.reduce.asPercent\n    servers.server3.disk.reduce.asPercent\n    ...\n    servers.serverN.disk.reduce.asPercent\n\n.. seealso:: :py:func:`mapSeries`",
    "function": "reduce(seriesLists, reduceFunction, reduceNode, *reduceMatchers)",
  },
  "reduceSeries": {
    "description": "Short form: ``reduce()``\n\nTakes a list of seriesLists and reduces it to a list of series by means of the reduceFunction.\n\nReduction is performed by matching the reduceNode in each series against the list of\nreduceMatchers. Then each series is passed to the reduceFunction as arguments in the order\ngiven by reduceMatchers. The reduceFunction should yield a single series.\n\nThe resulting list of series are aliased so that they can easily be nested in other functions.\n\n**Example**: Map/Reduce asPercent(bytes_used,total_bytes) for each server\n\nAssume that metrics in the form below exist:\n\n.. code-block:: none\n\n     servers.server1.disk.bytes_used\n     servers.server1.disk.total_bytes\n     servers.server2.disk.bytes_used\n     servers.server2.disk.total_bytes\n     servers.server3.disk.bytes_used\n     servers.server3.disk.total_bytes\n     ...\n     servers.serverN.disk.bytes_used\n     servers.serverN.disk.total_bytes\n\nTo get the percentage of disk used for each server:\n\n.. code-block:: none\n\n    reduceSeries(mapSeries(servers.*.disk.*,1),\"asPercent\",3,\"bytes_used\",\"total_bytes\") =>\n\n      alias(asPercent(servers.server1.disk.bytes_used,servers.server1.disk.total_bytes),\"servers.server1.disk.reduce.asPercent\"),\n      alias(asPercent(servers.server2.disk.bytes_used,servers.server2.disk.total_bytes),\"servers.server2.disk.reduce.asPercent\"),\n      alias(asPercent(servers.server3.disk.bytes_used,servers.server3.disk.total_bytes),\"servers.server3.disk.reduce.asPercent\"),\n      ...\n      alias(asPercent(servers.serverN.disk.bytes_used,servers.serverN.disk.total_bytes),\"servers.serverN.disk.reduce.asPercent\")\n\nIn other words, we will get back the following metrics::\n\n    servers.server1.disk.reduce.asPercent\n    servers.server2.disk.reduce.asPercent\n    servers.server3.disk.reduce.asPercent\n    ...\n    servers.serverN.disk.reduce.asPercent\n\n.. seealso:: :py:func:`mapSeries`",
    "function": "reduceSeries(seriesLists, reduceFunction, reduceNode, *reduceMatchers)",
  },
  "stddevSeries": {
    "description": "Takes one metric or a wildcard seriesList.\nDraws the standard deviation of all metrics passed at each time.\n\nExample:\n\n.. code-block:: none\n\n  &target=stddevSeries(company.server.*.threads.busy)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``stddev``.",
    "function": "stddevSeries(*seriesLists)",
  },
  "sum": {
    "description": "Short form: sum()\n\nThis will add metrics together and return the sum at each datapoint. (See\nintegral for a sum over time)\n\nExample:\n\n.. code-block:: none\n\n  &target=sum(company.server.application*.requestsHandled)\n\nThis would show the sum of all requests handled per minute (provided\nrequestsHandled are collected once a minute).   If metrics with different\nretention rates are combined, the coarsest metric is graphed, and the sum\nof the other metrics is averaged for the metrics with finer retention rates.\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``sum``.",
    "function": "sum(*seriesLists)",
  },
  "sumSeries": {
    "description": "Short form: sum()\n\nThis will add metrics together and return the sum at each datapoint. (See\nintegral for a sum over time)\n\nExample:\n\n.. code-block:: none\n\n  &target=sum(company.server.application*.requestsHandled)\n\nThis would show the sum of all requests handled per minute (provided\nrequestsHandled are collected once a minute).   If metrics with different\nretention rates are combined, the coarsest metric is graphed, and the sum\nof the other metrics is averaged for the metrics with finer retention rates.\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``sum``.",
    "function": "sumSeries(*seriesLists)",
  },
  "sumSeriesWithWildcards": {
    "description": "Call sumSeries after inserting wildcards at the given position(s).\n\nExample:\n\n.. code-block:: none\n\n  &target=sumSeriesWithWildcards(host.cpu-[0-7].cpu-{user,system}.value, 1)\n\nThis would be the equivalent of\n\n.. code-block:: none\n\n  &target=sumSeries(host.cpu-[0-7].cpu-user.value)&target=sumSeries(host.cpu-[0-7].cpu-system.value)\n\nThis is an alias for :py:func:`aggregateWithWildcards <aggregateWithWildcards>` with aggregation ``sum``.",
    "function": "sumSeriesWithWildcards(seriesList, *position)",
  },
  "weightedAverage": {
    "description": "Takes a series of average values and a series of weights and\nproduces a weighted average for all values.\nThe corresponding values should share one or more zero-indexed nodes and/or tags.\n\nExample:\n\n.. code-block:: none\n\n  &target=weightedAverage(*.transactions.mean,*.transactions.count,0)\n\nEach node may be an integer referencing a node in the series name or a string identifying a tag.",
    "function": "weightedAverage(seriesListAvg, seriesListWeight, *nodes)",
  }

  // filter points group
  "removeAbovePercentile": {
    "description": "Removes data above the nth percentile from the series or list of series provided.\nValues above this percentile are assigned a value of None.",
    "function": "removeAbovePercentile(seriesList, n)",
  },
  "removeAboveValue": {
    "description": "Removes data above the given threshold from the series or list of series provided.\nValues above this threshold are assigned a value of None.",
    "function": "removeAboveValue(seriesList, n)",
  },
  "removeBelowPercentile": {
    "description": "Removes data below the nth percentile from the series or list of series provided.\nValues below this percentile are assigned a value of None.",
    "function": "removeBelowPercentile(seriesList, n)",
  },
  "removeBelowValue": {
    "description": "Removes data below the given threshold from the series or list of series provided.\nValues below this threshold are assigned a value of None.",
    "function": "removeBelowValue(seriesList, n)",
  }

  // filter series group
  "averageAbove": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\nOut of all metrics passed, draws only the metrics with an average value\nabove N for the time period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=averageAbove(server*.instance*.threads.busy,25)\n\nDraws the servers with average values above 25.",
    "function": "averageAbove(seriesList, n)",
  },
  "averageBelow": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\nOut of all metrics passed, draws only the metrics with an average value\nbelow N for the time period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=averageBelow(server*.instance*.threads.busy,25)\n\nDraws the servers with average values below 25.",
    "function": "averageBelow(seriesList, n)",
  },
  "averageOutsidePercentile": {
    "description": "Removes series lying inside an average percentile interval",
    "function": "averageOutsidePercentile(seriesList, n)",
  },
  "currentAbove": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\nOut of all metrics passed, draws only the  metrics whose value is above N\nat the end of the time period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=currentAbove(server*.instance*.threads.busy,50)\n\nDraws the servers with more than 50 busy threads.",
    "function": "currentAbove(seriesList, n)",
  },
  "currentBelow": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\nOut of all metrics passed, draws only the  metrics whose value is below N\nat the end of the time period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=currentBelow(server*.instance*.threads.busy,3)\n\nDraws the servers with less than 3 busy threads.",
    "function": "currentBelow(seriesList, n)",
  },
  "exclude": {
    "description": "Takes a metric or a wildcard seriesList, followed by a regular expression\nin double quotes.  Excludes metrics that match the regular expression.\n\nExample:\n\n.. code-block:: none\n\n  &target=exclude(servers*.instance*.threads.busy,\"server02\")",
    "function": "exclude(seriesList, pattern)",
  },
  "filterSeries": {
    "description": "Takes one metric or a wildcard seriesList followed by a consolidation function, an operator and a threshold.\nDraws only the metrics which match the filter expression.\n\nExample:\n\n.. code-block:: none\n\n  &target=filterSeries(system.interface.eth*.packetsSent, 'max', '>', 1000)\n\nThis would only display interfaces which has a peak throughput higher than 1000 packets/min.\n\nSupported aggregation functions: ``average``, ``median``, ``sum``, ``min``,\n``max``, ``diff``, ``stddev``, ``range``, ``multiply`` & ``last``.\n\nSupported operators: ``=``, ``!=``, ``>``, ``>=``, ``<`` & ``<=``.",
    "function": "filterSeries(seriesList, func, operator, threshold)",
    "operator_options": ["!=", "<", "<=", "=", ">", ">="],
  },
  "grep": {
    "description": "Takes a metric or a wildcard seriesList, followed by a regular expression\nin double quotes.  Excludes metrics that don't match the regular expression.\n\nExample:\n\n.. code-block:: none\n\n  &target=grep(servers*.instance*.threads.busy,\"server02\")",
    "function": "grep(seriesList, pattern)",
  },
  "highest": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N and an aggregation function.\nOut of all metrics passed, draws only the N metrics with the highest aggregated value over the\ntime period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=highest(server*.instance*.threads.busy,5,'max')\n\nDraws the 5 servers with the highest number of busy threads.",
    "function": "highest(seriesList, n=1, func='average')",
  },
  "highestAverage": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\nOut of all metrics passed, draws only the top N metrics with the highest\naverage value for the time period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=highestAverage(server*.instance*.threads.busy,5)\n\nDraws the top 5 servers with the highest average value.\n\nThis is an alias for :py:func:`highest <highest>` with aggregation ``average``.",
    "function": "highestAverage(seriesList, n)",
  },
  "highestCurrent": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\nOut of all metrics passed, draws only the N metrics with the highest value\nat the end of the time period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=highestCurrent(server*.instance*.threads.busy,5)\n\nDraws the 5 servers with the highest busy threads.\n\nThis is an alias for :py:func:`highest <highest>` with aggregation ``current``.",
    "function": "highestCurrent(seriesList, n)",
  },
  "highestMax": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\n\nOut of all metrics passed, draws only the N metrics with the highest maximum\nvalue in the time period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=highestMax(server*.instance*.threads.busy,5)\n\nDraws the top 5 servers who have had the most busy threads during the time\nperiod specified.\n\nThis is an alias for :py:func:`highest <highest>` with aggregation ``max``.",
    "function": "highestMax(seriesList, n)",
  },
  "limit": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\n\nOnly draw the first N metrics.  Useful when testing a wildcard in a metric.\n\nExample:\n\n.. code-block:: none\n\n  &target=limit(server*.instance*.memory.free,5)\n\nDraws only the first 5 instance's memory free.",
    "function": "limit(seriesList, n)",
  },
  "lowest": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N and an aggregation function.\nOut of all metrics passed, draws only the N metrics with the lowest aggregated value over the\ntime period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=lowest(server*.instance*.threads.busy,5,'min')\n\nDraws the 5 servers with the lowest number of busy threads.",
    "function": "lowest(seriesList, n=1, func='average')",
  },
  "lowestAverage": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\nOut of all metrics passed, draws only the bottom N metrics with the lowest\naverage value for the time period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=lowestAverage(server*.instance*.threads.busy,5)\n\nDraws the bottom 5 servers with the lowest average value.\n\nThis is an alias for :py:func:`lowest <lowest>` with aggregation ``average``.",
    "function": "lowestAverage(seriesList, n)",
  },
  "lowestCurrent": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\nOut of all metrics passed, draws only the N metrics with the lowest value at\nthe end of the time period specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=lowestCurrent(server*.instance*.threads.busy,5)\n\nDraws the 5 servers with the least busy threads right now.\n\nThis is an alias for :py:func:`lowest <lowest>` with aggregation ``current``.",
    "function": "lowestCurrent(seriesList, n)",
  },
  "maximumAbove": {
    "description": "Takes one metric or a wildcard seriesList followed by a constant n.\nDraws only the metrics with a maximum value above n.\n\nExample:\n\n.. code-block:: none\n\n  &target=maximumAbove(system.interface.eth*.packetsSent,1000)\n\nThis would only display interfaces which sent more than 1000 packets/min.",
    "function": "maximumAbove(seriesList, n)",
  },
  "maximumBelow": {
    "description": "Takes one metric or a wildcard seriesList followed by a constant n.\nDraws only the metrics with a maximum value below n.\n\nExample:\n\n.. code-block:: none\n\n  &target=maximumBelow(system.interface.eth*.packetsSent,1000)\n\nThis would only display interfaces which sent less than 1000 packets/min.",
    "function": "maximumBelow(seriesList, n)",
  },
  "minimumAbove": {
    "description": "Takes one metric or a wildcard seriesList followed by a constant n.\nDraws only the metrics with a minimum value above n.\n\nExample:\n\n.. code-block:: none\n\n  &target=minimumAbove(system.interface.eth*.packetsSent,1000)\n\nThis would only display interfaces which sent more than 1000 packets/min.",
    "function": "minimumAbove(seriesList, n)",
  },
  "minimumBelow": {
    "description": "Takes one metric or a wildcard seriesList followed by a constant n.\nDraws only the metrics with a minimum value below n.\n\nExample:\n\n.. code-block:: none\n\n  &target=minimumBelow(system.interface.eth*.packetsSent,1000)\n\nThis would only display interfaces which at one point sent less than 1000 packets/min.",
    "function": "minimumBelow(seriesList, n)",
  },
  "mostDeviant": {
    "description": "Takes one metric or a wildcard seriesList followed by an integer N.\nDraws the N most deviant metrics.\nTo find the deviants, the standard deviation (sigma) of each series\nis taken and ranked. The top N standard deviations are returned.\n\n  Example:\n\n.. code-block:: none\n\n  &target=mostDeviant(server*.instance*.memory.free, 5)\n\nDraws the 5 instances furthest from the average memory free.",
    "function": "mostDeviant(seriesList, n)",
  },
  "removeBetweenPercentile": {
    "description": "Removes series that do not have an value lying in the x-percentile of all the values at a moment",
    "function": "removeBetweenPercentile(seriesList, n)",
  },
  "removeEmptySeries": {
    "description": "Takes one metric or a wildcard seriesList.\nOut of all metrics passed, draws only the metrics with not empty data\n\nExample:\n\n.. code-block:: none\n\n  &target=removeEmptySeries(server*.instance*.threads.busy)\n\nDraws only live servers with not empty data.\n\n`xFilesFactor` follows the same semantics as in Whisper storage schemas.  Setting it to 0 (the\ndefault) means that only a single value in the series needs to be non-null for it to be\nconsidered non-empty, setting it to 1 means that all values in the series must be non-null.\nA setting of 0.5 means that at least half the values in the series must be non-null.",
    "function": "removeEmptySeries(seriesList, xFilesFactor=None)",
  },
  "unique": {
    "description": "Takes an arbitrary number of seriesLists and returns unique series, filtered by name.\n\nExample:\n\n.. code-block:: none\n\n  &target=unique(mostDeviant(server.*.disk_free,5),lowestCurrent(server.*.disk_free,5))\n\nDraws servers with low disk space, and servers with highly deviant disk space, but never the same series twice.",
    "function": "unique(*seriesLists)",
  },
  "useSeriesAbove": {
    "description": "Compares the maximum of each series against the given `value`. If the series\nmaximum is greater than `value`, the regular expression search and replace is\napplied against the series name to plot a related metric\n\ne.g. given useSeriesAbove(ganglia.metric1.reqs,10,'reqs','time'),\nthe response time metric will be plotted only when the maximum value of the\ncorresponding request/s metric is > 10\n\n.. code-block:: none\n\n  &target=useSeriesAbove(ganglia.metric1.reqs,10,\"reqs\",\"time\")",
    "function": "useSeriesAbove(seriesList, value, search, replace)",
  }

  // graph group
  "alpha": {
    "description": "Assigns the given alpha transparency setting to the series. Takes a float value between 0 and 1.",
    "function": "alpha(seriesList, alpha)",
  },
  "areaBetween": {
    "description": "Draws the vertical area in between the two series in seriesList. Useful for\nvisualizing a range such as the minimum and maximum latency for a service.\n\nareaBetween expects **exactly one argument** that results in exactly two series\n(see example below). The order of the lower and higher values series does not\nmatter. The visualization only works when used in conjunction with\n``areaMode=stacked``.\n\nMost likely use case is to provide a band within which another metric should\nmove. In such case applying an ``alpha()``, as in the second example, gives\nbest visual results.\n\nExample:\n\n.. code-block:: none\n\n  &target=areaBetween(service.latency.{min,max})&areaMode=stacked\n\n  &target=alpha(areaBetween(service.latency.{min,max}),0.3)&areaMode=stacked\n\nIf for instance, you need to build a seriesList, you should use the ``group``\nfunction, like so:\n\n.. code-block:: none\n\n  &target=areaBetween(group(minSeries(a.*.min),maxSeries(a.*.max)))",
    "function": "areaBetween(seriesList)",
  },
  "color": {
    "description": "Assigns the given color to the seriesList\n\nExample:\n\n.. code-block:: none\n\n  &target=color(collectd.hostname.cpu.0.user, 'green')\n  &target=color(collectd.hostname.cpu.0.system, 'ff0000')\n  &target=color(collectd.hostname.cpu.0.idle, 'gray')\n  &target=color(collectd.hostname.cpu.0.idle, '6464ffaa')",
    "function": "color(seriesList, theColor)",
  },
  "dashed": {
    "description": "Takes one metric or a wildcard seriesList, followed by a float F.\n\nDraw the selected metrics with a dotted line with segments of length F\nIf omitted, the default length of the segments is 5.0\n\nExample:\n\n.. code-block:: none\n\n  &target=dashed(server01.instance01.memory.free,2.5)",
    "function": "dashed(seriesList, dashLength=5)",
  },
  "drawAsInfinite": {
    "description": "Takes one metric or a wildcard seriesList.\nIf the value is zero, draw the line at 0.  If the value is above zero, draw\nthe line at infinity. If the value is null or less than zero, do not draw\nthe line.\n\nUseful for displaying on/off metrics, such as exit codes. (0 = success,\nanything else = failure.)\n\nExample:\n\n.. code-block:: none\n\n  drawAsInfinite(Testing.script.exitCode)",
    "function": "drawAsInfinite(seriesList)",
  },
  "lineWidth": {
    "description": "Takes one metric or a wildcard seriesList, followed by a float F.\n\nDraw the selected metrics with a line width of F, overriding the default\nvalue of 1, or the &lineWidth=X.X parameter.\n\nUseful for highlighting a single metric out of many, or having multiple\nline widths in one graph.\n\nExample:\n\n.. code-block:: none\n\n  &target=lineWidth(server01.instance01.memory.free,5)",
    "function": "lineWidth(seriesList, width)",
  },
  "secondYAxis": {
    "description": "Graph the series on the secondary Y axis.",
    "function": "secondYAxis(seriesList)",
  },
  "stacked": {
    "description": "Takes one metric or a wildcard seriesList and change them so they are\nstacked. This is a way of stacking just a couple of metrics without having\nto use the stacked area mode (that stacks everything). By means of this a mixed\nstacked and non stacked graph can be made\n\nIt can also take an optional argument with a name of the stack, in case there is\nmore than one, e.g. for input and output metrics.\n\nExample:\n\n.. code-block:: none\n\n  &target=stacked(company.server.application01.ifconfig.TXPackets, 'tx')",
    "function": "stacked(seriesLists, stackName='__DEFAULT__')",
  },
  "threshold": {
    "description": "Takes a float F, followed by a label (in double quotes) and a color.\n(See ``bgcolor`` in the render\\_api_ for valid color names & formats.)\n\nDraws a horizontal line at value F across the graph.\n\nExample:\n\n.. code-block:: none\n\n  &target=threshold(123.456, \"omgwtfbbq\", \"red\")",
    "function": "threshold(value, label=None, color=None)",
  },
  "verticalLine": {
    "description": "Takes a timestamp string ts.\n\nDraws a vertical line at the designated timestamp with optional\n'label' and 'color'. Supported timestamp formats include both\nrelative (e.g. -3h) and absolute (e.g. 16:00_20110501) strings,\nsuch as those used with ``from`` and ``until`` parameters. When\nset, the 'label' will appear in the graph legend.\n\nNote: Any timestamps defined outside the requested range will\nraise a 'ValueError' exception.\n\nExample:\n\n.. code-block:: none\n\n  &target=verticalLine(\"12:3420131108\",\"event\",\"blue\")\n  &target=verticalLine(\"16:00_20110501\",\"event\")\n  &target=verticalLine(\"-5mins\")",
    "function": "verticalLine(ts, label=None, color=None)",
  }

  // sorting functions
  "sortBy": {
    "description": "Takes one metric or a wildcard seriesList followed by an aggregation function and an\noptional ``reverse`` parameter.\n\nReturns the metrics sorted according to the specified function.\n\nExample:\n\n.. code-block:: none\n\n  &target=sortBy(server*.instance*.threads.busy,'max')\n\nDraws the servers in ascending order by maximum.",
    "function": "sortBy(seriesList, func='average', reverse=False)",
  },
  "sortByMaxima": {
    "description": "Takes one metric or a wildcard seriesList.\n\nSorts the list of metrics in descending order by the maximum value across the time period\nspecified.  Useful with the &areaMode=all parameter, to keep the\nlowest value lines visible.\n\nExample:\n\n.. code-block:: none\n\n  &target=sortByMaxima(server*.instance*.memory.free)",
    "function": "sortByMaxima(seriesList)",
  },
  "sortByMinima": {
    "description": "Takes one metric or a wildcard seriesList.\n\nSorts the list of metrics by the lowest value across the time period\nspecified, including only series that have a maximum value greater than 0.\n\nExample:\n\n.. code-block:: none\n\n  &target=sortByMinima(server*.instance*.memory.free)",
    "function": "sortByMinima(seriesList)",
  },
  "sortByName": {
    "description": "Takes one metric or a wildcard seriesList.\nSorts the list of metrics by the metric name using either alphabetical order or natural sorting.\nNatural sorting allows names containing numbers to be sorted more naturally, e.g:\n- Alphabetical sorting: server1, server11, server12, server2\n- Natural sorting: server1, server2, server11, server12",
    "function": "sortByName(seriesList, natural=False, reverse=False)",
  },
  "sortByTotal": {
    "description": "Takes one metric or a wildcard seriesList.\n\nSorts the list of metrics in descending order by the sum of values across the time period\nspecified.",
    "function": "sortByTotal(seriesList)",
  }

  // special functions
  "cactiStyle": {
    "description": "Takes a series list and modifies the aliases to provide column aligned\noutput with Current, Max, and Min values in the style of cacti. Optionally\ntakes a \"system\" value to apply unit formatting in the same style as the\nY-axis, or a \"unit\" string to append an arbitrary unit suffix.\n\n.. code-block:: none\n\n  &target=cactiStyle(ganglia.*.net.bytes_out,\"si\")\n  &target=cactiStyle(ganglia.*.net.bytes_out,\"si\",\"b\")\n\nA possible value for ``system`` is ``si``, which would express your values in\nmultiples of a thousand. A second option is to use ``binary`` which will\ninstead express your values in multiples of 1024 (useful for network devices).\n\nColumn alignment of the Current, Max, Min values works under two conditions:\nyou use a monospace font such as terminus and use a single cactiStyle call, as\nseparate cactiStyle calls are not aware of each other. In case you have\ndifferent targets for which you would like to have cactiStyle to line up, you\ncan use ``group()`` to combine them before applying cactiStyle, such as:\n\n.. code-block:: none\n\n  &target=cactiStyle(group(metricA,metricB))",
    "function": "cactiStyle(seriesList, system=None, units=None)",
    "system_options": ["si", "binary"],
  },
  "changed": {
    "description": "Takes one metric or a wildcard seriesList.\nOutput 1 when the value changed, 0 when null or the same\n\nExample:\n\n.. code-block:: none\n\n  &target=changed(Server01.connections.handled)",
    "function": "changed(seriesList)",
  },
  "consolidateBy": {
    "description": "Takes one metric or a wildcard seriesList and a consolidation function name.\n\nValid function names are 'sum', 'average', 'min', 'max', 'first' & 'last'.\n\nWhen a graph is drawn where width of the graph size in pixels is smaller than\nthe number of datapoints to be graphed, Graphite consolidates the values to\nto prevent line overlap. The consolidateBy() function changes the consolidation\nfunction from the default of 'average' to one of 'sum', 'max', 'min', 'first', or 'last'.\nThis is especially useful in sales graphs, where fractional values make no sense and a 'sum'\nof consolidated values is appropriate.\n\n.. code-block:: none\n\n  &target=consolidateBy(Sales.widgets.largeBlue, 'sum')\n  &target=consolidateBy(Servers.web01.sda1.free_space, 'max')",
    "function": "consolidateBy(seriesList, consolidationFunc)",
    "function_options": ["sum", "average", "min", "max", "first", "last"],
  },
  "constantLine": {
    "description": "Takes a float F.\n\nDraws a horizontal line at value F across the graph.\n\nExample:\n\n.. code-block:: none\n\n  &target=constantLine(123.456)",
    "function": "constantLine(value)",
  },
  "cumulative": {
    "description": "Takes one metric or a wildcard seriesList.\n\nWhen a graph is drawn where width of the graph size in pixels is smaller than\nthe number of datapoints to be graphed, Graphite consolidates the values to\nto prevent line overlap. The cumulative() function changes the consolidation\nfunction from the default of 'average' to 'sum'. This is especially useful in\nsales graphs, where fractional values make no sense and a 'sum' of consolidated\nvalues is appropriate.\n\nAlias for :func:`consolidateBy(series, 'sum') <graphite.render.functions.consolidateBy>`\n\n.. code-block:: none\n\n  &target=cumulative(Sales.widgets.largeBlue)",
    "function": "cumulative(seriesList)",
  },
  "events": {
    "description": "Returns the number of events at this point in time. Usable with\ndrawAsInfinite.\n\nExample:\n\n.. code-block:: none\n\n  &target=events(\"tag-one\", \"tag-two\")\n  &target=events(\"*\")\n\nReturns all events tagged as \"tag-one\" and \"tag-two\" and the second one\nreturns all events.",
    "function": "events(*tags)",
  },
  "fallbackSeries": {
    "description": "Takes a wildcard seriesList, and a second fallback metric.\nIf the wildcard does not match any series, draws the fallback metric.\n\nExample:\n\n.. code-block:: none\n\n  &target=fallbackSeries(server*.requests_per_second, constantLine(0))\n\nDraws a 0 line when server metric does not exist.",
    "function": "fallbackSeries(seriesList, fallback)",
  },
  "seriesByTag": {
    "description": "Returns a SeriesList of series matching all the specified tag expressions.\n\nExample:\n\n.. code-block:: none\n\n  &target=seriesByTag(\"tag1=value1\",\"tag2!=value2\")\n\nReturns a seriesList of all series that have tag1 set to value1, AND do not have tag2 set to value2.\n\nTags specifiers are strings, and may have the following formats:\n\n.. code-block:: none\n\n  tag=spec    tag value exactly matches spec\n  tag!=spec   tag value does not exactly match spec\n  tag=~value  tag value matches the regular expression spec\n  tag!=~spec  tag value does not match the regular expression spec\n\nAny tag spec that matches an empty value is considered to match series that don't have that tag.\n\nAt least one tag spec must require a non-empty value.\n\nRegular expression conditions are treated as being anchored at the start of the value.\n\nSee :ref:`querying tagged series <querying-tagged-series>` for more detail.",
    "function": "seriesByTag(*tagExpressions)",
  },
  "setXFilesFactor": {
    "description": "Short form: xFilesFactor()\n\nTakes one metric or a wildcard seriesList and an xFilesFactor value between 0 and 1\n\nWhen a series needs to be consolidated, this sets the fraction of values in an interval that must\nnot be null for the consolidation to be considered valid.  If there are not enough values then\nNone will be returned for that interval.\n\n.. code-block:: none\n\n  &target=xFilesFactor(Sales.widgets.largeBlue, 0.5)\n  &target=Servers.web01.sda1.free_space|consolidateBy('max')|xFilesFactor(0.5)\n\nThe `xFilesFactor` set via this function is used as the default for all functions that accept an\n`xFilesFactor` parameter, all functions that aggregate data across multiple series and/or\nintervals, and `maxDataPoints <render_api.html#maxdatapoints>`_ consolidation.\n\nA default for the entire render request can also be set using the\n`xFilesFactor <render_api.html#xfilesfactor>`_ query parameter.\n\n.. note::\n\n  `xFilesFactor` follows the same semantics as in Whisper storage schemas.  Setting it to 0 (the\n  default) means that only a single value in a given interval needs to be non-null, setting it to\n  1 means that all values in the interval must be non-null.  A setting of 0.5 means that at least\n  half the values in the interval must be non-null.",
    "function": "setXFilesFactor(seriesList, xFilesFactor)",
  },
  "substr": {
    "description": "Takes one metric or a wildcard seriesList followed by 1 or 2 integers.  Assume that the\nmetric name is a list or array, with each element separated by dots.  Prints\nn - length elements of the array (if only one integer n is passed) or n - m\nelements of the array (if two integers n and m are passed).  The list starts\nwith element 0 and ends with element (length - 1).\n\nExample:\n\n.. code-block:: none\n\n  &target=substr(carbon.agents.hostname.avgUpdateTime,2,4)\n\nThe label would be printed as \"hostname.avgUpdateTime\".",
    "function": "substr(seriesList, start=0, stop=0)",
  },
  "xFilesFactor": {
    "description": "Short form: xFilesFactor()\n\nTakes one metric or a wildcard seriesList and an xFilesFactor value between 0 and 1\n\nWhen a series needs to be consolidated, this sets the fraction of values in an interval that must\nnot be null for the consolidation to be considered valid.  If there are not enough values then\nNone will be returned for that interval.\n\n.. code-block:: none\n\n  &target=xFilesFactor(Sales.widgets.largeBlue, 0.5)\n  &target=Servers.web01.sda1.free_space|consolidateBy('max')|xFilesFactor(0.5)\n\nThe `xFilesFactor` set via this function is used as the default for all functions that accept an\n`xFilesFactor` parameter, all functions that aggregate data across multiple series and/or\nintervals, and `maxDataPoints <render_api.html#maxdatapoints>`_ consolidation.\n\nA default for the entire render request can also be set using the\n`xFilesFactor <render_api.html#xfilesfactor>`_ query parameter.\n\n.. note::\n\n  `xFilesFactor` follows the same semantics as in Whisper storage schemas.  Setting it to 0 (the\n  default) means that only a single value in a given interval needs to be non-null, setting it to\n  1 means that all values in the interval must be non-null.  A setting of 0.5 means that at least\n  half the values in the interval must be non-null.",
    "function": "xFilesFactor(seriesList, xFilesFactor)",
  }

  // transform group
  "absolute": {
    "description": "Takes one metric or a wildcard seriesList and applies the mathematical abs function to each\ndatapoint transforming it to its absolute value.\n\nExample:\n\n.. code-block:: none\n\n  &target=absolute(Server.instance01.threads.busy)\n  &target=absolute(Server.instance*.threads.busy)",
    "function": "absolute(seriesList)",
  },
  "delay": {
    "description": "This shifts all samples later by an integer number of steps. This can be\nused for custom derivative calculations, among other things. Note: this\nwill pad the early end of the data with None for every step shifted.\n\nThis complements other time-displacement functions such as timeShift and\ntimeSlice, in that this function is indifferent about the step intervals\nbeing shifted.\n\nExample:\n\n.. code-block:: none\n\n  &target=divideSeries(server.FreeSpace,delay(server.FreeSpace,1))\n\nThis computes the change in server free space as a percentage of the previous\nfree space.",
    "function": "delay(seriesList, steps)",
  },
  "derivative": {
    "description": "This is the opposite of the integral function.  This is useful for taking a\nrunning total metric and calculating the delta between subsequent data points.\n\nThis function does not normalize for periods of time, as a true derivative would.\nInstead see the perSecond() function to calculate a rate of change over time.\n\nExample:\n\n.. code-block:: none\n\n  &target=derivative(company.server.application01.ifconfig.TXPackets)\n\nEach time you run ifconfig, the RX and TXPackets are higher (assuming there\nis network traffic.) By applying the derivative function, you can get an\nidea of the packets per minute sent or received, even though you're only\nrecording the total.",
    "function": "derivative(seriesList)",
  },
  "hitcount": {
    "description": "Estimate hit counts from a list of time series.\n\nThis function assumes the values in each time series represent\nhits per second.  It calculates hits per some larger interval\nsuch as per day or per hour.  This function is like summarize(),\nexcept that it compensates automatically for different time scales\n(so that a similar graph results from using either fine-grained\nor coarse-grained records) and handles rarely-occurring events\ngracefully.",
    "function": "hitcount(seriesList, intervalString, alignToInterval=False)",
  },
  "integral": {
    "description": "This will show the sum over time, sort of like a continuous addition function.\nUseful for finding totals or trends in metrics that are collected per minute.\n\nExample:\n\n.. code-block:: none\n\n  &target=integral(company.sales.perMinute)\n\nThis would start at zero on the left side of the graph, adding the sales each\nminute, and show the total sales for the time period selected at the right\nside, (time now, or the time specified by '&until=').",
    "function": "integral(seriesList)",
  },
  "integralByInterval": {
    "description": "This will do the same as integral() function, except resetting the total to 0\nat the given time in the parameter \"from\"\nUseful for finding totals per hour/day/week/..\n\nExample:\n\n.. code-block:: none\n\n  &target=integralByInterval(company.sales.perMinute, \"1d\")&from=midnight-10days\n\nThis would start at zero on the left side of the graph, adding the sales each\nminute, and show the evolution of sales per day during the last 10 days.",
    "function": "integralByInterval(seriesList, intervalUnit)",
  },
  "interpolate": {
    "description": "Takes one metric or a wildcard seriesList, and optionally a limit to the number of 'None' values to skip over.\nContinues the line with the last received value when gaps ('None' values) appear in your data, rather than breaking your line.\n\nExample:\n\n.. code-block:: none\n\n  &target=interpolate(Server01.connections.handled)\n  &target=interpolate(Server01.connections.handled, 10)",
    "function": "interpolate(seriesList, limit=inf)",
  },
  "invert": {
    "description": "Takes one metric or a wildcard seriesList, and inverts each datapoint (i.e. 1/x).\n\nExample:\n\n.. code-block:: none\n\n  &target=invert(Server.instance01.threads.busy)",
    "function": "invert(seriesList)",
  },
  "keepLastValue": {
    "description": "Takes one metric or a wildcard seriesList, and optionally a limit to the number of 'None' values to skip over.\nContinues the line with the last received value when gaps ('None' values) appear in your data, rather than breaking your line.\n\nExample:\n\n.. code-block:: none\n\n  &target=keepLastValue(Server01.connections.handled)\n  &target=keepLastValue(Server01.connections.handled, 10)",
    "function": "keepLastValue(seriesList, limit=inf)",
  },
  "log": {
    "description": "Takes one metric or a wildcard seriesList, a base, and draws the y-axis in logarithmic\nformat.  If base is omitted, the function defaults to base 10.\n\nExample:\n\n.. code-block:: none\n\n  &target=log(carbon.agents.hostname.avgUpdateTime,2)",
    "function": "log(seriesList, base=10)",
  },
  "minMax": {
    "description": "Applies the popular min max normalization technique, which takes\neach point and applies the following normalization transformation\nto it: normalized = (point - min) / (max - min).\n\nExample:\n\n.. code-block:: none\n\n  &target=minMax(Server.instance01.threads.busy)",
    "function": "minMax(seriesList)",
  },
  "nonNegativeDerivative": {
    "description": "Same as the derivative function above, but ignores datapoints that trend\ndown.  Useful for counters that increase for a long time, then wrap or\nreset. (Such as if a network interface is destroyed and recreated by unloading\nand re-loading a kernel module, common with USB / WiFi cards.\n\nExample:\n\n.. code-block:: none\n\n  &target=nonNegativederivative(company.server.application01.ifconfig.TXPackets)",
    "function": "nonNegativeDerivative(seriesList, maxValue=None)",
  },
  "offset": {
    "description": "Takes one metric or a wildcard seriesList followed by a constant, and adds the constant to\neach datapoint.\n\nExample:\n\n.. code-block:: none\n\n  &target=offset(Server.instance01.threads.busy,10)",
    "function": "offset(seriesList, factor)",
  },
  "offsetToZero": {
    "description": "Offsets a metric or wildcard seriesList by subtracting the minimum\nvalue in the series from each datapoint.\n\nUseful to compare different series where the values in each series\nmay be higher or lower on average but you're only interested in the\nrelative difference.\n\nAn example use case is for comparing different round trip time\nresults. When measuring RTT (like pinging a server), different\ndevices may come back with consistently different results due to\nnetwork latency which will be different depending on how many\nnetwork hops between the probe and the device. To compare different\ndevices in the same graph, the network latency to each has to be\nfactored out of the results. This is a shortcut that takes the\nfastest response (lowest number in the series) and sets that to zero\nand then offsets all of the other datapoints in that series by that\namount. This makes the assumption that the lowest response is the\nfastest the device can respond, of course the more datapoints that\nare in the series the more accurate this assumption is.\n\nExample:\n\n.. code-block:: none\n\n  &target=offsetToZero(Server.instance01.responseTime)\n  &target=offsetToZero(Server.instance*.responseTime)",
    "function": "offsetToZero(seriesList)",
  },
  "perSecond": {
    "description": "NonNegativeDerivative adjusted for the series time interval\nThis is useful for taking a running total metric and showing how many requests\nper second were handled.\n\nExample:\n\n.. code-block:: none\n\n  &target=perSecond(company.server.application01.ifconfig.TXPackets)\n\nEach time you run ifconfig, the RX and TXPackets are higher (assuming there\nis network traffic.) By applying the perSecond function, you can get an\nidea of the packets per second sent or received, even though you're only\nrecording the total.",
    "function": "perSecond(seriesList, maxValue=None)",
  },
  "pow": {
    "description": "Takes one metric or a wildcard seriesList followed by a constant, and raises the datapoint\nby the power of the constant provided at each point.\n\nExample:\n\n.. code-block:: none\n\n  &target=pow(Server.instance01.threads.busy,10)\n  &target=pow(Server.instance*.threads.busy,10)",
    "function": "pow(seriesList, factor)",
  },
  "powSeries": {
    "description": "Takes two or more series and pows their points. A constant line may be\nused.\n\nExample:\n\n.. code-block:: none\n\n  &target=powSeries(Server.instance01.app.requests, Server.instance01.app.replies)",
    "function": "powSeries(*seriesLists)",
  },
  "round": {
    "description": "Takes one metric or a wildcard seriesList optionally followed by a precision, and rounds each\ndatapoint to the specified precision.\n\nExample:\n\n.. code-block:: none\n\n  &target=round(Server.instance01.threads.busy)\n  &target=round(Server.instance01.threads.busy,2)",
    "function": "round(seriesList, precision=None)",
  },
  "scaleToSeconds": {
    "description": "Takes one metric or a wildcard seriesList and returns \"value per seconds\" where\nseconds is a last argument to this functions.\n\nUseful in conjunction with derivative or integral function if you want\nto normalize its result to a known resolution for arbitrary retentions",
    "function": "scaleToSeconds(seriesList, seconds)",
  },
  "smartSummarize": {
    "description": "Smarter version of summarize.\n\nThe alignToFrom boolean parameter has been replaced by alignTo and no longer has any effect.\nAlignment can be to years, months, weeks, days, hours, and minutes.\n\nThis function can be used with aggregation functions ``average``, ``median``, ``sum``, ``min``,\n``max``, ``diff``, ``stddev``, ``count``, ``range``, ``multiply`` & ``last``.",
    "function": "smartSummarize(seriesList, intervalString, func='sum', alignTo=None)",
  },
  "squareRoot": {
    "description": "Takes one metric or a wildcard seriesList, and computes the square root of each datapoint.\n\nExample:\n\n.. code-block:: none\n\n  &target=squareRoot(Server.instance01.threads.busy)",
    "function": "squareRoot(seriesList)",
  },
  "summarize": {
    "description": "Summarize the data into interval buckets of a certain size.\n\nBy default, the contents of each interval bucket are summed together. This is\nuseful for counters where each increment represents a discrete event and\nretrieving a \"per X\" value requires summing all the events in that interval.\n\nSpecifying 'average' instead will return the mean for each bucket, which can be more\nuseful when the value is a gauge that represents a certain value in time.\n\nThis function can be used with aggregation functions ``average``, ``median``, ``sum``, ``min``,\n``max``, ``diff``, ``stddev``, ``count``, ``range``, ``multiply`` & ``last``.\n\nBy default, buckets are calculated by rounding to the nearest interval. This\nworks well for intervals smaller than a day. For example, 22:32 will end up\nin the bucket 22:00-23:00 when the interval=1hour.\n\nPassing alignToFrom=true will instead create buckets starting at the from\ntime. In this case, the bucket for 22:32 depends on the from time. If\nfrom=6:30 then the 1hour bucket for 22:32 is 22:30-23:30.\n\nExample:\n\n.. code-block:: none\n\n  &target=summarize(counter.errors, \"1hour\") # total errors per hour\n  &target=summarize(nonNegativeDerivative(gauge.num_users), \"1week\") # new users per week\n  &target=summarize(queue.size, \"1hour\", \"avg\") # average queue size per hour\n  &target=summarize(queue.size, \"1hour\", \"max\") # maximum queue size during each hour\n  &target=summarize(metric, \"13week\", \"avg\", true)&from=midnight+20100101 # 2010 Q1-4",
    "function": "summarize(seriesList, intervalString, func='sum', alignToFrom=False)",
  },
  "timeShift": {
    "description": "Takes one metric or a wildcard seriesList, followed by a quoted string with the\nlength of time (See ``from / until`` in the render\\_api_ for examples of time formats).\n\nDraws the selected metrics shifted in time. If no sign is given, a minus sign ( - ) is\nimplied which will shift the metric back in time. If a plus sign ( + ) is given, the\nmetric will be shifted forward in time.\n\nWill reset the end date range automatically to the end of the base stat unless\nresetEnd is False. Example case is when you timeshift to last week and have the graph\ndate range set to include a time in the future, will limit this timeshift to pretend\nending at the current time. If resetEnd is False, will instead draw full range including\nfuture time.\n\nBecause time is shifted by a fixed number of seconds, comparing a time period with DST to\na time period without DST, and vice-versa, will result in an apparent misalignment. For\nexample, 8am might be overlaid with 7am. To compensate for this, use the alignDST option.\n\nUseful for comparing a metric against itself at a past periods or correcting data\nstored at an offset.\n\nExample:\n\n.. code-block:: none\n\n  &target=timeShift(Sales.widgets.largeBlue,\"7d\")\n  &target=timeShift(Sales.widgets.largeBlue,\"-7d\")\n  &target=timeShift(Sales.widgets.largeBlue,\"+1h\")",
    "function": "timeShift(seriesList, timeShift, resetEnd=True, alignDST=False)",
  },
  "timeSlice": {
    "description": "Takes one metric or a wildcard metric, followed by a quoted string with the\ntime to start the line and another quoted string with the time to end the line.\nThe start and end times are inclusive. See ``from / until`` in the render\\_api_\nfor examples of time formats.\n\nUseful for filtering out a part of a series of data from a wider range of\ndata.\n\nExample:\n\n.. code-block:: none\n\n  &target=timeSlice(network.core.port1,\"00:00 20140101\",\"11:59 20140630\")\n  &target=timeSlice(network.core.port1,\"12:00 20140630\",\"now\")",
    "function": "timeSlice(seriesList, startSliceAt, endSliceAt='now')",
  },
  "timeStack": {
    "description": "Takes one metric or a wildcard seriesList, followed by a quoted string with the\nlength of time (See ``from / until`` in the render\\_api_ for examples of time formats).\nAlso takes a start multiplier and end multiplier for the length of time\n\ncreate a seriesList which is composed the original metric series stacked with time shifts\nstarting time shifts from the start multiplier through the end multiplier\n\nUseful for looking at history, or feeding into averageSeries or stddevSeries.\n\nExample:\n\n.. code-block:: none\n\n  &target=timeStack(Sales.widgets.largeBlue,\"1d\",0,7)    # create a series for today and each of the previous 7 days",
    "function": "timeStack(seriesList, timeShiftUnit='1d', timeShiftStart=0, timeShiftEnd=7)",
  },
  "transformNull": {
    "description": "Takes a metric or wildcard seriesList and replaces null values with the value\nspecified by `default`.  The value 0 used if not specified.  The optional\nreferenceSeries, if specified, is a metric or wildcard series list that governs\nwhich time intervals nulls should be replaced.  If specified, nulls are replaced\nonly in intervals where a non-null is found for the same interval in any of\nreferenceSeries.  This method compliments the drawNullAsZero function in\ngraphical mode, but also works in text-only mode.\n\nExample:\n\n.. code-block:: none\n\n  &target=transformNull(webapp.pages.*.views,-1)\n\nThis would take any page that didn't have values and supply negative 1 as a default.\nAny other numeric value may be used as well.",
    "function": "transformNull(seriesList, default=0, referenceSeries=None)",
  }
  */
});

SeriesFunction get_query_function(const string& function_name) {
  return functions.at(function_name);
}
