# rrqueue

`rrqueue` is a *distributed task queue* for R, implemented on top of  [Redis](http://redis.io).  At the cost of a little more work it allows for more flexible parallelisation than afforded by `mclapply`.  The main goal is to support non-map style operations: submit some tasks, collect the completed results, queue more even while some tasks are still running.

Other features include:

* Low-level task submission / retrieval has a simple API so that asynchronous task queues can be created.
* Objects representing tasks, workers, queues, etc can be queried.
* While blocking `mclapply`-like functions are available, the package is designed to be non-blocking so that intermediate results can be used.
* Automatic fingerprinting of environments so that code run on a remote machine will correspond to the code found locally.
* Works well connecting to a Redis database running on the cloud (e.g., on an AWS machine over an ssh tunnel).
* Local workers can be added to a remote pool, so long as everything can talk to the same Redis server.
?
* The worker pool can be scaled at any time (up or down).
* Basic fault tolerance, supporting requeuing tasks lost on crashed workers.

# Simple usage

The basic workflow is:

1. Create a queue
2. Submit tasks to the queue
3. Start workers
4. Collect results

The workers can be started at any time between 1-3, though they do need to be started before results can be collected.

## Create a queue

Start a queue that we will submit tasks to
```
con <- rrqueue::queue("jobs")
```

Expressions can be queued using the `enqueue` method:

```
task <- con$enqueue(sin(1))
```

Task objects can be inspected to find out (for example) how long they have been waiting for:

```
task$times()
```

or what their status is:

```
task$status()
```

To get workers to process jobs from this queue, interactively run (in a separate R instance)

```
w <- rrqueue::worker("jobs")
```

or spawn a worker in the background with

```
logfile <- tempfile()
rrqueue::worker_spawn("jobs", logfile)
```

The task will complete:

```
task$status()
```

and the value can be retrieved:

```
task$result()
```

```
con$send_message("STOP")
```

In contrast with many parallel approaches in R, workers can be added at at any time and will automatically start working on any remaining jobs.

There's lots more in various stages of completion, including `mclapply`-like functions (`rrqlapply`), and lots of information gathering.

# Installation

Redis must be installed, `redis-server` must be running.  If you are familiar with docker, the [redis](https://registry.hub.docker.com/_/redis/) docker image might be a good idea here. Alternatively, [download redis](http://redis.io/download), unpack and then install by running `make install` in a terminal window within the downloaded folder.

Once installed start `redis-server` by typing in a terminal window

```
redis-server
```
(On Linux the server will probably be running for you if you. On Mac OSX, you might like to set it up to run as a daemon -- i.e. [background process](https://en.wikipedia.org/wiki/Daemon_(computing)) -- i f you end up using redis at lot, following [these instructions](http://naleid.com/blog/2011/03/05/running-redis-as-a-user-daemon-on-osx-with-launchd))

Try `redis-cli PING` to see if it is running; it should return `PONG` and not give an error.

We'll also need to install some R packages (before), which will require installing the [hiredis](https://github.com/redis/hiredis) library.  See the details on the [https://github.com/richfitz/RedisHeartbeat](RedisHeartbeat) page, but briefly:

* Download the archive from the [https://github.com/redis/hiredis/releases/latest](hiredis release  page)
* install with `tar -zxvf hiredis-0.XX.Y.tar.gz && cd hiredis-0.XX.Y && make && sudo make install`
* On OS/X, add to your `~/.profile`

```
export DYLD_LIBRARY_PATH=/usr/local/lib
```

On debian/ubuntu, install the `libhiredis-dev` package in `apt-get`.

You can then install the required R packages:

```
install.packages(c("RcppRedis", "R6", "digest", "docopt"))
devtools::install_github(c("ropensci/RedisAPI", "richfitz/redux", "richfitz/RedisHeartbeat", "richfitz/ids"))
devtools::install_github("traitecoevo/rrqueue")
```

(*optional*) to see what is going on, in a terminal, run `redis-cli monitor` which will print all the Redis chatter, though it will impact on redis performance.

# Starting workers

Workers can be started from within an R process using `rrqueue::worker_spawn` function.  This takes an optional argument `n` to start more than one worker at a time, and will block until all workers have appeared.

From the command line, workers can be started using the `rrqueue_worker` script.  The script can be installed by running (from R)

```
rrqueue::install_scripts("~/bin")
```

replacing `"~/bin"` with a path that is in your executable search path and which is writeable.

```
$ rrqueue_worker --help
Usage:
  rrqueue_worker [options] <queue_name>
  rrqueue_worker --config=FILENAME [options] [<queue_name>]
  rrqueue_worker -h | --help

Options:
  --redis-host HOSTNAME   Hostname for Redis
  --redis-port PORT       Port for Redis
  --heartbeat-period T    Heartbeat period
  --heartbeat-expire T    Heartbeat expiry time
  --key-worker-alive KEY  Key to write to when the worker becomes alive
  --config FILENAME       Optional YAML configuration filename

  Arguments:
  <queue_name>   Name of queue
```

the arguments correspond to the arguments documented in `?worker_spawn`.  The queue name is determined by position.

The `config` argument is an optional path to a yml configuration file.  That configuration file contains values for any of the arguments to `worker_spawn`, for example:

```yaml
queue_name: tmpjobs
redis_host: 127.0.0.1
redis_port: 6379
heartbeat_period: 30
heartbeat_expire: 90
```

Arguments passed to `rrqueue_worker` in addition to the configuration will override values in the yaml.

This file can also be passed to `queue` and `observer` as the `config` argument (e.g., `queue(config="config.yml")` rather than having to pass in lots of parameters.

# Documentation

Reference documentation and vignettes are available on [this website](http://traitecoevo.github.io/rrqueue/).  If the vignettes are built (`make vignettes`), they will be avilable in the package, and this will be commited to github once things settle down.

# Performance

So far, I've done relatively little performance tuning.  In particular, the *workers* make no effort to minimise the number of calls to Redis and assumes that this is fast connection.  On the other hand, we use `rrqueue` where the controller many hops across the internet (controlling a queue on AWS).  To reduce the time involved, `rrqueue` uses [lua scripting](https://en.wikipedia.org/wiki/Lua_(programming_language)) to reduce the number of instruction round trips.

# False warning errors

You may see a variant on errors like

```
Calls: <Anonymous> -> .handleSimpleError -> h -> signalCondition
Error in signalCondition(e) :
  no function to return from, jumping to top level
```

This is an issue somewhere within Rcpp modules (which RcppRedis uses) and seems harmless.
