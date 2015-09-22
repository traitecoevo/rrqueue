# rrqueue

`rrqueue` is a *distributed task queue* for R, implemented on top of  [Redis](http://redis.io).  At the cost of a little more work it allows for more flexible parallelisation than afforded by `mclapply`.  The main goal is to support non-map style operations: submit some tasks, collect the completed results, queue more even while some tasks are still running.

Other features include:

* Low-level task submission / retrieval has a simple API so that asynchronous task queues can be created.
* Objects representing tasks, workers, queues, etc can be queried.
* While blocking `mclapply`-like functions are available, the package is designed to be non-blocking so that intermediate results can be used.
* Automatic fingerprinting of environments so that code run on a remote machine will correspond to the code found locally.
* Works well connecting to a Redis database running on the cloud (e.g., on an AWS machine over an ssh tunnel).
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

Redis must be installed, `redis-server` must be running.  If you are familiar with docker, the [redis](https://registry.hub.docker.com/_/redis/) docker image might be a good idea here. Alterantively, [download redis](http://redis.io/download), unpack and then install by running `make install` in a terminal window within the downlaoded folder.

Once installed start `redis-server` by typing in a terminal window

```
redis-server
```

On Linux the server will probably be running for you if you.  Try `redis-server PING` to see if it is running.


R packages:

```
install.packages(c("RcppRedis", "R6", "digest", "docopt"))
devtools::install_github(c("ropensci/RedisAPI", "richfitz/RedisHeartbeat", "richfitz/storr", "richfitz/ids"))
devtools::install_git("https://github.com/traitecoevo/rrqueue")
```

(*optional*) to see what is going on, in a terminal, run `redis-cli monitor` which will print all the Redis chatter, though it will impact on redis performance.

# Performance

So far, I've done relatively little performance tuning.  In particular, the *workers* make no effort to minimise the number of calls to Redis and assumes that this is fast connection.  On the other hand, we use `rrqueue` where the controller many hops across the internet (controlling a queue on AWS).  To reduce the time involved, `rrqueue` uses lua scripting to reduce the number of instruction round trips.
