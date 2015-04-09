# rrqueue

Beyond `mclapply` -- queue-based parallel processing in R, using [Redis](http://redis.io).

# Installation

Redis must be installed, `redis-server` must be running.  The [redis](https://registry.hub.docker.com/_/redis/) docker image might be a good idea here.

R packages:

```
install.packages(c("RcppRedis", "R6", "digest", "docopt"))
devtools::install_github(c("gaborcsardi/crayon", "richfitz/rfiglet", "richfitz/remoji"))
devtools::install_git("https://github.com/richfitz/rrlite", args="--recursive")
devtools::install_git("https://github.com/traitecoevo/rrqueue")
```

(*optional*) to see what is going on, in a terminal, run `redis-cli monitor` which will print all the Redis chatter

Start a queue that we will submit tasks to
```
con <- rrqueue::queue("jobs")
```

In a *separate* R instance, start a worker, pointing at the same queue

```
w <- rrqueue::worker("jobs")
```

(alternatively, run `rrqueue::rrqueue_worker_spawn("jobs", "worker.log")`, but the interface there will change).

Then, on the node with the queue, run

```
id <- con$enqueue(sin(1))
```

which queues the command `sin(1)`.  You'll see this job get picked up immediately by the worker, even though it only appears to poll every few seconds.

Get the status of a job:

```
con$tasks_status(id)
```

Retrieve the results:

```
con$tasks_collect(id)
```

Shut down the worker, either by running `Ctrl-C` in the window with the worker in, or by running

```
con$send_message("STOP")
```

This is all low level stuff for now.  Implementations of things like `mclapply` will be built on top.
