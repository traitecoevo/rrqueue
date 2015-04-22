Through the lifecycle of a task:

Assuming a queue name `rrq`, so all keys begin with `rrq:`

## Controller queues a task

1. Given `expr` (some unevaluted R expression) and `envir` (an environment to locate local variables) we do some processing to create something that can be serialised by Redis (see [below](#preparing-the-expression))
2. Increment the *task counter* (`rqr:tasks:counter`) to get the *task id*
3. Store things in hashes with the key as the task id:
   * serialised expression in `rrq::tasks:expr`
   * environment id in `rrq:tasks:envir`
   * task status (pending) in `rrq:tasks:status`
   * time submitted in `rrq:tasks:time:sub`
   * name of the key to push to when the job is complete to `rrq:tasks:complete`
4. Push the task onto the task queue: `rrq:tasks:id`

## Worker accepts a task

1. Pop a task off `rrq:tasks:id`
2. Retrieve the expression
3. Store things in hashes indicating that we're working on the task:
   * Set the worker status as busy (`rrq:workers:status`)
   * Store the task id in `rrq:workers:task`
   * Store the worker id for this task in `rrq:tasks:worker`
   * Store the time the job was begun in `rrq:tasks:times:beg`
   * Set the task status as running in `rrq:tasks:status`
4. Get to work running the task

(note that the task can be lost between steps 1 and 3 though it's easy enough to work out that this is the case because the task is still known to the system but will not be associated with any worker).

## Worker completes the task

1. Store things in hashes:
   * Write the serialized result to hash `rrq:tasks:result`
   * Store task status (complete / error / etc) in `rrq:tasks:status`
   * Store finished time in `rrq:tasks:times:end`
   * Set worker status as idle in `rrq:workers:status`
2. Push the task onto the finished queue - which we look up from `rrq:tasks:complete`
3. Return to polling `rrq:tasks:id` for new jobs

# Preparing the expression

1. Look at all arguments of the expression and work out which entries are symbols
2. For each symbol
   * retrieve the value from the given environment
   * serialise it to Redis using the "object cache" object, using a key which is `<task_id>:<object_name>`.
3. Serialise the expression, plus the list of object names and mapping to the mangled names.

The serialisation to Redis via the object cache uses a content-addressable system where objects are actually stored against keys that are the *hash* of the object contents, and then a pointer is stored from the mangled name to the object.  There's also a counter in there that indicates how many things point at a given piece of data - once that counter drops to zero the object is deleted.  This means that if there are regularly referenced large pieces of data, they are only stored occasionally.

On the recieving end, the workers only pull the objects from the database if they have not pulled an object with that hash before.  This avoids the bottleneck of pulling over the network and deserialising.
