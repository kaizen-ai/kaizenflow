# Cache

## How it works

- `Cache` is provided as a decorator function `@hcac.cache` that may be used on any function or regular class method.

- By default, it uses two levels of caching:
  - `Memory` level
  - `File` level

- Whenever a call is being made to the wrapped function
  - firstly `Memory` level is being checked;
  - If there's no hit, `File` level is checked;
  - if there's no hit again, the wrapped function is called.

- The result is then stored in both `File` and `Memory` levels.

- `Cache` is equipped with level tracing via `get_last_cache_accessed` method so
  there is a way to understand if the call hit the cache and on which level.

- `Cache` works also in Python notebooks with `%autoreload`.
- `Cache` traces source code of the wrapped function and tracks its changes
  - For performance reasons, it checks the code only one time unless pointer to
    the function is changed, e.g. in notebooks.

### File level

- `File` level is implemented via [joblib.Memory](https://joblib.readthedocs.io/en/latest/generated/joblib.Memory.html).

### Memory level

- Initially, the idea was to use [functools.lru_cache](https://docs.python.org/3/library/functools.html#functools.lru_cache) for memory cache.

- Pros:
    1. Standard library implementation.
    2. Quietly fast in-memory implementation.
    
- Cons:
    1. Only hashable arguments are supported.
    2. No access to cache -- no ability to check if item is in cache or not.
    3. Does not work properly in notebooks.

- Because Cons outweighed Pros, it was decided to implement `Memory` level as
  [joblib.Memory](https://joblib.readthedocs.io/en/latest/generated/joblib.Memory.html)
  but over [`tmpfs`](https://uk.wikipedia.org/wiki/Tmpfs).
- Basically, reuse the same `File` level cache but over a RAM-based disk. This
  implementation overcomes all listed Cons, albeit it is slightly slower.
