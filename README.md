# asio_async_redis

[![Language](https://img.shields.io/badge/Language-C++23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![Build System](https://img.shields.io/badge/Build%20System-xmake-brightgreen.svg)](https://xmake.io)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`asio_async_redis` is a lightweight, high-performance C++ asynchronous Redis client based on Boost.Asio and redis-plus-plus.

## Features

- **Fully Asynchronous**: All operations are non-blocking and leverage C++20/23 coroutines (`co_await`) to simplify asynchronous code.
- **Modern C++**: Written in C++23 standard, resulting in clean and efficient code.

## Dependencies

- C++20 Compiler (GCC, Clang)
- [Boost](https://www.boost.org/)
- [redis-plus-plus](https://github.com/sewenew/redis-plus-plus)

## Build and Run

### 1. Clone the Repository

```bash
git clone https://github.com/fantasy-peak/asio_async_redis.git
cd asio_async_redis
```

### 2. Build the Project

xmake will automatically handle the download and compilation of dependencies.

```bash
xmake build -j 16 -v
```

### 3. Run Tests

The project includes two test targets: one for standalone Redis and another for Redis Cluster.

```bash
# Run standalone mode tests
xmake run test_redis

# Run cluster mode tests
xmake run test_redis_cluster
```

## Usage Example

Below is a simple example demonstrating `SET` and `GET` operations using C++23 coroutines.

```cpp
#include "asio_async_redis.h"
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <asio/use_awaitable.hpp>
#include <iostream>

asio::awaitable<void> do_redis_operations()
{
    std::string redis_uri =
        R"(tcp://127.0.0.1:6379?socket_timeout=50s&connect_timeout=10s&pool_size=10&pool_wait_timeout=1s&pool_connection_lifetime=50s&pool_connection_idle_time=50s)";
    auto async_redis = std::make_unique<asio_async_redis::Redis<sw::redis::AsyncRedis>>(redis_uri);
    // Set a key-value pair
    auto set_ret = co_await async_redis->async_set("my_key", "my_value", asio::use_awaitable);
    if (set_ret.has_value())
    {
        std::cout << "SET successful" << std::endl;
    } else {
        std::cout << "SET failed:" << set_ret.error().message() << std::endl;
    }

    // Get the value of a key
    auto get_ret = co_await async_redis->async_get("my_key", asio::use_awaitable);
    if (get_ret.has_value())
    {
        std::cout << "GET my_key: " << get_ret.value() << std::endl;
    }

    // Delete a key
    auto del_ret = co_await async_redis->async_del("my_key", asio::use_awaitable);
    if (del_ret.has_value())
    {
        std::cout << "DEL my_key count: " << del_ret.value() << std::endl;
    }

    co_return;
}

int main()
{
    // ... Initialize io_context, thread_pool, and redis_client ...

    // Execute the coroutine within the io_context context
    asio::co_spawn(pool->getIoContext(), do_redis_operations, asio::detached);

    // ... Run the io_context ...

    return 0;
}
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.