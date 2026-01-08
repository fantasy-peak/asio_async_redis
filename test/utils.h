#pragma once

#include <asio_async_redis.h>
#include <boost/asio.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/scope_exit.hpp>

#include <type_traits>

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>
#include <spdlog/spdlog.h>

#ifdef ASIO_ASYNC_REDIS_USE_BOOST_ASIO
#include <boost/asio.hpp>
namespace asio = boost::asio;
#else
#include <asio.hpp>
#endif

#ifdef TEST_REDIS_CLUSTER
inline std::string redis_uri =
    R"(tcp://127.0.0.1:7000?socket_timeout=30s&connect_timeout=10s&pool_size=10&pool_wait_timeout=0s&pool_connection_lifetime=0s&pool_connection_idle_time=0s)";
inline auto async_redis = std::make_unique<asio_async_redis::Redis<sw::redis::AsyncRedisCluster>>(redis_uri);
inline auto pool = std::make_unique<asio_async_redis::ContextPool>(1);
#else
#if 1
inline std::string redis_uri =
    R"(tcp://127.0.0.1:6379?socket_timeout=30s&connect_timeout=10s&pool_size=10&pool_wait_timeout=0s&pool_connection_lifetime=0s&pool_connection_idle_time=0s)";
inline auto async_redis = std::make_unique<asio_async_redis::Redis<>>(redis_uri);
#else
inline auto create_sentinel_options() {
    sw::redis::SentinelOptions sentinel_opts;
    sentinel_opts.nodes = {
        {"127.0.0.1", 6479},
        {"127.0.0.1", 26379},
    };
    sentinel_opts.connect_timeout = std::chrono::milliseconds(1000);
    sentinel_opts.socket_timeout = std::chrono::milliseconds(10000);
    return sentinel_opts;
}

inline auto sentinel = std::make_shared<sw::redis::AsyncSentinel>(create_sentinel_options());

inline auto create_connection_opts() {
    sw::redis::ConnectionOptions connection_opts;
    connection_opts.connect_timeout = std::chrono::milliseconds(1000);
    connection_opts.socket_timeout = std::chrono::milliseconds(10000);
    return connection_opts;
}

inline auto async_redis = std::make_unique<asio_async_redis::Redis<>>(sentinel,
                                                                      "redis-master",
                                                                      sw::redis::Role::MASTER,
                                                                      create_connection_opts(),
                                                                      sw::redis::ConnectionPoolOptions{},
                                                                      nullptr,
                                                                      1);
#endif
inline auto pool = std::make_shared<asio_async_redis::ContextPool>(1);

#endif
