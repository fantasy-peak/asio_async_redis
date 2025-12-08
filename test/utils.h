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
    R"(tcp://127.0.0.1:7000?socket_timeout=50s&connect_timeout=10s&pool_size=10&pool_wait_timeout=1s&pool_connection_lifetime=50s&pool_connection_idle_time=50s)";
inline auto async_redis = std::make_unique<asio_async_redis::Redis<sw::redis::AsyncRedisCluster>>(redis_uri);
inline auto pool = std::make_unique<asio_async_redis::ContextPool>(1);
#else
inline std::string redis_uri =
    R"(tcp://127.0.0.1:6379?socket_timeout=50s&connect_timeout=10s&pool_size=10&pool_wait_timeout=1s&pool_connection_lifetime=50s&pool_connection_idle_time=50s)";
inline auto async_redis = std::make_unique<asio_async_redis::Redis<>>(redis_uri);
inline auto pool = std::make_unique<asio_async_redis::ContextPool>(1);
#endif
