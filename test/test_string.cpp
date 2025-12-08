#include "utils.h"

#include <initializer_list>
#include <memory>
#include <vector>

TEST_CASE("Test redis string")
{
    std::string key = "asio_redis";
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void>
        {
            spdlog::info("start test redis string");
            std::string key = "asio_redis";
            co_await async_redis->async_del(key, asio::use_awaitable);
            {
                co_await async_redis->async_del("num", asio::use_awaitable);
                auto ret = co_await async_redis->async_set("num", "10", asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == true);
                auto iret = co_await async_redis->async_incr("num", asio::use_awaitable);
                REQUIRE(iret.has_value());
                REQUIRE(iret.value() == 11);
                auto tret = co_await async_redis->async_ttl("num", asio::use_awaitable);
                REQUIRE(tret.has_value());
                REQUIRE(tret.value() == -1);
            }
            {
                auto ret = co_await async_redis->async_set(key, "test_value", asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == true);
            }
            {
                auto ret = co_await async_redis->async_mget(std::vector<std::string>{key}, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value()[0].value() == "test_value");
            }
            {
                auto ret = co_await async_redis->async_expire(key, std::chrono::seconds(100), asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == true);
            }
            {
                auto ret = co_await async_redis->async_get(key, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == "test_value");
            }
            {
                auto ret = co_await async_redis->async_exists(std::vector<std::string>{key}, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 1);
            }
            {
                auto ret = co_await async_redis->async_del(key, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 1);
            }
            {
                auto ret = co_await async_redis->async_command<std::string>(
                    std::vector<std::string>{"SET", "hello", "world"}, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == "OK");
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}
