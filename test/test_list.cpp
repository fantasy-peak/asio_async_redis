#include "utils.h"

#include <initializer_list>
#include <memory>
#include <vector>

TEST_CASE("Test redis list")
{
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void>
        {
            spdlog::info("start test redis list");
            std::string key = "test_list";
            std::vector<std::string> data{"a", "b", "c"};
            {
                co_await async_redis->async_del(key, asio::use_awaitable);
                co_await async_redis->async_lpush(key, data, asio::use_awaitable);
                auto ret = co_await async_redis->async_lrange(key, 0, -1, asio::use_awaitable);
                REQUIRE(ret.has_value());
                std::ranges::reverse(ret.value());
                REQUIRE(ret.value() == data);
            }
            {
                co_await async_redis->async_del(key, asio::use_awaitable);
                co_await async_redis->async_rpush(key, data, asio::use_awaitable);
                auto ret = co_await async_redis->async_lrange(key, 0, -1, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == data);
                auto lret = co_await async_redis->async_lpop(key, asio::use_awaitable);
                REQUIRE(lret.has_value());
                REQUIRE(lret.value().value() == "a");
                auto rret = co_await async_redis->async_rpop(key, asio::use_awaitable);
                REQUIRE(rret.has_value());
                REQUIRE(rret.value().value() == "c");
                auto len_ret = co_await async_redis->async_llen(key, asio::use_awaitable);
                REQUIRE(len_ret.has_value());
                REQUIRE(len_ret.value() == 1);
            }
            {
                data = {"a", "b"};
                {
                    co_await async_redis->async_del(key, asio::use_awaitable);
                    co_await async_redis->async_rpush(key, data, asio::use_awaitable);
                    auto ret = co_await async_redis->async_blpop(key, std::chrono::seconds(1), asio::use_awaitable);
                    REQUIRE(ret.has_value());
                    REQUIRE(std::get<1>(ret.value().value()) == "a");
                }
                {
                    auto ret = co_await async_redis->async_brpop(key, std::chrono::seconds(1), asio::use_awaitable);
                    REQUIRE(ret.has_value());
                    REQUIRE(std::get<1>(ret.value().value()) == "b");
                }
                auto timeout_ret = co_await async_redis->async_blpop(key, std::chrono::seconds(1), asio::use_awaitable);
                REQUIRE(timeout_ret.has_value());
                REQUIRE(!timeout_ret.value().has_value());
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}
