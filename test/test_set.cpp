#include "utils.h"

#include <algorithm>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <vector>

TEST_CASE("Test redis set")
{
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void>
        {
            spdlog::info("start test redis set");
            std::string key = "test_set";
            co_await async_redis->async_del(key);
            std::vector<std::string> data{"a", "b", "c"};
            std::ranges::sort(data);
            {
                auto ret = co_await async_redis->async_sadd(key, data);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 3);
            }
            {
                auto ret = co_await async_redis->async_scard(key);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 3);
            }
            {
                auto ret = co_await async_redis->async_smembers(key);
                REQUIRE(ret.has_value());
                std::ranges::sort(ret.value());
                REQUIRE(ret.value() == data);
            }
            {
                auto ret = co_await async_redis->async_sismember(key, "a");
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 1);
            }
            {
                auto ret = co_await async_redis->async_spop(key, std::nullopt);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value().size() == 1);
            }
            {
                // for redis cluster, need keys hash to the same slot
                std::vector<std::string> keys{"a1"};
                auto ret = co_await async_redis->async_del(keys);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 0);
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}

TEST_CASE("Test redis sort set")
{
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void>
        {
            spdlog::info("start test redis sort set");
            std::string sorted_set_key = "sorted_set_1";
            {
                co_await async_redis->async_del(sorted_set_key);
                auto ret = co_await async_redis->async_zadd(sorted_set_key, "a", 99, sw::redis::UpdateType::ALWAYS,
                                                            true);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 1);
            }
            {
                co_await async_redis->async_del(sorted_set_key);
                std::unordered_map<std::string, double> scores = {{"m2", 2.3}, {"m3", 4.5}};
                auto ret = co_await async_redis->async_zadd(sorted_set_key, scores, sw::redis::UpdateType::ALWAYS, true,
                                                            asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 2);
            }
            {
                int offset = 0;
                int count = 100;
                auto ret = co_await async_redis->async_zrangebyscore(sorted_set_key, 0, 5, true, offset, count,
                                                                     asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value().size() == 2);
            }
            {
                int offset = -1;
                int count = -1;
                auto ret = co_await async_redis->async_zrangebyscore<std::vector<std::string>>(
                    sorted_set_key, 0, 5, false, offset, count);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value().size() == 2);
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}
