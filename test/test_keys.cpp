#include "utils.h"

#include <algorithm>
#include <memory>
#include <string_view>
#include <vector>

TEST_CASE("Test redis keys") {
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void> {
            spdlog::info("start test redis keys");
            std::string key1 = "test_keys1";
            std::string key2 = "test_keys2";
            co_await async_redis->async_del(key1);
            co_await async_redis->async_del(key2);
            co_await async_redis->async_set(key1, "test_value1");
            co_await async_redis->async_set(key2, "test_value2");
            {
                auto ret = co_await async_redis->async_exists(std::vector<std::string>{key1});
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 1);
            }
            {
                auto ret = co_await async_redis->async_expire(key2, std::chrono::seconds(100));
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == true);
            }
            {
                auto tret = co_await async_redis->async_ttl(key2);
                REQUIRE(tret.has_value());
                REQUIRE(tret.value() != -1);
            }
            {
                auto ret = co_await async_redis->async_persist(key2);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == true);
            }
            {
                auto tret = co_await async_redis->async_ttl(key2);
                REQUIRE(tret.has_value());
                REQUIRE(tret.value() == -1);
            }
#ifndef TEST_REDIS_CLUSTER
            {
                auto ret = co_await async_redis->async_keys("test_keys*");
                REQUIRE(ret.has_value());
                REQUIRE(ret.value().size() == 2);
                std::ranges::sort(ret.value());
                REQUIRE(ret.value()[0] == key1);
                REQUIRE(ret.value()[1] == key2);
            }
#endif
            co_return;
        },
        asio::use_future);
    f.wait();
}
