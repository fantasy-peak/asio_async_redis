#include "utils.h"

#include <chrono>
#include <initializer_list>
#include <memory>
#include <vector>

TEST_CASE("Test redis string") {
    std::string key = "asio_redis";
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void> {
            spdlog::info("start test redis string");
            std::string key = "asio_redis";
            co_await async_redis->async_del(key);
            {
                co_await async_redis->async_del("num");
                auto ret = co_await async_redis->async_set("num", "10");
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == true);
                auto iret = co_await async_redis->async_incr("num");
                REQUIRE(iret.has_value());
                REQUIRE(iret.value() == 11);
                auto bret = co_await async_redis->async_incrby("num", 5);
                REQUIRE(bret.has_value());
                REQUIRE(bret.value() == 16);
            }
            {
                auto ret = co_await async_redis->async_set(key, "test_value");
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == true);
            }
            {
                auto ret = co_await async_redis->async_mget(std::vector<std::string>{key});
                REQUIRE(ret.has_value());
                REQUIRE(ret.value()[0].value() == "test_value");
            }
            {
                auto ret = co_await async_redis->async_get(key);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == "test_value");
            }
            {
                auto ret = co_await async_redis->async_del(key);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 1);
            }
            {
                std::vector<std::string> cmd{"SET", key, "hello"};
                auto ret = co_await async_redis->async_command<std::string>(cmd);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == "OK");
            }
#ifndef TEST_REDIS_CLUSTER
            {
                co_await async_redis->async_set("sc1", "1");
                co_await async_redis->async_set("sc2", "1");
                std::vector<std::string> cmd{"SCAN", "0", "MATCH", "sc*", "COUNT", "1000"};
                auto ret = co_await async_redis->async_command<std::pair<std::string, std::vector<std::string>>>(cmd);
                REQUIRE(ret.has_value());
                auto& [cursor, keys] = ret.value();
                REQUIRE(cursor == "0");
            }
#endif
            {
                auto ret = co_await async_redis->async_append(key, " world");
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 11);
            }
            {
                auto ret = co_await async_redis->async_setex(key, std::chrono::seconds(10), "val");
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == "OK");
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}
