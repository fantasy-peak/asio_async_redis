#include "utils.h"

#include <exception>
#include <memory>
#include <vector>

TEST_CASE("Test redis hash") {
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void> {
            spdlog::info("start test redis hash");
            std::string key = "test_hash";
            co_await async_redis->async_del(key);
            {
                auto ret = co_await async_redis->async_hset(key, "field1", "value1");
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 1);
                if (ret.has_value()) {
                    spdlog::info("hset: {}", ret.value());
                }
            }
            {
                std::unordered_map<std::string, std::string> m = {{"field1", "val1"}, {"field2", "val2"}};
                auto ret = co_await async_redis->async_hmset(key, m);
                REQUIRE(ret.has_value());
            }
            {
                auto ret = co_await async_redis->async_hgetall(key);
                REQUIRE(ret.has_value());
            }
            {
                std::vector<std::string> f{"field1", "field2"};
                auto ret = co_await async_redis->async_hmget(key, f);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value()[0].value() == "val1");
            }
            {
                auto ret = co_await async_redis->async_hdel(key, std::vector<std::string>{"field1"});
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 1);
            }
            try {
                auto ret = async_redis->async_hdel(key, std::vector<std::string>{"field2"}, asio::use_future);
                ret.wait();
                auto data = ret.get();
                REQUIRE(data.has_value());
                REQUIRE(data.value() == 1);
            } catch (const std::exception& e) {
                spdlog::error("{}", e.what());
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}
