#include "utils.h"

#include <iostream>
#include <memory>

TEST_CASE("Test redis connection cmd")
{
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void>
        {
            spdlog::info("start test redis connection cmd");
            std::string key = "connection_cmd";
            {
                auto ret = co_await async_redis->async_ping(key, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == key);
            }
            {
                auto ret = co_await async_redis->async_echo(key, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == key);
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}
