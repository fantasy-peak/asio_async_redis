#include "utils.h"

#include <algorithm>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <vector>

TEST_CASE("Test redis PubSub")
{
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void>
        {
            std::string channel{"channel"};
            std::string message{"hello world"};
            spdlog::info("start test redis publish");
            {
                auto ret = co_await async_redis->async_publish(channel, message, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 0);
            }
            {
                auto ret = co_await async_redis->async_spublish(channel, message, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 0);
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}
