#include "utils.h"

#include <spdlog/spdlog.h>

#include <chrono>
#include <memory>

TEST_CASE("Test redis stream")
{
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void>
        {
            std::string stream_name = "test_stream";
            std::vector<std::string> value{"field1", "value1", "field2", "value2"};
            spdlog::info("start test redis stream");
            co_await async_redis->async_del(stream_name);
            std::string id;
            {
                auto add_ret = co_await async_redis->async_xadd(stream_name, "*", value);
                REQUIRE(add_ret.has_value());
                id = add_ret.value();
            }
            {
                auto ret = co_await async_redis->async_xadd(stream_name, "*", value);
                auto data = co_await async_redis->async_xrange(stream_name, "-", "+", std::make_optional(1),
                                                               asio::use_awaitable);
                REQUIRE(data.value().size() == 1);
            }
            {
                auto ret = co_await async_redis->async_xread(stream_name, id, std::chrono::milliseconds(5000), 100,
                                                             asio::use_awaitable);
                REQUIRE(ret.has_value());
            }
            {
                auto ret = co_await async_redis->async_xadd(stream_name, "*", value);
                REQUIRE(ret.has_value());
                auto now = std::chrono::system_clock::now();
                auto xret =
                    co_await async_redis->async_xread(stream_name, ret.value(), std::chrono::milliseconds(2000), 100);
                REQUIRE(!xret.has_value());
                REQUIRE(std::chrono::system_clock::now() - now >= std::chrono::milliseconds(2000));
                if (!xret.has_value())
                {
                    spdlog::error("{}, {}", xret.error().message(),
                                  asio_async_redis::to_string(xret.error().error_code()));
                }
            }
            {
                co_await async_redis->async_del(stream_name);
                co_await async_redis->async_xadd(stream_name, "*", value);
                auto ret = co_await async_redis->async_xgroup_create(stream_name, "mygroup", "0", true);
                REQUIRE(ret.has_value());
                bool noack = false;
                // XPENDING test_stream mygroup query not ack message
                auto xret = co_await async_redis->async_xreadgroup(
                    "mygroup", "consumer", std::chrono::milliseconds(5000), stream_name, ">", 1, noack);
                REQUIRE(xret.has_value());
                for (auto& [stream, item_stream] : xret.value())
                {
                    for (auto& [id, data] : item_stream)
                    {
                        spdlog::info("ack id: {}", id);
                        auto xret =
                            co_await async_redis->async_xack(stream_name, "mygroup", std::vector<std::string>{id});
                        REQUIRE(xret.has_value());
                        REQUIRE(xret.value() == 1);
                    }
                }
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}