#include "utils.h"

#include <memory>

TEST_CASE("Test Redis class construction")
{
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void>
        {
            sw::redis::EventLoopSPtr loop = asio_async_redis::Redis<>::createEventLoop();
            asio_async_redis::Redis<> redis(redis_uri, pool, loop);
            spdlog::info("start Redis class construction");
            {
                auto ret = co_await async_redis->async_set("classconstruction-construction", "value1");
                REQUIRE(ret.has_value());
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}
