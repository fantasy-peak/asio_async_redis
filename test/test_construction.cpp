#include "utils.h"

#include <memory>

TEST_CASE("Test Redis class construction") {
    auto client_pool = asio_async_redis::createRedisClientPool<asio_async_redis::Redis<>>(redis_uri, 1, 1);
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void> {
#if 0
            sw::redis::EventLoopSPtr loop = asio_async_redis::Redis<>::createEventLoop();
            asio_async_redis::Redis<> redis(redis_uri, pool, loop);
#endif
            spdlog::info("start Redis class construction");
            {
                auto opt = co_await client_pool->acquire();
                REQUIRE(opt.has_value());
                auto& redis = opt.value()->get();
                auto ret = co_await redis->async_set("classconstruction-construction", "value1");
                REQUIRE(ret.has_value());
                {
                    spdlog::info("start call client_pool->acquire");
                    auto tmp = co_await client_pool->acquire();
                    spdlog::info("start call client_pool->acquire done");
                    REQUIRE(!tmp.has_value());
                }
            }
            {
                auto opt = co_await client_pool->acquire();
                REQUIRE(opt.has_value());
                auto redis = opt.value()->get();
                auto ret = co_await redis->async_set("classconstruction-construction", "value1");
                REQUIRE(ret.has_value());
            }
            spdlog::info("start Redis class construction done");
            co_return;
        },
        asio::use_future);
    f.wait();
}
