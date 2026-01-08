#include "utils.h"

#include <memory>
#include <vector>

TEST_CASE("Test redis cancel") {
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void> {
            auto sleep = [](auto i) -> asio::awaitable<bool> {
                auto ex = co_await asio::this_coro::executor;

                asio::steady_timer timer{ex};
                timer.expires_after(std::chrono::seconds(i));
                auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
                spdlog::info("sleep done: {}", ec.message());
                co_return true;
            };
            spdlog::info("start test cancel");
            using namespace boost::asio::experimental::awaitable_operators;
            std::string stream_name = "test_stream";
            std::vector<std::string> value{"field1", "value1", "field2", "value2"};
            auto add_ret = co_await async_redis->async_xadd(stream_name, "*", value);
            auto result =
                co_await (sleep(1) ||
                          async_redis->async_xread(stream_name, add_ret.value(), std::chrono::milliseconds(2500), 500));
            REQUIRE(result.index() == 0);
            REQUIRE(std::get<0>(result) == true);
            co_await sleep(2);
            spdlog::info("start test cancel done");
            co_return;
        },
        asio::use_future);
    f.wait();
}
