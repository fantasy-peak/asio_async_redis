#include "utils.h"

#include <algorithm>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <vector>

TEST_CASE("Test redis Functions")
{
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void>
        {
            std::string lib_name{"testlib"};
            spdlog::info("start test redis lua Functions");
            {
                std::string code = std::format("#!lua name={}\n", lib_name) +
                                   "redis.register_function('my_func', function(keys, args) "
                                   "redis.call('set', keys[1], 1);"
                                   "redis.call('set', keys[2], 2);"
                                   "local first = redis.call('get', keys[1]);"
                                   "local second = redis.call('get', keys[2]);"
                                   "return first + second\n"
                                   "end)";
                auto ret = co_await async_redis->async_function_load(code, true, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == lib_name);
            }
            {
                std::vector<std::string> keys = {"k1", "k2"};
                std::vector<std::string> empty_list = {};
                auto ret =
                    co_await async_redis->async_fcall<long long>("my_func", keys, empty_list, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 3);
            }
            {
                auto ret = co_await async_redis->async_function_delete(lib_name, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == "OK");
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}

TEST_CASE("Test redis EVAL")
{
    auto f = asio::co_spawn(
        pool->getIoContext(),
        [&] -> asio::awaitable<void>
        {
            std::string script =
                "redis.call('set', KEYS[1], 1);"
                "redis.call('set', KEYS[2], 2);"
                "local first = redis.call('get', KEYS[1]);"
                "local second = redis.call('get', KEYS[2]);"
                "return first + second";
            spdlog::info("start test redis lua EVAL");
            // auto fret = co_await async_redis->async_script_flush(asio::use_awaitable);
            // REQUIRE(fret.has_value());
            // REQUIRE(fret.value() == "OK");
            {
                std::vector<std::string> keys = {"k1", "k2"};
                std::vector<std::string> empty_list = {};
                auto ret = co_await async_redis->async_eval<long long>(script, keys, empty_list, asio::use_awaitable);
                REQUIRE(ret.has_value());
                REQUIRE(ret.value() == 3);
            }
            {
                std::vector<std::string> keys = {"k1", "k2"};
                std::vector<std::string> empty_list = {};
                auto ret = co_await async_redis->async_script_load(script, asio::use_awaitable);
                REQUIRE(ret.has_value());
                spdlog::info("sha1: {}", ret.value());
                auto eret =
                    co_await async_redis->async_evalsha<long long>(ret.value(), keys, empty_list, asio::use_awaitable);
                REQUIRE(eret.has_value());
                REQUIRE(eret.value() == 3);
            }
            co_return;
        },
        asio::use_future);
    f.wait();
}
