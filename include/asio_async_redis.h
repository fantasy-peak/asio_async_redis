/**
 *  Copyright 2025, fantasy-peak.  All rights reserved.
 *  https://github.com/fantasy-peak/asio_async_redis.git
 *  Use of this source code is governed by a MIT license
 *  that can be found in the License file.
 *
 *  asio_async_redis
 *
 */

#pragma once

#include <sw/redis++/async_redis.h>
#include <sw/redis++/async_redis_cluster.h>
#include <sw/redis++/async_sentinel.h>
#include <sw/redis++/errors.h>
#include <sw/redis++/event_loop.h>
#include <sw/redis++/redis_uri.h>

#include <asio_async_redis_utils.h>

#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace asio_async_redis
{
template <typename REDIS = sw::redis::AsyncRedis>
class Redis final
{
  public:
    Redis(const std::string& uri, int32_t size = 1)
        : m_redis(std::make_unique<REDIS>(uri)), m_pool(std::make_shared<ContextPool>(size)), m_stop_pool(true)
    {
    }
    Redis(const std::string& uri, std::shared_ptr<ContextPool> ptr)
        : m_redis(std::make_unique<REDIS>(uri)), m_pool(std::move(ptr)), m_stop_pool(false)
    {
    }
    Redis(const sw::redis::ConnectionOptions& opts, const sw::redis::ConnectionPoolOptions& pool_opts = {},
          const sw::redis::EventLoopSPtr& loop = nullptr, int32_t size = 1)
        : m_redis(std::make_unique<REDIS>(opts, pool_opts, loop)),
          m_pool(std::make_shared<ContextPool>(size)),
          m_stop_pool(true)
    {
    }
    Redis(std::shared_ptr<ContextPool> ptr, const sw::redis::ConnectionOptions& opts,
          const sw::redis::ConnectionPoolOptions& pool_opts = {}, const sw::redis::EventLoopSPtr& loop = nullptr)
        : m_redis(std::make_unique<REDIS>(opts, pool_opts, loop)), m_pool(std::move(ptr)), m_stop_pool(false)
    {
    }
    Redis(const std::string& uri, std::shared_ptr<ContextPool> ptr, sw::redis::EventLoopSPtr loop)
        : m_redis(
              [&]
              {
                  sw::redis::Uri uri_obj(uri);
                  return std::make_unique<REDIS>(uri_obj.connection_options(), uri_obj.connection_pool_options(), loop);
              }()),
          m_pool(std::move(ptr)),
          m_stop_pool(false)
    {
    }
    Redis(const std::shared_ptr<sw::redis::AsyncSentinel>& sentinel, const std::string& master_name,
          sw::redis::Role role, const sw::redis::ConnectionOptions& connection_opts,
          const sw::redis::ConnectionPoolOptions& pool_opts = {}, const sw::redis::EventLoopSPtr& loop = nullptr,
          int32_t size = 1)
        : m_redis(std::make_unique<REDIS>(sentinel, master_name, role, connection_opts, pool_opts, loop)),
          m_pool(std::make_shared<ContextPool>(size)),
          m_stop_pool(true)
    {
    }

    ~Redis() { stop(); }

    Redis(const Redis&) = delete;
    Redis& operator=(const Redis&) = delete;
    Redis(Redis&&) = delete;
    Redis& operator=(Redis&&) = delete;

    void start()
    {
        if (m_stop_pool)
        {
            m_pool->start();
        }
    }

    void stop()
    {
        if (m_stop) return;
        m_stop = true;
        if (m_stop_pool)
        {
            m_pool->stop();
        }
    }

    // connection
    template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_echo(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_ping(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_ping(CompletionToken&& token = CompletionToken{});

    // string
    template <asio::completion_token_for<void(Expected<std::optional<std::string>>)> CompletionToken =
                  asio::use_awaitable_t<>>
    auto async_get(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<bool>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_set(std::string_view key, std::string_view val, CompletionToken&& token = CompletionToken{});
    // The return value may become a boolean in the future.
    template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_setex(std::string_view key, const std::chrono::seconds& ttl, std::string_view val,
                     CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_del(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <StringSequence Input,
              asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_del(Input&& input, CompletionToken&& token = CompletionToken{});
    template <StringSequence Input,
              asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_exists(Input&& input, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<bool>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_expire(std::string_view key, const std::chrono::seconds& timeout,
                      CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_incr(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_incrby(std::string_view key, long long increment, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_ttl(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <StringSequence Input, asio::completion_token_for<void(Expected<std::vector<std::optional<std::string>>>)>
                                        CompletionToken = asio::use_awaitable_t<>>
    auto async_mget(Input&& input, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_append(std::string_view key, std::string_view str, CompletionToken&& token = CompletionToken{});

    // hash
    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_hset(std::string_view key, std::string_view field, std::string_view val,
                    CompletionToken&& token = CompletionToken{});
    template <StringKVContainer Input,
              asio::completion_token_for<void(Expected<void>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_hmset(std::string_view key, const Input& m, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::unordered_map<std::string, std::string>>)> CompletionToken =
                  asio::use_awaitable_t<>>
    auto async_hgetall(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <StringSequence Input, asio::completion_token_for<void(Expected<std::vector<std::optional<std::string>>>)>
                                        CompletionToken = asio::use_awaitable_t<>>
    auto async_hmget(std::string_view key, Input&& input, CompletionToken&& token = CompletionToken{});
    template <StringSequence Input,
              asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_hdel(std::string_view key, Input&& input, CompletionToken&& token = CompletionToken{});

    // stream
    template <StringSequence Input,
              asio::completion_token_for<void(Expected<std::string>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_xadd(std::string_view key, std::string_view id, Input&& input,
                    CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::unordered_map<std::string, ItemStream>>)> CompletionToken =
                  asio::use_awaitable_t<>>
    auto async_xread(std::string_view key, std::string_view id, const std::optional<std::chrono::milliseconds>& timeout,
                     long long count, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<ItemStream>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_xrange(std::string_view key, std::string_view start, std::string_view end,
                      const std::optional<long long>& count, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<void>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_xgroup_create(std::string_view key, std::string_view group, std::string_view id, bool mkstream,
                             CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::unordered_map<std::string, ItemStream>>)> CompletionToken =
                  asio::use_awaitable_t<>>
    auto async_xreadgroup(std::string_view group, std::string_view consumer,
                          std::optional<std::chrono::milliseconds> block, std::string_view key, std::string_view id,
                          long long count, bool noack, CompletionToken&& token = CompletionToken{});
    template <StringSequence Input,
              asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_xack(std::string_view key, std::string_view group, Input&& input,
                    CompletionToken&& token = CompletionToken{});

    // list
    template <StringSequence Input,
              asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_lpush(std::string_view key, Input&& input, CompletionToken&& token = CompletionToken{});
    template <StringSequence Input,
              asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_rpush(std::string_view key, Input&& input, CompletionToken&& token = CompletionToken{});
    template <
        asio::completion_token_for<void(Expected<std::vector<std::string>>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_lrange(std::string_view key, long long start, long long stop,
                      CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::optional<std::string>>)> CompletionToken =
                  asio::use_awaitable_t<>>
    auto async_lpop(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::optional<std::string>>)> CompletionToken =
                  asio::use_awaitable_t<>>
    auto async_rpop(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_llen(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::optional<std::pair<std::string, std::string>>>)>
                  CompletionToken = asio::use_awaitable_t<>>
    auto async_blpop(std::string_view key, const std::chrono::seconds& timeout,
                     CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::optional<std::pair<std::string, std::string>>>)>
                  CompletionToken = asio::use_awaitable_t<>>
    auto async_brpop(std::string_view key, const std::chrono::seconds& timeout,
                     CompletionToken&& token = CompletionToken{});

    // SET commands
    template <StringSequence Input,
              asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_sadd(std::string_view key, Input&& input, CompletionToken&& token = CompletionToken{});
    template <
        asio::completion_token_for<void(Expected<std::vector<std::string>>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_smembers(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_sismember(std::string_view key, std::string_view member, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_scard(std::string_view key, CompletionToken&& token = CompletionToken{});
    template <typename Result = std::set<std::string>,
              asio::completion_token_for<void(Expected<Result>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_spop(std::string_view key, const std::optional<long long>& count,
                    CompletionToken&& token = CompletionToken{});

    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_zadd(std::string_view key, std::string_view member, double score, sw::redis::UpdateType type,
                    bool changed, CompletionToken&& token = CompletionToken{});
    template <typename Input,
              asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_zadd(std::string_view key, Input&& input, sw::redis::UpdateType type, bool changed,
                    CompletionToken&& token = CompletionToken{});
    template <typename Result = std::vector<std::pair<std::string, double>>,
              asio::completion_token_for<void(Expected<Result>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_zrangebyscore(std::string_view key, double min, double max, bool withscores, long long offset,
                             long long count, CompletionToken&& token = CompletionToken{});

    // lua
    template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_function_load(std::string_view code, bool replace, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_function_delete(std::string_view lib_name, CompletionToken&& token = CompletionToken{});
    template <typename Result, StringSequence Keys, StringSequence Args,
              asio::completion_token_for<void(Expected<Result>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_fcall(std::string_view func, Keys&& keys, Args&& args, CompletionToken&& token = CompletionToken{});
    template <typename Result, StringSequence Keys, StringSequence Args,
              asio::completion_token_for<void(Expected<Result>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_eval(std::string_view script, Keys&& keys, Args&& args, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_script_load(std::string_view script, CompletionToken&& token = CompletionToken{});
    template <typename Result, StringSequence Keys, StringSequence Args,
              asio::completion_token_for<void(Expected<Result>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_evalsha(std::string_view script, Keys&& keys, Args&& args, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_script_flush(CompletionToken&& token = CompletionToken{});

    template <typename RET, StringSequence Input,
              asio::completion_token_for<void(Expected<RET>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_command(Input&& input, CompletionToken&& token = CompletionToken{});

    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_publish(std::string_view channel, std::string_view message, CompletionToken&& token = CompletionToken{});
    template <asio::completion_token_for<void(Expected<long long>)> CompletionToken = asio::use_awaitable_t<>>
    auto async_spublish(std::string_view channel, std::string_view message,
                        CompletionToken&& token = CompletionToken{});

    auto& ref() { return m_redis; }
    auto& pool() { return m_pool; }
    static auto createEventLoop() { return std::make_shared<sw::redis::EventLoop>(); }

  private:
    template <typename H>
    auto register_slot(H&& handler);
    template <typename R, typename H, typename F>
    void process(asio::io_context* io_context, std::shared_ptr<bool> cancelled, H&& h, F&& fut);
    template <typename RET, typename Input, typename Handler>
    void call_command(const Input& cmd, Handler&& handler);

    std::unique_ptr<REDIS> m_redis;
    std::shared_ptr<ContextPool> m_pool;
    bool m_stop_pool;
    bool m_stop{false};
};

template <typename REDIS>
template <typename RET, typename Input, typename Handler>
inline void Redis<REDIS>::call_command(const Input& cmd, Handler&& handler)
{
    auto [h, io_context, cancelled] = register_slot(std::forward<Handler>(handler));
    try
    {
        m_redis->template command<RET>(cmd.begin(), cmd.end(),
                                       [io_context, h, cancelled, this](std::future<RET>&& fut) mutable
                                       {
                                           process<RET>(io_context, std::move(cancelled), std::move(h), std::move(fut));
                                       });
    }
    catch (const std::exception& err)
    {
        Expected<RET> result =
            UnExpected(RedisError{std::string{"call_command error:"} + err.what(), RedisError::ErrorCode::Error});
        asio::post(*io_context,
                   [this, cancelled, h, result = std::move(result)] mutable
                   {
                       if (*cancelled)
                       {
                           return;
                       }
                       *cancelled = true;
                       auto ex = asio::get_associated_executor(*h);
                       asio::post(ex,
                                  [h = std::move(h), ret = std::move(result)] mutable
                                  {
                                      std::move (*h)(std::move(ret));
                                  });
                   });
    }
    return;
}

template <typename REDIS>
template <typename R, typename H, typename F>
inline void Redis<REDIS>::process(asio::io_context* io_context, std::shared_ptr<bool> cancelled, H&& h, F&& fut)
{
    asio::post(*io_context,
               [this, cancelled = std::move(cancelled), h = std::forward<H>(h), fut = std::forward<F>(fut)] mutable
               {
                   if (*cancelled)
                   {
                       return;
                   }
                   *cancelled = true;
                   Expected<R> result;
                   try
                   {
                       if constexpr (!std::is_void_v<R>)
                       {
                           if constexpr (std::is_arithmetic_v<R>)
                           {
                               result = fut.get();
                           }
                           else
                           {
                               result = std::move(fut.get());
                           }
                       }
                   }
                   catch (const sw::redis::TimeoutError& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::TimeoutError});
                   }
                   catch (const sw::redis::IoError& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::IoError});
                   }
                   catch (const sw::redis::ClosedError& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::ClosedError});
                   }
                   catch (const sw::redis::ProtoError& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::ProtoError});
                   }
                   catch (const sw::redis::OomError& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::OomError});
                   }
                   catch (const sw::redis::WatchError& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::WatchError});
                   }
                   catch (const sw::redis::MovedError& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::MovedError});
                   }
                   catch (const sw::redis::AskError& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::AskError});
                   }
                   catch (const sw::redis::ReplyError& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::ReplyError});
                   }
                   catch (const sw::redis::Error& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::Error});
                   }
                   catch (const std::exception& err)
                   {
                       result = UnExpected(RedisError{err.what(), RedisError::ErrorCode::UnknownError});
                   }
                   auto ex = asio::get_associated_executor(*h);
                   asio::post(ex,
                              [h = std::move(h), ret = std::move(result)] mutable
                              {
                                  std::move (*h)(std::move(ret));
                              });
               });
}

template <typename REDIS>
template <typename H>
inline auto Redis<REDIS>::register_slot(H&& handler)
{
    auto h = std::make_shared<H>(std::forward<H>(handler));
    auto cancelled = std::make_shared<bool>(false);
    auto io_context = m_pool->getIoContextRawPtr();
    auto slot = asio::get_associated_cancellation_slot(*h);
    if (slot.is_connected())
    {
        slot.assign(
            [cancelled, h, io_context](boost::asio::cancellation_type_t& /*type*/) mutable
            {
                asio::post(*io_context,
                           [cancelled = std::move(cancelled), h = std::move(h)] mutable
                           {
                               if (*cancelled)
                               {
                                   return;
                               }
                               *cancelled = true;
                               auto ex = asio::get_associated_executor(*h);
                               asio::post(
                                   ex,
                                   [h = std::move(h)]
                                   {
                                       std::move (*h)(UnExpected(RedisError{"cancel", RedisError::ErrorCode::Cancel}));
                                   });
                           });
            });
    }
    return std::make_tuple(std::move(h), io_context, std::move(cancelled));
}

template <typename REDIS>
template <StringSequence Input, asio::completion_token_for<void(Expected<std::string>)> CompletionToken>
inline auto Redis<REDIS>::async_xadd(std::string_view key, std::string_view id, Input&& input, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::string>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto id, auto&& input) mutable
        {
            std::vector<std::string_view> cmd{"XADD", key, id};
            cmd.insert(cmd.end(), input.begin(), input.end());
            using RET = std::string;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, id, std::forward<Input>(input));
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::unordered_map<std::string, ItemStream>>)> CompletionToken>
inline auto Redis<REDIS>::async_xread(std::string_view key, std::string_view id,
                                      const std::optional<std::chrono::milliseconds>& timeout, long long count,
                                      CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::unordered_map<std::string, ItemStream>>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto id, auto timeout, auto count) mutable
        {
            auto count_str = std::to_string(count);
            std::vector<std::string_view> cmd{
                "XREAD",
                "COUNT",
                count_str,
            };
            std::string timeout_str;
            if (timeout.has_value())
            {
                timeout_str = std::to_string(timeout.value().count());
                cmd.emplace_back("BLOCK");
                cmd.emplace_back(timeout_str);
            }
            cmd.emplace_back("STREAMS");
            cmd.emplace_back(key);
            cmd.emplace_back(id);
            using RET = std::unordered_map<std::string, ItemStream>;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, id, timeout, count);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<ItemStream>)> CompletionToken>
inline auto Redis<REDIS>::async_xrange(std::string_view key, std::string_view start, std::string_view end,
                                       const std::optional<long long>& count, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<ItemStream>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto start, auto end, auto count) mutable
        {
            std::vector<std::string_view> cmd{"XRANGE", key, start, end};
            std::string count_str;
            if (count.has_value())
            {
                count_str = std::to_string(count.value());
                cmd.emplace_back("COUNT");
                cmd.emplace_back(count_str);
            }
            using RET = ItemStream;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, start, end, count);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::optional<std::string>>)> CompletionToken>
inline auto Redis<REDIS>::async_get(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::optional<std::string>>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            using RET = std::optional<std::string>;
            auto [h, io_context, cancelled] = register_slot(std::forward<Handler>(handler));
            m_redis->get(
                key,
                [io_context, h = std::move(h), cancelled = std::move(cancelled), this](std::future<RET>&& fut) mutable
                {
                    process<RET>(io_context, std::move(cancelled), std::move(h), std::move(fut));
                });
        },
        token, key);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<bool>)> CompletionToken>
inline auto Redis<REDIS>::async_set(std::string_view key, std::string_view val, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<bool>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto val) mutable
        {
            using RET = bool;
            auto [h, io_context, cancelled] = register_slot(std::forward<Handler>(handler));
            m_redis->set(
                key, val,
                [io_context, h = std::move(h), cancelled = std::move(cancelled), this](std::future<RET>&& fut) mutable
                {
                    process<RET>(io_context, std::move(cancelled), std::move(h), std::move(fut));
                });
        },
        token, key, val);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken>
inline auto Redis<REDIS>::async_setex(std::string_view key, const std::chrono::seconds& ttl, std::string_view val,
                                      CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::string>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto val, auto ttl) mutable
        {
            using RET = std::string;
            auto ttl_str = std::to_string(ttl.count());
            std::initializer_list<std::string_view> cmd{"SETEX", key, ttl_str, val};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, val, ttl);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_del(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            using RET = long long;
            auto [h, io_context, cancelled] = register_slot(std::forward<Handler>(handler));
            m_redis->del(
                key,
                [io_context, h = std::move(h), cancelled = std::move(cancelled), this](std::future<RET>&& fut) mutable
                {
                    process<long long>(io_context, std::move(cancelled), std::move(h), std::move(fut));
                });
        },
        token, key);
}

template <typename REDIS>
template <StringSequence Input, asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_del(Input&& input, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto&& input) mutable
        {
            using RET = long long;
            auto [h, io_context, cancelled] = register_slot(std::forward<Handler>(handler));
            m_redis->del(
                input.begin(), input.end(),
                [io_context, h = std::move(h), cancelled = std::move(cancelled), this](std::future<RET>&& fut) mutable
                {
                    process<long long>(io_context, std::move(cancelled), std::move(h), std::move(fut));
                });
        },
        token, std::forward<Input>(input));
}

template <typename REDIS>
template <StringSequence Input, asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_exists(Input&& input, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto&& input) mutable
        {
            std::vector<std::string_view> cmd{"exists"};
            cmd.insert(cmd.end(), input.begin(), input.end());
            using RET = long long;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, std::forward<Input>(input));
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<bool>)> CompletionToken>
inline auto Redis<REDIS>::async_expire(std::string_view key, const std::chrono::seconds& timeout,
                                       CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<bool>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto timeout) mutable
        {
            using RET = bool;
            auto coutnt_str = std::to_string(timeout.count());
            std::initializer_list<std::string_view> cmd{"EXPIRE", key, coutnt_str};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, timeout);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_hset(std::string_view key, std::string_view field, std::string_view val,
                                     CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto field, auto val) mutable
        {
            using RET = long long;
            auto [h, io_context, cancelled] = register_slot(std::forward<Handler>(handler));
            m_redis->hset(
                key, field, val,
                [io_context, h = std::move(h), cancelled = std::move(cancelled), this](std::future<RET>&& fut) mutable
                {
                    process<RET>(io_context, std::move(cancelled), std::move(h), std::move(fut));
                });
        },
        token, key, field, val);
}

template <typename REDIS>
template <StringKVContainer Input, asio::completion_token_for<void(Expected<void>)> CompletionToken>
inline auto Redis<REDIS>::async_hmset(std::string_view key, const Input& m, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<void>)>(
        [this]<typename Handler>(Handler&& handler, auto key, const auto& m) mutable
        {
            std::vector<std::string_view> cmd{"HMSET", key};
            for (const auto& [k, v] : m)
            {
                cmd.emplace_back(k);
                cmd.emplace_back(v);
            }
            using RET = void;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, m);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::unordered_map<std::string, std::string>>)> CompletionToken>
inline auto Redis<REDIS>::async_hgetall(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::unordered_map<std::string, std::string>>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            using RET = std::unordered_map<std::string, std::string>;
            auto [h, io_context, cancelled] = register_slot(std::forward<Handler>(handler));
            m_redis->template hgetall<RET>(
                key,
                [io_context, h = std::move(h), cancelled = std::move(cancelled), this](std::future<RET>&& fut) mutable
                {
                    process<RET>(io_context, std::move(cancelled), std::move(h), std::move(fut));
                });
        },
        token, key);
}

template <typename REDIS>
template <StringSequence Input,
          asio::completion_token_for<void(Expected<std::vector<std::optional<std::string>>>)> CompletionToken>
inline auto Redis<REDIS>::async_hmget(std::string_view key, Input&& input, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::vector<std::optional<std::string>>>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto&& input) mutable
        {
            using RET = std::vector<std::optional<std::string>>;
            std::vector<std::string_view> cmd{"HMGET", key};
            cmd.insert(cmd.end(), input.begin(), input.end());
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, std::forward<Input>(input));
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_incr(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            using RET = long long;
            std::initializer_list<std::string_view> cmd{"INCR", key};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_incrby(std::string_view key, long long increment, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto increment) mutable
        {
            using RET = long long;
            auto str = std::to_string(increment);
            std::initializer_list<std::string_view> cmd{"INCRBY", key, str};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, increment);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_ttl(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            using RET = long long;
            std::initializer_list<std::string_view> cmd{"TTL", key};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key);
}

template <typename REDIS>
template <StringSequence Input,
          asio::completion_token_for<void(Expected<std::vector<std::optional<std::string>>>)> CompletionToken>
inline auto Redis<REDIS>::async_mget(Input&& input, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::vector<std::optional<std::string>>>)>(
        [this]<typename Handler>(Handler&& handler, auto&& input) mutable
        {
            using RET = std::vector<std::optional<std::string>>;
            std::vector<std::string_view> cmd{"MGET"};
            cmd.insert(cmd.end(), input.begin(), input.end());
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, std::forward<Input>(input));
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_append(std::string_view key, std::string_view str, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto str) mutable
        {
            using RET = long long;
            std::initializer_list<std::string_view> cmd{"APPEND", key, str};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, str);
}

template <typename REDIS>
template <StringSequence Input, asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_hdel(std::string_view key, Input&& input, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto&& input) mutable
        {
            using RET = long long;
            std::vector<std::string_view> cmd{"HDEL", key};
            cmd.insert(cmd.end(), input.begin(), input.end());
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, std::forward<Input>(input));
}

template <typename REDIS>
template <typename RET, StringSequence Input, asio::completion_token_for<void(Expected<RET>)> CompletionToken>
inline auto Redis<REDIS>::async_command(Input&& input, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<RET>)>(
        [this]<typename Handler>(Handler&& handler, auto&& input) mutable
        {
            this->call_command<RET>(input, std::forward<Handler>(handler));
        },
        token, std::forward<Input>(input));
}

template <typename REDIS>
template <StringSequence Input, asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_lpush(std::string_view key, Input&& input, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto&& input) mutable
        {
            using RET = long long;
            std::vector<std::string_view> cmd{"LPUSH", key};
            cmd.insert(cmd.end(), input.begin(), input.end());
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, std::forward<Input>(input));
}

template <typename REDIS>
template <StringSequence Input, asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_rpush(std::string_view key, Input&& input, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto&& input) mutable
        {
            using RET = long long;
            std::vector<std::string_view> cmd{"RPUSH", key};
            cmd.insert(cmd.end(), input.begin(), input.end());
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, std::forward<Input>(input));
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::vector<std::string>>)> CompletionToken>
inline auto Redis<REDIS>::async_lrange(std::string_view key, long long start, long long stop, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::vector<std::string>>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto start, auto stop) mutable
        {
            auto start_str = std::to_string(start);
            auto stop_str = std::to_string(stop);
            std::initializer_list<std::string_view> cmd{"LRANGE", key, start_str, stop_str};
            using RET = std::vector<std::string>;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, start, stop);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::optional<std::string>>)> CompletionToken>
inline auto Redis<REDIS>::async_lpop(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::optional<std::string>>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            std::initializer_list<std::string_view> cmd{"LPOP", key};
            using RET = std::optional<std::string>;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key);
}

template <typename REDIS>
template <
    asio::completion_token_for<void(Expected<std::optional<std::pair<std::string, std::string>>>)> CompletionToken>
inline auto Redis<REDIS>::async_blpop(std::string_view key, const std::chrono::seconds& timeout,
                                      CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::optional<std::pair<std::string, std::string>>>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto timeout) mutable
        {
            auto timeout_str = std::to_string(timeout.count());
            std::initializer_list<std::string_view> cmd{"BLPOP", key, timeout_str};
            using RET = std::optional<std::pair<std::string, std::string>>;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, timeout);
}

template <typename REDIS>
template <
    asio::completion_token_for<void(Expected<std::optional<std::pair<std::string, std::string>>>)> CompletionToken>
inline auto Redis<REDIS>::async_brpop(std::string_view key, const std::chrono::seconds& timeout,
                                      CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::optional<std::pair<std::string, std::string>>>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto timeout) mutable
        {
            auto timeout_str = std::to_string(timeout.count());
            std::initializer_list<std::string_view> cmd{"BRPOP", key, timeout_str};
            using RET = std::optional<std::pair<std::string, std::string>>;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, timeout);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::optional<std::string>>)> CompletionToken>
inline auto Redis<REDIS>::async_rpop(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::optional<std::string>>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            std::initializer_list<std::string_view> cmd{"RPOP", key};
            using RET = std::optional<std::string>;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_llen(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            std::initializer_list<std::string_view> cmd{"LLEN", key};
            using RET = long long;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<void>)> CompletionToken>
inline auto Redis<REDIS>::async_xgroup_create(std::string_view key, std::string_view group, std::string_view id,
                                              bool mkstream, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<void>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto group, auto id, auto mkstream) mutable
        {
            std::vector<std::string_view> cmd{"XGROUP", "CREATE", key, group, id};
            if (mkstream)
            {
                cmd.emplace_back("MKSTREAM");
            };
            using RET = void;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, group, id, mkstream);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::unordered_map<std::string, ItemStream>>)> CompletionToken>
inline auto Redis<REDIS>::async_xreadgroup(std::string_view group, std::string_view consumer,
                                           std::optional<std::chrono::milliseconds> block, std::string_view key,
                                           std::string_view id, long long count, bool noack, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::unordered_map<std::string, ItemStream>>)>(
        [this]<typename Handler>(Handler&& handler, auto group, auto consumer, auto block, auto key, auto id,
                                 auto count, auto noack) mutable
        {
            // XREADGROUP GROUP mygroup consumer1 COUNT 100 BLOCK 50 NOACK STREAMS mystream >
            auto count_str = std::to_string(count);
            std::string block_str;
            std::vector<std::string_view> cmd{"XREADGROUP", "GROUP", group, consumer};
            if (count > 0)
            {
                cmd.emplace_back("COUNT");
                cmd.emplace_back(count_str);
            }
            if (block)
            {
                block_str = std::to_string(block->count());
                cmd.emplace_back("BLOCK");
                cmd.emplace_back(block_str);
            }
            if (noack)
            {
                cmd.emplace_back("NOACK");
            }
            cmd.emplace_back("STREAMS");
            cmd.emplace_back(key);
            cmd.emplace_back(id);
            using RET = std::unordered_map<std::string, ItemStream>;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, group, consumer, std::move(block), key, id, count, noack);
}

template <typename REDIS>
template <StringSequence Input, asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_xack(std::string_view key, std::string_view group, Input&& input,
                                     CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto group, auto&& input) mutable
        {
            std::vector<std::string_view> cmd{"XACK", key, group};
            cmd.insert(cmd.end(), input.begin(), input.end());
            using RET = long long;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, group, std::forward<Input>(input));
}

template <typename REDIS>
template <StringSequence Input, asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_sadd(std::string_view key, Input&& input, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto&& input) mutable
        {
            std::vector<std::string_view> cmd{"SADD", key};
            cmd.insert(cmd.end(), input.begin(), input.end());
            using RET = long long;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, std::forward<Input>(input));
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::vector<std::string>>)> CompletionToken>
inline auto Redis<REDIS>::async_smembers(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::vector<std::string>>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            std::initializer_list<std::string_view> cmd{"SMEMBERS", key};
            using RET = std::vector<std::string>;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_sismember(std::string_view key, std::string_view member, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto member) mutable
        {
            std::initializer_list<std::string_view> cmd{"SISMEMBER", key, member};
            using RET = long long;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, member);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_scard(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            std::initializer_list<std::string_view> cmd{"SCARD", key};
            using RET = long long;
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key);
}

template <typename REDIS>
template <typename Result, asio::completion_token_for<void(Expected<Result>)> CompletionToken>
inline auto Redis<REDIS>::async_spop(std::string_view key, const std::optional<long long>& count,
                                     CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<Result>)>(
        [this]<typename Handler>(Handler&& handler, auto key, auto count) mutable
        {
            std::vector<std::string_view> cmd{"SPOP", key};
            std::string count_str{"1"};
            if (count.has_value())
            {
                count_str = std::to_string(count.value());
                cmd.emplace_back("COUNT");
                cmd.emplace_back(count_str);
            }
            else
            {
                cmd.emplace_back(count_str);
            }
            this->call_command<Result>(cmd, std::forward<Handler>(handler));
        },
        token, key, count);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_zadd(std::string_view key, std::string_view member, double score,
                                     sw::redis::UpdateType type, bool changed, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, std::string_view key, std::string_view member, double score,
                                 sw::redis::UpdateType type, bool changed) mutable
        {
            using RET = long long;
            auto [h, io_context, cancelled] = register_slot(std::forward<Handler>(handler));
            m_redis->zadd(
                key, member, score, type, changed,
                [io_context, h = std::move(h), cancelled = std::move(cancelled), this](std::future<RET>&& fut) mutable
                {
                    process<RET>(io_context, std::move(cancelled), std::move(h), std::move(fut));
                });
        },
        token, key, member, score, type, changed);
}

template <typename REDIS>
template <typename Input, asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_zadd(std::string_view key, Input&& input, sw::redis::UpdateType type, bool changed,
                                     CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, std::string_view key, auto&& input, auto type, auto changed) mutable
        {
            using RET = long long;
            auto [h, io_context, cancelled] = register_slot(std::forward<Handler>(handler));
            m_redis->zadd(
                key, input.begin(), input.end(), type, changed,
                [io_context, h = std::move(h), cancelled = std::move(cancelled), this](std::future<RET>&& fut) mutable
                {
                    process<RET>(io_context, std::move(cancelled), std::move(h), std::move(fut));
                });
        },
        token, key, std::forward<Input>(input), type, changed);
}

template <typename REDIS>
template <typename Result, asio::completion_token_for<void(Expected<Result>)> CompletionToken>
inline auto Redis<REDIS>::async_zrangebyscore(std::string_view key, double min, double max, bool withscores,
                                              long long offset, long long count, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<Result>)>(
        [this]<typename Handler>(Handler&& handler, std::string_view key, auto min, auto max, auto withscores,
                                 auto offset, auto count) mutable
        {
            using RET = Result;
            auto mix_str = std::to_string(min);
            auto max_str = std::to_string(max);
            std::vector<std::string_view> cmd{"ZRANGEBYSCORE", key, mix_str, max_str};
            if (withscores)
            {
                cmd.emplace_back("WITHSCORES");
            }
            std::string offset_str;
            std::string count_str;
            if (offset >= 0 && count >= 0)
            {
                offset_str = std::to_string(offset);
                count_str = std::to_string(count);
                cmd.emplace_back("LIMIT");
                cmd.emplace_back(offset_str);
                cmd.emplace_back(count_str);
            }
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key, min, max, withscores, offset, count);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken>
inline auto Redis<REDIS>::async_function_load(std::string_view code, bool replace, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::string>)>(
        [this]<typename Handler>(Handler&& handler, std::string_view code, bool replace) mutable
        {
            using RET = std::string;
            std::vector<std::string_view> cmd{"FUNCTION", "LOAD"};
            if (replace)
            {
                cmd.emplace_back("REPLACE");
            }
            cmd.emplace_back(code);
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, code, replace);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken>
inline auto Redis<REDIS>::async_function_delete(std::string_view lib_name, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::string>)>(
        [this]<typename Handler>(Handler&& handler, std::string_view lib_name) mutable
        {
            using RET = std::string;
            std::initializer_list<std::string_view> cmd{"FUNCTION", "DELETE", lib_name};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, lib_name);
}

template <typename REDIS>
template <typename Result, StringSequence Keys, StringSequence Args,
          asio::completion_token_for<void(Expected<Result>)> CompletionToken>
inline auto Redis<REDIS>::async_fcall(std::string_view func, Keys&& keys, Args&& args, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<Result>)>(
        [this]<typename Handler>(Handler&& handler, std::string_view func, auto&& keys, auto&& args) mutable
        {
            using RET = Result;
            auto num_keys = std::to_string(keys.size());
            std::vector<std::string_view> cmd{"FCALL", func, num_keys};
            cmd.insert(cmd.end(), keys.begin(), keys.end());
            cmd.insert(cmd.end(), args.begin(), args.end());
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, func, std::forward<Keys>(keys), std::forward<Args>(args));
}

template <typename REDIS>
template <typename Result, StringSequence Keys, StringSequence Args,
          asio::completion_token_for<void(Expected<Result>)> CompletionToken>
inline auto Redis<REDIS>::async_eval(std::string_view script, Keys&& keys, Args&& args, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<Result>)>(
        [this]<typename Handler>(Handler&& handler, std::string_view script, auto&& keys, auto&& args) mutable
        {
            using RET = Result;
            auto num_keys = std::to_string(keys.size());
            std::vector<std::string_view> cmd{"EVAL", script, num_keys};
            cmd.insert(cmd.end(), keys.begin(), keys.end());
            cmd.insert(cmd.end(), args.begin(), args.end());
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, script, std::forward<Keys>(keys), std::forward<Args>(args));
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken>
inline auto Redis<REDIS>::async_script_load(std::string_view script, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::string>)>(
        [this]<typename Handler>(Handler&& handler, std::string_view script) mutable
        {
            using RET = std::string;
            std::initializer_list<std::string_view> cmd{"SCRIPT", "LOAD", script};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, script);
}

template <typename REDIS>
template <typename Result, StringSequence Keys, StringSequence Args,
          asio::completion_token_for<void(Expected<Result>)> CompletionToken>
inline auto Redis<REDIS>::async_evalsha(std::string_view sha1, Keys&& keys, Args&& args, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<Result>)>(
        [this]<typename Handler>(Handler&& handler, std::string_view sha1, auto&& keys, auto&& args) mutable
        {
            using RET = Result;
            auto num_keys = std::to_string(keys.size());
            std::vector<std::string_view> cmd{"EVALSHA", sha1, num_keys};
            cmd.insert(cmd.end(), keys.begin(), keys.end());
            cmd.insert(cmd.end(), args.begin(), args.end());
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, sha1, std::forward<Keys>(keys), std::forward<Args>(args));
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken>
inline auto Redis<REDIS>::async_script_flush(CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::string>)>(
        [this]<typename Handler>(Handler&& handler) mutable
        {
            using RET = std::string;
            static std::vector<std::string_view> cmd{"SCRIPT", "FLUSH"};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_publish(std::string_view channel, std::string_view message, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto channel, auto message) mutable
        {
            using RET = long long;
            std::initializer_list<std::string_view> cmd{"PUBLISH", channel, message};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, channel, message);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<long long>)> CompletionToken>
inline auto Redis<REDIS>::async_spublish(std::string_view channel, std::string_view message, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<long long>)>(
        [this]<typename Handler>(Handler&& handler, auto channel, auto message) mutable
        {
            using RET = long long;
            std::initializer_list<std::string_view> cmd{"SPUBLISH", channel, message};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, channel, message);
}
template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken>
inline auto Redis<REDIS>::async_echo(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::string>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            using RET = std::string;
            std::initializer_list<std::string_view> cmd{"ECHO", key};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken>
inline auto Redis<REDIS>::async_ping(std::string_view key, CompletionToken&& token)
{
    return asio::async_initiate<CompletionToken, void(Expected<std::string>)>(
        [this]<typename Handler>(Handler&& handler, auto key) mutable
        {
            using RET = std::string;
            std::initializer_list<std::string_view> cmd{"PING", key};
            this->call_command<RET>(cmd, std::forward<Handler>(handler));
        },
        token, key);
}

template <typename REDIS>
template <asio::completion_token_for<void(Expected<std::string>)> CompletionToken>
inline auto Redis<REDIS>::async_ping(CompletionToken&& token)
{
    if constexpr (std::is_same_v<REDIS, sw::redis::AsyncRedis>)
    {
        return asio::async_initiate<CompletionToken, void(Expected<std::string>)>(
            [this]<typename Handler>(Handler&& handler) mutable
            {
                using RET = std::string;
                static std::vector<std::string> cmd{"PING"};
                this->call_command<RET>(cmd, std::forward<Handler>(handler));
            },
            token);
    }
    else
    {
        static_assert(false, "sw::redis::AsyncRedisCluster not support");
    }
}

}  // namespace asio_async_redis
