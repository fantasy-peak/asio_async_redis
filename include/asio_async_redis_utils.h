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

#include <atomic>

#if __has_include(<expected>)
#include <expected>
#else
#define USE_TL_EXPECTED 1
#include <tl/expected.hpp>
#endif

#include <list>
#include <memory>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#ifdef ASIO_ASYNC_REDIS_USE_BOOST_ASIO
#include <boost/asio.hpp>
#else
#include <asio.hpp>
#endif

namespace asio_async_redis
{

#ifdef ASIO_ASYNC_REDIS_USE_BOOST_ASIO
namespace asio = boost::asio;
#endif

using Attrs = std::vector<std::pair<std::string, std::string>>;
using Item = std::pair<std::string, std::optional<Attrs>>;
using ItemStream = std::vector<Item>;

class ContextPool final
{
  public:
    ContextPool(std::size_t pool_size) : m_next_io_context(0)
    {
        if (pool_size == 0) throw std::runtime_error("ContextPool size is 0");
        for (std::size_t i = 0; i < pool_size; ++i)
        {
            auto io_context_ptr = std::make_shared<asio::io_context>();
            m_io_contexts.emplace_back(io_context_ptr);
            m_work.emplace_back(
                asio::require(io_context_ptr->get_executor(), asio::execution::outstanding_work.tracked));
        }
    }

    void start()
    {
        for (auto& context : m_io_contexts)
            m_threads.emplace_back(
                [&]
                {
                    context->run();
                });
    }

    void stop()
    {
        for (auto& context_ptr : m_io_contexts) context_ptr->stop();
        for (auto& thread : m_threads)
        {
            if (thread.joinable()) thread.join();
        }
    }

    asio::io_context& getIoContext()
    {
        size_t index = m_next_io_context.fetch_add(1, std::memory_order_relaxed);
        return *m_io_contexts[index % m_io_contexts.size()];
    }

    auto& getIoContextPtr()
    {
        size_t index = m_next_io_context.fetch_add(1, std::memory_order_relaxed);
        return m_io_contexts[index % m_io_contexts.size()];
    }

    auto getIoContextRawPtr()
    {
        size_t index = m_next_io_context.fetch_add(1, std::memory_order_relaxed);
        return m_io_contexts[index % m_io_contexts.size()].get();
    }

  private:
    std::vector<std::shared_ptr<asio::io_context>> m_io_contexts;
    std::list<asio::any_io_executor> m_work;
    std::atomic_uint64_t m_next_io_context;
    std::vector<std::thread> m_threads;
};

class RedisError
{
  public:
    enum class ErrorCode : int8_t
    {
        Cancel,
        Error,
        IoError,
        TimeoutError,
        ClosedError,
        ProtoError,
        OomError,
        ReplyError,
        WatchError,
        MovedError,
        AskError,
        UnknownError,
    };

    RedisError(std::string msg, ErrorCode code) : m_error_msg(std::move(msg)), m_error_code(code) {}

    [[nodiscard]] auto& message() const { return m_error_msg; }
    [[nodiscard]] auto& error_code() const { return m_error_code; }

  private:
    std::string m_error_msg;
    ErrorCode m_error_code{ErrorCode::UnknownError};
};

inline std::string_view to_string(RedisError::ErrorCode error_code)
{
    switch (error_code)
    {
        case RedisError::ErrorCode::Cancel:
            return "Cancel";
        case RedisError::ErrorCode::Error:
            return "Error";
        case RedisError::ErrorCode::IoError:
            return "IoError";
        case RedisError::ErrorCode::TimeoutError:
            return "TimeoutError";
        case RedisError::ErrorCode::ClosedError:
            return "ClosedError";
        case RedisError::ErrorCode::ProtoError:
            return "ProtoError";
        case RedisError::ErrorCode::OomError:
            return "OomError";
        case RedisError::ErrorCode::ReplyError:
            return "ReplyError";
        case RedisError::ErrorCode::WatchError:
            return "WatchError";
        case RedisError::ErrorCode::MovedError:
            return "MovedError";
        case RedisError::ErrorCode::AskError:
            return "AskError";
        case RedisError::ErrorCode::UnknownError:
            return "UnknownError";
    }
    return "UnknownError";
}

#ifdef USE_TL_EXPECTED
template <typename T>
using Expected = tl::expected<T, RedisError>;
template <typename E>
using UnExpected = tl::unexpected<E>;
#else
template <typename T>
using Expected = std::expected<T, RedisError>;
template <typename E>
using UnExpected = std::unexpected<E>;
#endif

template <typename T>
concept StringSequence =
    std::same_as<std::decay_t<T>, std::vector<std::string>> ||
    std::same_as<std::decay_t<T>, std::vector<std::string_view>> ||
    std::same_as<std::decay_t<T>, std::list<std::string>> || std::same_as<std::decay_t<T>, std::list<std::string_view>>;

template <typename T>
concept StringKVContainer = std::same_as<std::decay_t<T>, std::unordered_map<std::string, std::string>> ||
                            std::same_as<std::decay_t<T>, std::map<std::string, std::string>> ||
                            std::same_as<std::decay_t<T>, std::vector<std::pair<std::string, std::string>>>;

template <typename T>
concept REDIS = std::is_same_v<std::decay_t<T>, sw::redis::AsyncRedis> ||
                std::is_same_v<std::decay_t<T>, sw::redis::AsyncRedisCluster>;

}  // namespace asio_async_redis
