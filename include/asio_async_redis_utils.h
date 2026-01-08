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
#include <chrono>
#include <functional>
#include <future>
#include <optional>
#include <queue>
#include <string>
#include <string_view>

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

namespace asio_async_redis {

#ifdef ASIO_ASYNC_REDIS_USE_BOOST_ASIO
namespace asio = boost::asio;
#endif

using Attrs = std::vector<std::pair<std::string, std::string>>;
using Item = std::pair<std::string, std::optional<Attrs>>;
using ItemStream = std::vector<Item>;

class ContextPool final {
  public:
    ContextPool(std::size_t pool_size) : m_next_io_context(0) {
        if (pool_size == 0)
            throw std::runtime_error("ContextPool size is 0");
        for (std::size_t i = 0; i < pool_size; ++i) {
            auto io_context_ptr = std::make_shared<asio::io_context>();
            m_io_contexts.emplace_back(io_context_ptr);
            m_work.emplace_back(
                asio::require(io_context_ptr->get_executor(), asio::execution::outstanding_work.tracked));
            m_threads.emplace_back([io_context_ptr] { io_context_ptr->run(); });
        }
    }

    ContextPool(const ContextPool&) = delete;
    ContextPool& operator=(const ContextPool&) = delete;
    ContextPool(ContextPool&&) = delete;
    ContextPool& operator=(ContextPool&&) = delete;

    ~ContextPool() {
        for (auto& context_ptr : m_io_contexts)
            context_ptr->stop();
        for (auto& thread : m_threads) {
            if (thread.joinable())
                thread.join();
        }
    }

    asio::io_context& getIoContext() {
        size_t index = m_next_io_context.fetch_add(1, std::memory_order_relaxed);
        return *m_io_contexts[index % m_io_contexts.size()];
    }

    auto& getIoContextPtr() {
        size_t index = m_next_io_context.fetch_add(1, std::memory_order_relaxed);
        return m_io_contexts[index % m_io_contexts.size()];
    }

    auto getIoContextRawPtr() {
        size_t index = m_next_io_context.fetch_add(1, std::memory_order_relaxed);
        return m_io_contexts[index % m_io_contexts.size()].get();
    }

  private:
    std::vector<std::shared_ptr<asio::io_context>> m_io_contexts;
    std::list<asio::any_io_executor> m_work;
    std::atomic_uint64_t m_next_io_context;
    std::vector<std::thread> m_threads;
};

class RedisError {
  public:
    enum class ErrorCode : int8_t {
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

    RedisError(std::string msg, ErrorCode code) : m_error_msg(std::move(msg)), m_error_code(code) {
    }

    [[nodiscard]] auto& message() const {
        return m_error_msg;
    }

    [[nodiscard]] auto& error_code() const {
        return m_error_code;
    }

  private:
    std::string m_error_msg;
    ErrorCode m_error_code{ErrorCode::UnknownError};
};

inline std::string_view to_string(RedisError::ErrorCode error_code) {
    switch (error_code) {
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

struct executor_alert_base {
    [[deprecated("ASIO_REDIS: Specific executor detected (inline/system). Consider binding other executor.")]]
    static void trigger_warning() {
    }
};

struct no_alert_base {
    static void trigger_warning() {
    }
};

template <bool IsBad>
struct alert_selector : no_alert_base {};

template <>
struct alert_selector<true> : executor_alert_base {};

template <typename T>
struct executor_warning {
#if ASIO_ASYNC_REDIS_USE_BOOST_ASIO
#if BOOST_VERSION >= 109000
    static constexpr bool is_bad = std::is_same_v<T, asio::system_executor> || std::is_same_v<T, asio::inline_executor>;
#else
    static constexpr bool is_bad = std::is_same_v<T, asio::system_executor>;
#endif
#else
    // todo asio standalone
    static constexpr bool is_bad = std::is_same_v<T, asio::system_executor>;
#endif
    executor_warning() {
        alert_selector<is_bad>::trigger_warning();
    }
};

class CmdArgs {
  public:
    CmdArgs() = default;

    CmdArgs& operator<<(long long input) {
        m_args.push_back(std::to_string(input));
        m_cmds.emplace_back(m_args.back());
        return *this;
    }

    CmdArgs& operator<<(std::string_view input) {
        m_cmds.emplace_back(input);
        return *this;
    }

    CmdArgs& operator<<(const std::chrono::seconds& input) {
        m_args.push_back(std::to_string(input.count()));
        m_cmds.emplace_back(m_args.back());
        return *this;
    }

    CmdArgs& operator<<(const std::chrono::milliseconds& input) {
        m_args.push_back(std::to_string(input.count()));
        m_cmds.emplace_back(m_args.back());
        return *this;
    }

    template <StringSequence Input>
    CmdArgs& operator<<(const std::reference_wrapper<Input>& input) {
        auto& input_ref = input.get();
        m_cmds.insert(m_cmds.end(), input_ref.begin(), input_ref.end());
        return *this;
    }

    const auto& operator()() {
        return m_cmds;
    }

  private:
    std::vector<std::string_view> m_cmds;
    std::list<std::string> m_args;
};

template <typename T>
class RedisClientPool final : public std::enable_shared_from_this<RedisClientPool<T>> {
  public:
    RedisClientPool(const std::string& uri, size_t max_size = 30, size_t pool_size = 1)
        : m_redis_uri(uri), m_max_size(max_size) {
        m_loop_ptr = std::make_shared<sw::redis::EventLoop>();
        m_pool_ptr = std::make_shared<asio_async_redis::ContextPool>(pool_size);
        m_ctx_ptr = m_pool_ptr->getIoContextPtr();
        for (size_t i = 0; i < max_size; i++) {
            m_pool.push(std::make_shared<T>(m_redis_uri, m_pool_ptr, m_loop_ptr));
        }
    }

    RedisClientPool(const RedisClientPool&) = delete;
    RedisClientPool& operator=(const RedisClientPool&) = delete;
    RedisClientPool(RedisClientPool&&) = delete;
    RedisClientPool& operator=(RedisClientPool&&) = delete;

    ~RedisClientPool() {
        std::promise<void> done;
        asio::dispatch(*m_ctx_ptr, [&] {
            std::queue<std::shared_ptr<T>> empty;
            m_pool.swap(empty);
            done.set_value();
        });
        done.get_future().wait();
    }

    class Handle {
      public:
        Handle(std::weak_ptr<RedisClientPool> pool, std::shared_ptr<T> conn)
            : m_pool(std::move(pool)), m_conn(std::move(conn)) {
        }

        ~Handle() {
            if (auto sp = m_pool.lock()) {
                sp->release(m_conn);
            }
        }

        [[nodiscard]] auto& get() const {
            return m_conn;
        }

        Handle(const Handle&) = delete;
        Handle& operator=(const Handle&) = delete;
        Handle(Handle&&) = delete;
        Handle& operator=(Handle&&) = delete;

      private:
        std::weak_ptr<RedisClientPool> m_pool;
        std::shared_ptr<T> m_conn;
    };

    template <typename CompletionToken = asio::use_awaitable_t<>>
    [[nodiscard]] auto acquire(CompletionToken&& token = CompletionToken{}) {
        return asio::async_initiate<CompletionToken, void(std::optional<std::unique_ptr<Handle>>)>(
            [this]<typename Handler>(Handler&& handler) mutable {
                asio::post(*m_ctx_ptr, [this, handler = std::forward<Handler>(handler)] mutable {
                    std::optional<std::unique_ptr<Handle>> result;
                    if (!m_pool.empty()) {
                        auto conn = std::move(m_pool.front());
                        m_pool.pop();
                        result = std::make_unique<Handle>(this->shared_from_this(), std::move(conn));
                    }
                    auto ex = asio::get_associated_executor(handler);
                    asio::post(ex, [h = std::move(handler), ret = std::move(result)] mutable {
                        std::move(h)(std::move(ret));
                    });
                });
            },
            token);
    }

  private:
    void release(std::shared_ptr<T> conn) {
        if (conn) {
            asio::post(*m_ctx_ptr, [this, conn = std::move(conn)] mutable { m_pool.push(std::move(conn)); });
        }
    }

  private:
    std::string m_redis_uri;
    size_t m_max_size;

    std::shared_ptr<sw::redis::EventLoop> m_loop_ptr;
    std::shared_ptr<asio_async_redis::ContextPool> m_pool_ptr;
    std::shared_ptr<asio::io_context> m_ctx_ptr;

    std::queue<std::shared_ptr<T>> m_pool;
};

}  // namespace asio_async_redis
