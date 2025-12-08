#include "utils.h"

#include <memory>
#include <string>

int main(int argc, char* argv[])
{
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [thread %t] [%l] %v");

    async_redis->start();
    pool->start();

    std::this_thread::sleep_for(std::chrono::seconds(1));

    asio::post(pool->getIoContext(),
               []
               {
                   spdlog::info("start test");
               });

    Catch::Session session;
    int return_code = session.applyCommandLine(argc, argv);
    if (return_code != 0) return return_code;

    BOOST_SCOPE_EXIT(&async_redis, &pool)
    {
        async_redis->stop();
        pool->stop();
    }
    BOOST_SCOPE_EXIT_END

    return session.run();
}