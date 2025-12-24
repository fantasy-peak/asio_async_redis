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

    return_code = session.run();

    spdlog::info("start stop...");
    async_redis->stop();
    pool->stop();
    spdlog::info("end test");

    return return_code;
}
