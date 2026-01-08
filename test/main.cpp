#include "utils.h"

#include <string>

int main(int argc, char* argv[]) {
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [thread %t] [%l] %v");

    asio::post(pool->getIoContext(), [] { spdlog::info("start test"); });

    Catch::Session session;
    int return_code = session.applyCommandLine(argc, argv);
    if (return_code != 0)
        return return_code;

    return_code = session.run();

    spdlog::info("end test");

    return return_code;
}
