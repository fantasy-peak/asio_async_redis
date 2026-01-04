add_rules("mode.debug", "mode.release")

add_repositories("my_private_repo https://github.com/fantasy-peak/xmake-repo.git")

add_requires("boost", {system = false, configs={cmake=false, url=true}})
add_requires("spdlog", "redis-plus-plus 90b8331530685026f608fafbab2e5855bfc07770", "catch2")
add_requires("tl_expected")

add_defines("ASIO_ASYNC_REDIS_USE_BOOST_ASIO")

set_languages("c23", "c++23")

-- set_policy("build.sanitizer.address", true)

target("test_redis")
    set_kind("binary")
    add_includedirs("include", "test")
    add_files(
        "test/main.cpp",
        "test/test_cancel.cpp",
        "test/test_hash.cpp",
        "test/test_list.cpp",
        "test/test_stream.cpp",
        "test/test_string.cpp",
        "test/test_set.cpp",
        "test/test_lua.cpp",
        "test/test_publish.cpp",
        "test/test_connection.cpp",
        "test/test_construction.cpp"
    )
    add_packages("boost", "spdlog", "redis-plus-plus", "catch2", "tl_expected")
    add_ldflags("-static-libstdc++", "-static-libgcc", {force = true})
target_end()

target("test_redis_cluster")
    set_kind("binary")
    add_includedirs("include", "test")
    add_files(
        "test/main.cpp",
        -- "test/test_cancel.cpp",
        "test/test_hash.cpp",
        "test/test_list.cpp",
        "test/test_stream.cpp",
        "test/test_string.cpp",
        "test/test_set.cpp",
        -- "test/test_lua.cpp",
        "test/test_publish.cpp",
        "test/test_connection.cpp"
        )
    add_defines("TEST_REDIS_CLUSTER")
    add_packages("boost", "spdlog", "redis-plus-plus", "catch2", "tl_expected")
    add_ldflags("-static-libstdc++", "-static-libgcc", {force = true})
target_end()
