# 添加项目的示例和 benchmark 程序

# 查找 Threads 包，用于多线程 benchmark
find_package(Threads REQUIRED)

# 设置 benchmark 文件列表
set(BENCHMARK_SOURCES
    benchmarks/insertion_benchmark.cpp
    benchmarks/query_benchmark.cpp
    benchmarks/integration_example.cpp
    benchmarks/performance_comparison.cpp
)

# 为每个 benchmark 文件创建可执行文件
foreach(benchmark_src ${BENCHMARK_SOURCES})
    get_filename_component(benchmark_name ${benchmark_src} NAME_WE)
    add_executable(${benchmark_name} ${benchmark_src})
    target_link_libraries(${benchmark_name} lightoladb Threads::Threads)
endforeach()