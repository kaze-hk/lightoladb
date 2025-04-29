set(LIB_SOURCE_FILES
    common/data_types.cpp
    common/field.cpp
    storage/storage_factory.cpp
    sql/parser.cpp
    sql/executor.cpp
    core/database.cpp
)

# 创建库
add_library(lightoladb STATIC ${LIB_SOURCE_FILES})

# 创建主应用程序
add_executable(lightoladb_client main.cpp)
target_link_libraries(lightoladb_client lightoladb)