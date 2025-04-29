# LightOLAP - 轻量级OLAP数据库

LightOLAP 是一个用C++实现的轻量级列式存储OLAP数据库，设计目标是提供类似ClickHouse的基本功能，同时保持简单易用、易于编译和集成。

## 功能特点

- 列式存储引擎，支持高效的数据压缩和查询
- 简单的SQL接口，支持基本的DDL和DML操作
- 内存存储引擎，适合中小规模数据分析
- 轻量级设计，易于集成到其他应用程序中
- 支持多种数据类型，包括数值、字符串等
- 支持聚合

## 编译和安装

### 前提条件

- CMake 3.14 或更高版本
- 支持C++17的编译器（如GCC 7+、Clang 5+、MSVC 2017+）

### 编译步骤

```bash
mkdir build
cd build
cmake ..
make
```

编译完成后，会在 build/src 目录下生成 lightoladb_client 可执行文件。

### benchmark程序
```bash
# 运行数据插入性能测试
./examples/insertion_benchmark

# 运行查询性能测试
./examples/query_benchmark

# 运行集成示例
./examples/integration_example

# 运行性能比较测试
./examples/performance_comparison
```

## 使用示例

启动交互式客户端：

```bash
./lightoladb_client
```

创建表：

```sql
CREATE TABLE example (
    id UInt32,
    name String,
    value Float64
) ENGINE = Memory
```

插入数据：

```sql
INSERT INTO example (id, name, value) VALUES (1, 'test', 123.45), (2, 'sample', 678.90)
```

查询数据：

```sql
SELECT * FROM example
SELECT id, value FROM example
```

## 架构设计

LightOLAP 采用模块化设计，主要包括以下组件：

1. **核心数据结构**：列和数据块的抽象表示
2. **存储引擎**：处理数据的存储和检索
3. **SQL解析器**：解析和验证SQL查询
4. **查询执行引擎**：执行SQL查询并返回结果
5. **客户端接口**：提供与数据库交互的界面

## 限制和注意事项

- 目前仅支持内存存储引擎，不支持持久化
- SQL解析器和执行引擎功能有限，仅支持基本操作
- 未实现高级OLAP功能，如窗口函数等
- 不支持事务和并发控制

## 未来计划

- 添加基于文件的持久化存储引擎
- 支持更多SQL语法和功能
- 实现基本的数据压缩算法
- 添加分布式查询支持
- 优化查询执行性能

## 许可

本项目采用Apache License Version 2.0许可证。详见LICENSE文件。
