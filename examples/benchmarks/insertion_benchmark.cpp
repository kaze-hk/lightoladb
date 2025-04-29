#include <iostream>
#include <chrono>
#include <vector>
#include <string>
#include <random>
#include <iomanip>
#include "lightoladb/core/database.h"

// 辅助函数：生成随机整数
int generateRandomInt(int min, int max) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(min, max);
    return distrib(gen);
}

// 辅助函数：生成随机字符串
std::string generateRandomString(int length) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    
    std::string result;
    result.reserve(length);
    
    for (int i = 0; i < length; ++i) {
        result += alphanum[generateRandomInt(0, sizeof(alphanum) - 2)];
    }
    
    return result;
}

// 辅助函数：生成随机浮点数
double generateRandomDouble(double min, double max) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_real_distribution<> distrib(min, max);
    return distrib(gen);
}

// 辅助函数：测量函数执行时间
template<typename Func>
double measureExecutionTime(Func func) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    
    std::chrono::duration<double, std::milli> duration = end - start;
    return duration.count();
}

int main() {
    // 创建数据库实例
    lightoladb::Database db;
    
    std::cout << "LightOLAP 插入性能测试" << std::endl;
    std::cout << "======================" << std::endl;
    
    // 创建测试表
    std::string createTableSQL = "CREATE TABLE benchmark_table ("
                                 "id UInt32, "
                                 "name String, "
                                 "value Float64, "
                                 "category String, "
                                 "timestamp UInt64"
                                 ") ENGINE = Memory";
    
    auto result = db.executeQuery(createTableSQL);
    if (!result.success()) {
        std::cerr << "表创建失败: " << result.errorMessage() << std::endl;
        return 1;
    }
    
    // 运行不同批量大小的插入测试
    const std::vector<int> batchSizes = {1, 10, 100, 1000, 10000};
    
    for (int batchSize : batchSizes) {
        std::cout << "\n测试批量插入 " << batchSize << " 行数据:" << std::endl;
        
        // 构建 INSERT SQL 语句
        std::stringstream insertSQL;
        insertSQL << "INSERT INTO benchmark_table (id, name, value, category, timestamp) VALUES ";
        
        for (int i = 0; i < batchSize; ++i) {
            if (i > 0) insertSQL << ", ";
            
            insertSQL << "(" 
                      << i << ", "
                      << "'" << generateRandomString(10) << "', "
                      << std::fixed << std::setprecision(2) << generateRandomDouble(0, 1000) << ", "
                      << "'" << generateRandomString(5) << "', "
                      << static_cast<uint64_t>(std::time(nullptr))
                      << ")";
        }
        
        // 测量插入时间
        double insertTime = measureExecutionTime([&]() {
            auto insertResult = db.executeQuery(insertSQL.str());
            if (!insertResult.success()) {
                std::cerr << "插入失败: " << insertResult.errorMessage() << std::endl;
            }
        });
        
        std::cout << "插入 " << batchSize << " 行数据用时: " << insertTime << " ms" << std::endl;
        std::cout << "每行数据平均用时: " << insertTime / batchSize << " ms" << std::endl;
        
        // 清空表，准备下一轮测试
        if (batchSize != batchSizes.back()) {
            db.executeQuery("DROP TABLE benchmark_table");
            db.executeQuery(createTableSQL);
        }
    }
    
    // 测试不同数据类型的插入性能
    std::cout << "\n不同数据类型的插入性能对比:" << std::endl;
    
    // 创建不同类型的表
    const std::vector<std::pair<std::string, std::string>> typeTests = {
        {"int_table", "id UInt32, value Int32"},
        {"float_table", "id UInt32, value Float64"},
        {"string_table", "id UInt32, value String"},
        {"mixed_table", "id UInt32, int_val Int32, float_val Float64, str_val String"}
    };
    
    const int rowsToInsert = 1000;
    
    for (const auto& typeTest : typeTests) {
        const auto& tableName = typeTest.first;
        const auto& columns = typeTest.second;
        
        // 创建表
        std::string createTypeTableSQL = "CREATE TABLE " + tableName + " (" + columns + ") ENGINE = Memory";
        db.executeQuery(createTypeTableSQL);
        
        // 构建 INSERT SQL
        std::stringstream insertTypeSQL;
        insertTypeSQL << "INSERT INTO " << tableName << " VALUES ";
        
        for (int i = 0; i < rowsToInsert; ++i) {
            if (i > 0) insertTypeSQL << ", ";
            
            if (tableName == "int_table") {
                insertTypeSQL << "(" << i << ", " << generateRandomInt(-1000, 1000) << ")";
            } else if (tableName == "float_table") {
                insertTypeSQL << "(" << i << ", " << generateRandomDouble(-1000, 1000) << ")";
            } else if (tableName == "string_table") {
                insertTypeSQL << "(" << i << ", '" << generateRandomString(20) << "')";
            } else if (tableName == "mixed_table") {
                insertTypeSQL << "(" 
                              << i << ", " 
                              << generateRandomInt(-1000, 1000) << ", "
                              << generateRandomDouble(-1000, 1000) << ", '"
                              << generateRandomString(10) << "')";
            }
        }
        
        // 测量插入时间
        double typeInsertTime = measureExecutionTime([&]() {
            auto insertResult = db.executeQuery(insertTypeSQL.str());
            if (!insertResult.success()) {
                std::cerr << "类型测试插入失败: " << insertResult.errorMessage() << std::endl;
            }
        });
        
        std::cout << "表 " << std::setw(12) << std::left << tableName
                  << " 插入 " << rowsToInsert << " 行用时: " 
                  << std::setw(10) << std::right << typeInsertTime << " ms" << std::endl;
    }
    
    std::cout << "\n测试完成！" << std::endl;
    return 0;
}