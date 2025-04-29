#include <iostream>
#include <chrono>
#include <vector>
#include <string>
#include <random>
#include <iomanip>
#include <functional>
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

// 创建并填充测试表
void setupTestTable(lightoladb::Database& db, const std::string& tableName, int rowCount) {
    // 创建测试表
    std::string createTableSQL = "CREATE TABLE " + tableName + " ("
                                "id UInt32, "
                                "name String, "
                                "age UInt8, "
                                "score Float64, "
                                "city String, "
                                "active UInt8"
                                ") ENGINE = Memory";
    
    auto result = db.executeQuery(createTableSQL);
    if (!result.success()) {
        std::cerr << "表创建失败: " << result.errorMessage() << std::endl;
        return;
    }
    
    // 生成城市列表
    std::vector<std::string> cities = {"北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安", "南京", "重庆"};
    
    // 批量插入数据
    const int batchSize = 1000; // 每次插入的行数
    int remainingRows = rowCount;
    
    while (remainingRows > 0) {
        int currentBatchSize = std::min(batchSize, remainingRows);
        remainingRows -= currentBatchSize;
        
        std::stringstream insertSQL;
        insertSQL << "INSERT INTO " + tableName + " (id, name, age, score, city, active) VALUES ";
        
        for (int i = 0; i < currentBatchSize; ++i) {
            if (i > 0) insertSQL << ", ";
            
            int id = rowCount - remainingRows - i;
            std::string city = cities[generateRandomInt(0, cities.size() - 1)];
            int age = generateRandomInt(18, 60);
            double score = generateRandomDouble(0, 100);
            int active = generateRandomInt(0, 1);
            
            insertSQL << "(" 
                      << id << ", "
                      << "'" << "user" << id << "', "
                      << age << ", "
                      << std::fixed << std::setprecision(2) << score << ", "
                      << "'" << city << "', "
                      << active
                      << ")";
        }
        
        auto insertResult = db.executeQuery(insertSQL.str());
        if (!insertResult.success()) {
            std::cerr << "插入失败: " << insertResult.errorMessage() << std::endl;
            return;
        }
    }
    
    std::cout << "已成功创建表 " << tableName << " 并插入 " << rowCount << " 行数据" << std::endl;
}

// 运行查询测试
void runQueryTests(lightoladb::Database& db, const std::string& tableName) {
    // 定义一系列要测试的查询
    struct QueryTest {
        std::string name;
        std::string query;
    };
    
    std::vector<QueryTest> queryTests = {
        {"简单全表扫描", "SELECT * FROM " + tableName},
        {"单列查询", "SELECT id FROM " + tableName},
        {"多列查询", "SELECT id, name, age FROM " + tableName},
        {"聚合查询", "SELECT COUNT(*) FROM " + tableName},
        {"限制结果集大小", "SELECT * FROM " + tableName + " LIMIT 10"},
        {"带过滤条件", "SELECT * FROM " + tableName + " WHERE age > 30"},
        {"复杂过滤条件", "SELECT * FROM " + tableName + " WHERE age > 30 AND score > 50 AND city = '北京'"},
        {"排序查询", "SELECT * FROM " + tableName + " ORDER BY score DESC LIMIT 100"}
    };
    
    std::cout << "\n开始运行查询性能测试...\n" << std::endl;
    std::cout << std::left << std::setw(30) << "查询类型"
              << std::right << std::setw(15) << "执行时间 (ms)"
              << std::right << std::setw(15) << "结果行数" << std::endl;
    std::cout << std::string(60, '-') << std::endl;
    
    // 对每个查询运行测试
    for (const auto& test : queryTests) {
        size_t rowCount = 0;
        
        double queryTime = measureExecutionTime([&]() {
            auto queryResult = db.executeQuery(test.query);
            if (!queryResult.success()) {
                std::cerr << "查询失败: " << queryResult.errorMessage() << std::endl;
            } else {
                rowCount = queryResult.rowCount();
            }
        });
        
        std::cout << std::left << std::setw(30) << test.name
                  << std::right << std::setw(15) << std::fixed << std::setprecision(2) << queryTime
                  << std::right << std::setw(15) << rowCount << std::endl;
    }
}

int main() {
    // 创建数据库实例
    lightoladb::Database db;
    
    std::cout << "LightOLAP 查询性能测试" << std::endl;
    std::cout << "======================" << std::endl;
    
    // 测试数据集大小
    const std::vector<int> testSizes = {1000, 10000, 100000};
    
    for (int size : testSizes) {
        std::string tableName = "query_test_" + std::to_string(size);
        
        std::cout << "\n--- 测试数据集: " << size << " 行 ---" << std::endl;
        
        // 准备测试表
        setupTestTable(db, tableName, size);
        
        // 运行查询测试
        runQueryTests(db, tableName);
        
        // 清理
        auto dropResult = db.executeQuery("DROP TABLE " + tableName);
        if (!dropResult.success()) {
            std::cerr << "删除表失败: " << dropResult.errorMessage() << std::endl;
        }
    }
    
    std::cout << "\n测试完成！" << std::endl;
    return 0;
}