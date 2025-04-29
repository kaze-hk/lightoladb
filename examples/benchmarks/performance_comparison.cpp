#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <numeric>
#include <random>
#include "lightoladb/core/database.h"

// 辅助函数：测量函数执行时间（毫秒）
template<typename Func>
double measure_time_ms(Func func) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    
    std::chrono::duration<double, std::milli> duration = end - start;
    return duration.count();
}

// 辅助函数：生成随机整数
int random_int(int min, int max) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(min, max);
    return distrib(gen);
}

// 辅助函数：生成随机浮点数
double random_double(double min, double max) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_real_distribution<> distrib(min, max);
    return distrib(gen);
}

// 辅助函数：计算平均值和标准差
std::pair<double, double> calculate_stats(const std::vector<double>& values) {
    double sum = std::accumulate(values.begin(), values.end(), 0.0);
    double mean = sum / values.size();
    
    std::vector<double> diff(values.size());
    std::transform(values.begin(), values.end(), diff.begin(),
                  [mean](double x) { return x - mean; });
    
    double sq_sum = std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
    double std_dev = std::sqrt(sq_sum / values.size());
    
    return {mean, std_dev};
}

// 生成CSV格式的性能报告
void generate_csv_report(const std::string& filename, 
                         const std::vector<std::string>& test_names,
                         const std::vector<std::vector<double>>& test_results) {
    std::ofstream file(filename);
    
    if (!file.is_open()) {
        std::cerr << "无法创建报告文件: " << filename << std::endl;
        return;
    }
    
    // 写入标题行
    file << "测试名称,平均时间(ms),标准差(ms),最小时间(ms),最大时间(ms)" << std::endl;
    
    // 写入数据行
    for (size_t i = 0; i < test_names.size(); ++i) {
        auto [mean, std_dev] = calculate_stats(test_results[i]);
        double min_time = *std::min_element(test_results[i].begin(), test_results[i].end());
        double max_time = *std::max_element(test_results[i].begin(), test_results[i].end());
        
        file << test_names[i] << ","
             << std::fixed << std::setprecision(2) << mean << ","
             << std::fixed << std::setprecision(2) << std_dev << ","
             << std::fixed << std::setprecision(2) << min_time << ","
             << std::fixed << std::setprecision(2) << max_time << std::endl;
    }
    
    std::cout << "性能报告已生成: " << filename << std::endl;
}

// 表结构定义
struct TableConfig {
    std::string name;
    std::string schema;
    int row_count;
};

// 查询定义
struct QueryConfig {
    std::string name;
    std::string sql;
    bool measure_rows;
};

int main() {
    // 创建数据库实例
    lightoladb::Database db;
    
    std::cout << "LightOLAP 性能比较测试" << std::endl;
    std::cout << "====================" << std::endl;
    
    // 设置测试参数
    const int num_iterations = 5;  // 每个测试重复次数
    
    // 定义测试数据集
    std::vector<TableConfig> tables = {
        {
            "small_table",
            "id UInt32, name String, value Float64, category String, enabled UInt8",
            1000
        },
        {
            "medium_table",
            "id UInt32, name String, value Float64, category String, enabled UInt8",
            10000
        },
        {
            "wide_table",
            "id UInt32, f1 Int32, f2 Int32, f3 Int32, f4 Int32, f5 Int32, "
            "f6 Int32, f7 Int32, f8 Int32, f9 Int32, f10 Int32",
            5000
        }
    };
    
    // 定义要测试的查询
    std::vector<QueryConfig> queries = {
        {"全表扫描", "SELECT * FROM {table}", true},
        {"单列查询", "SELECT id FROM {table}", true},
        {"多列查询", "SELECT id, name, value FROM {table}", false},
        {"聚合查询", "SELECT COUNT(*), SUM(id) FROM {table}", false},
        {"条件过滤", "SELECT * FROM {table} WHERE id % 10 = 0", true},
    };
    
    // 存储测试结果
    std::vector<std::string> test_names;
    std::vector<std::vector<double>> test_results;
    
    // 为每个表准备数据
    for (const auto& table_config : tables) {
        std::cout << "\n准备表 " << table_config.name << " (" 
                  << table_config.row_count << " 行)..." << std::endl;
        
        // 创建表
        std::string create_sql = "CREATE TABLE " + table_config.name + 
                                 " (" + table_config.schema + ") ENGINE = Memory";
        
        auto create_result = db.executeQuery(create_sql);
        if (!create_result.success()) {
            std::cerr << "创建表失败: " << create_result.errorMessage() << std::endl;
            continue;
        }
        
        // 构建INSERT语句
        const int batch_size = 1000;  // 每次插入的行数
        int remaining = table_config.row_count;
        
        while (remaining > 0) {
            int current_batch = std::min(batch_size, remaining);
            remaining -= current_batch;
            
            std::stringstream insert_sql;
            insert_sql << "INSERT INTO " << table_config.name << " VALUES ";
            
            for (int i = 0; i < current_batch; ++i) {
                if (i > 0) insert_sql << ", ";
                
                int id = table_config.row_count - remaining - i;
                
                if (table_config.name == "wide_table") {
                    // 宽表插入
                    insert_sql << "(" << id;
                    for (int j = 1; j <= 10; ++j) {
                        insert_sql << ", " << random_int(-1000, 1000);
                    }
                    insert_sql << ")";
                } else {
                    // 普通表插入
                    std::string name = "item_" + std::to_string(id);
                    double value = random_double(0, 1000);
                    std::string category = "category_" + std::to_string(id % 5);
                    int enabled = random_int(0, 1);
                    
                    insert_sql << "(" << id << ", '" << name << "', " 
                              << value << ", '" << category << "', " << enabled << ")";
                }
            }
            
            auto insert_result = db.executeQuery(insert_sql.str());
            if (!insert_result.success()) {
                std::cerr << "插入数据失败: " << insert_result.errorMessage() << std::endl;
                break;
            }
        }
        
        std::cout << "表 " << table_config.name << " 准备完成" << std::endl;
        
        // 对每个查询进行测试
        for (const auto& query_config : queries) {
            std::string formatted_query = query_config.sql;
            // 替换查询中的表名占位符
            size_t pos = formatted_query.find("{table}");
            if (pos != std::string::npos) {
                formatted_query.replace(pos, 7, table_config.name);
            }
            
            std::string test_name = table_config.name + " - " + query_config.name;
            test_names.push_back(test_name);
            
            std::cout << "运行测试: " << test_name << std::endl;
            
            std::vector<double> times;
            
            // 运行多次迭代
            for (int iter = 0; iter < num_iterations; ++iter) {
                size_t row_count = 0;
                
                double time = measure_time_ms([&]() {
                    auto result = db.executeQuery(formatted_query);
                    if (!result.success()) {
                        std::cerr << "查询失败: " << result.errorMessage() << std::endl;
                    } else {
                        row_count = result.rowCount();
                    }
                });
                
                times.push_back(time);
                
                if (query_config.measure_rows) {
                    std::cout << "  迭代 #" << (iter + 1) << ": " << time << " ms, "
                              << row_count << " 行" << std::endl;
                } else {
                    std::cout << "  迭代 #" << (iter + 1) << ": " << time << " ms" << std::endl;
                }
            }
            
            test_results.push_back(times);
            
            // 计算并显示统计信息
            auto [mean, std_dev] = calculate_stats(times);
            std::cout << "  平均: " << mean << " ms, 标准差: " << std_dev << " ms" << std::endl;
        }
        
        // 清理表
        db.executeQuery("DROP TABLE " + table_config.name);
    }
    
    // 生成报告
    generate_csv_report("lightoladb_performance.csv", test_names, test_results);
    
    std::cout << "\n性能比较测试完成！" << std::endl;
    return 0;
}