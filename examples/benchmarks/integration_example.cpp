#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include "lightoladb/core/database.h"
#include "lightoladb/sql/executor.h"

// 演示如何将 LightOLAP 集成到一个数据分析应用中
// 这个示例展示了一个典型的生产者-消费者模式，其中：
// - 多个生产者线程生成数据并插入数据库
// - 一个消费者线程执行查询分析数据

// 简单的线程安全队列，用于传递查询任务
template<typename T>
class ThreadSafeQueue {
private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cond_;
    std::atomic<bool> done_;

public:
    ThreadSafeQueue() : done_(false) {}

    void push(T item) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(item));
        cond_.notify_one();
    }

    bool try_pop(T& item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return false;
        }
        item = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    void wait_and_pop(T& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return !queue_.empty() || done_; });
        if (done_ && queue_.empty()) {
            return;
        }
        item = std::move(queue_.front());
        queue_.pop();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    void done() {
        done_ = true;
        cond_.notify_all();
    }

    bool is_done() const {
        return done_;
    }
};

// 查询任务类型
struct QueryTask {
    std::string query;
    std::string description;
};

// 生产者类 - 负责生成数据并插入数据库
class DataProducer {
private:
    lightoladb::Database& db_;
    int producer_id_;
    int max_records_;
    std::string table_name_;
    
public:
    DataProducer(lightoladb::Database& db, int id, int max_records, const std::string& table_name)
        : db_(db), producer_id_(id), max_records_(max_records), table_name_(table_name) {}
    
    void run() {
        try {
            std::cout << "生产者 #" << producer_id_ << " 开始运行" << std::endl;
            
            for (int i = 0; i < max_records_; ++i) {
                // 构建插入语句
                int record_id = producer_id_ * max_records_ + i;
                std::string name = "item_" + std::to_string(record_id);
                double value = static_cast<double>(rand()) / RAND_MAX * 1000.0;
                
                std::string insert_sql = "INSERT INTO " + table_name_ + 
                                          " (id, name, value) VALUES (" + 
                                          std::to_string(record_id) + ", '" + name + "', " +
                                          std::to_string(value) + ")";
                
                auto result = db_.executeQuery(insert_sql);
                if (!result.success()) {
                    std::cerr << "生产者 #" << producer_id_ << " 插入失败: " 
                              << result.errorMessage() << std::endl;
                }
                
                // 模拟一些处理时间
                if (i % 100 == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
            
            std::cout << "生产者 #" << producer_id_ << " 完成，已插入 " 
                      << max_records_ << " 条记录" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "生产者 #" << producer_id_ << " 异常: " << e.what() << std::endl;
        }
    }
};

// 消费者类 - 负责执行查询分析数据
class DataAnalyzer {
private:
    lightoladb::Database& db_;
    ThreadSafeQueue<QueryTask>& task_queue_;
    
public:
    DataAnalyzer(lightoladb::Database& db, ThreadSafeQueue<QueryTask>& task_queue)
        : db_(db), task_queue_(task_queue) {}
    
    void run() {
        try {
            std::cout << "分析器开始运行" << std::endl;
            
            while (!task_queue_.is_done() || !task_queue_.empty()) {
                QueryTask task;
                task_queue_.wait_and_pop(task);
                
                // 如果队列已标记结束并且为空，则退出
                if (task_queue_.is_done() && task.query.empty()) {
                    break;
                }
                
                std::cout << "\n执行查询: " << task.description << std::endl;
                auto start = std::chrono::high_resolution_clock::now();
                
                auto result = db_.executeQuery(task.query);
                
                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double, std::milli> duration = end - start;
                
                if (result.success()) {
                    std::cout << "查询结果：" << result.rowCount() << " 行，用时 " 
                              << duration.count() << " ms" << std::endl;
                    
                    // 输出查询结果摘要（仅显示前几行）
                    std::string formatted_result = db_.formatQueryResult(result);
                    std::istringstream result_stream(formatted_result);
                    std::string line;
                    int line_count = 0;
                    
                    while (std::getline(result_stream, line) && line_count < 10) {
                        std::cout << line << std::endl;
                        line_count++;
                    }
                    
                    if (result.rowCount() > 5) {
                        std::cout << "... (结果已截断)" << std::endl;
                    }
                } else {
                    std::cerr << "查询失败: " << result.errorMessage() << std::endl;
                }
            }
            
            std::cout << "分析器完成" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "分析器异常: " << e.what() << std::endl;
        }
    }
};

int main() {
    // 创建数据库实例
    lightoladb::Database db;
    
    std::cout << "LightOLAP 集成示例" << std::endl;
    std::cout << "=================" << std::endl;
    
    try {
        // 创建示例表
        std::string table_name = "sales_data";
        
        std::string create_sql = "CREATE TABLE " + table_name + " ("
                                "id UInt32, "
                                "name String, "
                                "value Float64"
                                ") ENGINE = Memory";
        
        auto create_result = db.executeQuery(create_sql);
        if (!create_result.success()) {
            std::cerr << "表创建失败: " << create_result.errorMessage() << std::endl;
            return 1;
        }
        
        std::cout << "已创建表 " << table_name << std::endl;
        
        // 设置生产者参数
        const int producer_count = 4;        // 生产者线程数
        const int records_per_producer = 500; // 每个生产者生成的记录数
        
        // 创建任务队列
        ThreadSafeQueue<QueryTask> task_queue;
        
        // 启动消费者（分析器）线程
        DataAnalyzer analyzer(db, task_queue);
        std::thread analyzer_thread(&DataAnalyzer::run, &analyzer);
        
        // 创建并启动生产者线程
        std::vector<std::unique_ptr<DataProducer>> producers;
        std::vector<std::thread> producer_threads;
        
        for (int i = 0; i < producer_count; ++i) {
            producers.push_back(std::make_unique<DataProducer>(
                db, i, records_per_producer, table_name));
            producer_threads.emplace_back(&DataProducer::run, producers[i].get());
        }
        
        // 等待所有生产者完成
        for (auto& thread : producer_threads) {
            thread.join();
        }
        
        std::cout << "\n所有生产者已完成数据插入" << std::endl;
        
        // 添加一些分析查询任务
        task_queue.push(QueryTask{
            "SELECT COUNT(*) FROM " + table_name,
            "总记录数统计"
        });
        
        task_queue.push(QueryTask{
            "SELECT MIN(value), MAX(value), AVG(value) FROM " + table_name,
            "值的统计信息"
        });
        
        task_queue.push(QueryTask{
            "SELECT * FROM " + table_name + " ORDER BY value DESC LIMIT 5",
            "按值排序的前5条记录"
        });
        
        task_queue.push(QueryTask{
            "SELECT * FROM " + table_name + " WHERE id % 100 = 0",
            "基于ID筛选的记录"
        });
        
        // 发出结束信号并等待消费者完成
        task_queue.done();
        analyzer_thread.join();
        
        std::cout << "\n集成示例运行完成！" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "发生异常: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}