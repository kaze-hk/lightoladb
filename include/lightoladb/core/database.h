#pragma once

#include <string>
#include <memory>
#include "lightoladb/sql/executor.h"

namespace lightoladb {

// 数据库引擎类，作为与用户交互的主要接口
class Database {
private:
    std::shared_ptr<sql::SQLExecutor> executor_;
    
public:
    Database() : executor_(std::make_shared<sql::SQLExecutor>()) {}
    
    // 执行SQL语句
    sql::QueryResult executeQuery(const std::string& query) {
        return executor_->execute(query);
    }
    
    // 获取格式化的查询结果
    std::string formatQueryResult(const sql::QueryResult& result);
    
    // 启动一个简单的交互式SQL终端
    void runInteractiveTerminal();
};

} // namespace lightoladb