#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include "lightoladb/sql/parser.h"
#include "lightoladb/storage/table.h"
#include "lightoladb/core/block.h"

namespace lightoladb {
namespace sql {

// 获取聚合函数名称的辅助函数
std::string getAggregateFunctionName(AggregateFunctionType type);

// 查询执行结果类
class QueryResult {
private:
    bool success_;
    std::string error_message_;
    std::vector<Block> blocks_;
    std::vector<std::string> column_names_;

public:
    QueryResult(bool success, const std::string& error_message = "")
        : success_(success), error_message_(error_message) {}

    QueryResult(const std::vector<Block>& blocks, const std::vector<std::string>& column_names)
        : success_(true), blocks_(blocks), column_names_(column_names) {}

    bool success() const { return success_; }
    const std::string& errorMessage() const { return error_message_; }
    const std::vector<Block>& blocks() const { return blocks_; }
    const std::vector<std::string>& columnNames() const { return column_names_; }

    // 获取结果的行数
    size_t rowCount() const {
        size_t count = 0;
        for (const auto& block : blocks_) {
            count += block.getRowCount();
        }
        return count;
    }

    // 获取结果的列数
    size_t columnCount() const {
        return column_names_.size();
    }
};

// SQL执行器类
class SQLExecutor {
private:
    // 存储表的映射
    std::unordered_map<std::string, std::shared_ptr<IStorage>> tables_;

public:
    // 执行SQL语句
    QueryResult execute(const std::string& query);

private:
    // 内部执行方法
    QueryResult executeCreateTable(const CreateTableAST& ast);
    QueryResult executeInsert(const InsertAST& ast);
    QueryResult executeSelect(const SelectAST& ast);
    QueryResult executeDropTable(const DropTableAST& ast);
    QueryResult executeShowTables(const ShowTablesAST& ast);
    QueryResult executeDescribe(const DescribeAST& ast);

    // 创建解析器
    std::shared_ptr<SQLParser> createParser() const {
        return std::make_shared<SQLParser>();
    }

    // 创建空数据块，包含指定的列
    Block createEmptyBlock(const std::vector<std::string>& column_names, 
                           const std::vector<std::shared_ptr<IDataType>>& types);
                           
    // 计算聚合函数
    template <typename T>
    std::pair<std::string, std::shared_ptr<IColumn>> computeAggregate(
        const ColumnExpression& col_expr, const std::vector<Block>& blocks);
        
    // 处理各种聚合函数
    template <typename T>
    T computeCount(const std::vector<Block>& blocks, const std::string& column_name);
    
    template <typename T, typename ValueType>
    T computeSum(const std::vector<Block>& blocks, const std::string& column_name);
    
    template <typename T, typename ValueType>
    T computeAvg(const std::vector<Block>& blocks, const std::string& column_name);
    
    template <typename T, typename ValueType>
    T computeMin(const std::vector<Block>& blocks, const std::string& column_name);
    
    template <typename T, typename ValueType>
    T computeMax(const std::vector<Block>& blocks, const std::string& column_name);
};

} // namespace sql
} // namespace lightoladb