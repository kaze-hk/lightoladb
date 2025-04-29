#pragma once

#include <string>
#include <memory>
#include <vector>
#include <stdexcept>

namespace lightoladb {
namespace sql {
enum class AggregateFunctionType {
    NONE,
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX
};

// 列表达式，可以是普通列名或聚合函数
struct ColumnExpression {
    std::string column;         // 列名
    std::string alias;          // 别名（可选）
    AggregateFunctionType agg_type = AggregateFunctionType::NONE;  // 聚合函数类型
    
    ColumnExpression() = default;
    
    ColumnExpression(const std::string& col, AggregateFunctionType type = AggregateFunctionType::NONE,
                   const std::string& al = "") : column(col), alias(al), agg_type(type) {}
};

// SQL语句类型
enum class StatementType {
    CREATE_TABLE,
    INSERT,
    SELECT,
    DROP_TABLE,
    SHOW_TABLES,
    DESCRIBE
};

// SQL抽象语法树基类
class ASTNode {
public:
    virtual ~ASTNode() = default;
    virtual StatementType getType() const = 0;
};

// 创建表语句
class CreateTableAST : public ASTNode {
public:
    std::string table_name;
    std::vector<std::pair<std::string, std::string>> columns; // 列名和类型名
    std::string engine = "Memory"; // 默认使用内存引擎
    
    StatementType getType() const override {
        return StatementType::CREATE_TABLE;
    }
};

// 插入语句
class InsertAST : public ASTNode {
public:
    std::string table_name;
    std::vector<std::string> column_names; // 可选的列名列表
    std::vector<std::vector<std::string>> values;
    
    StatementType getType() const override {
        return StatementType::INSERT;
    }
};

// 选择语句
class SelectAST : public ASTNode {
public:
    bool select_all = false;              // 是否选择所有列 (SELECT *)
    std::vector<ColumnExpression> columns; // 要选择的列表达式列表
    std::string table_name;               // 表名
    std::string where_expr;               // WHERE条件（可选）
    std::vector<std::string> group_by_columns; // GROUP BY列（可选）
    std::vector<std::pair<std::string, bool>> order_by_columns; // ORDER BY列和方向（可选，true表示DESC）
    size_t limit = 0;                     // LIMIT子句（可选，0表示无限制）
    
    StatementType getType() const override {
        return StatementType::SELECT;
    }
};

// 删除表语句
class DropTableAST : public ASTNode {
public:
    std::string table_name;
    bool if_exists = false; // 是否有IF EXISTS子句
    
    StatementType getType() const override {
        return StatementType::DROP_TABLE;
    }
};

// 显示所有表语句
class ShowTablesAST : public ASTNode {
public:
    StatementType getType() const override {
        return StatementType::SHOW_TABLES;
    }
};

// 描述表结构语句
class DescribeAST : public ASTNode {
public:
    std::string table_name;
    
    StatementType getType() const override {
        return StatementType::DESCRIBE;
    }
};

// SQL解析器类
class SQLParser {
public:
    // 解析SQL查询字符串
    std::shared_ptr<ASTNode> parse(const std::string& query);
    
    // 解析列表达式（普通列或聚合函数）
    ColumnExpression parseColumnExpression(const std::string& expr);
    
private:
    // 解析具体的SQL语句类型
    std::shared_ptr<ASTNode> parseCreateTable(const std::string& query);
    std::shared_ptr<ASTNode> parseInsert(const std::string& query);
    std::shared_ptr<ASTNode> parseSelect(const std::string& query);
    std::shared_ptr<ASTNode> parseDropTable(const std::string& query);
    std::shared_ptr<ASTNode> parseShowTables(const std::string& query);
    std::shared_ptr<ASTNode> parseDescribe(const std::string& query);
};

}}