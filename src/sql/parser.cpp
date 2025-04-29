#include "lightoladb/sql/parser.h"
#include <regex>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <cctype>

namespace lightoladb {
namespace sql {

// 辅助函数：去除字符串首尾空白
std::string trim(const std::string& str) {
    auto start = std::find_if_not(str.begin(), str.end(), [](char c) { return std::isspace(c); });
    auto end = std::find_if_not(str.rbegin(), str.rend(), [](char c) { return std::isspace(c); }).base();
    return (start < end) ? std::string(start, end) : std::string();
}

// 辅助函数：将字符串转换为大写
std::string toUpper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

// 辅助函数：分割字符串
std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(trim(token));
    }
    return tokens;
}

// 解析列表达式（普通列或聚合函数）
ColumnExpression SQLParser::parseColumnExpression(const std::string& expr) {
    ColumnExpression result;
    
    // 检查是否是聚合函数
    static std::regex agg_regex(R"(([A-Za-z]+)\(\s*([*a-zA-Z0-9_\.]+)\s*\)(?:\s+AS\s+([a-zA-Z0-9_]+))?)", std::regex::icase);
    std::smatch agg_match;
    
    if (std::regex_match(expr, agg_match, agg_regex)) {
        std::string func_name = toUpper(agg_match[1]);
        result.column = agg_match[2];
        
        // 设置聚合函数类型
        if (func_name == "COUNT") {
            result.agg_type = AggregateFunctionType::COUNT;
        } else if (func_name == "SUM") {
            result.agg_type = AggregateFunctionType::SUM;
        } else if (func_name == "AVG") {
            result.agg_type = AggregateFunctionType::AVG;
        } else if (func_name == "MIN") {
            result.agg_type = AggregateFunctionType::MIN;
        } else if (func_name == "MAX") {
            result.agg_type = AggregateFunctionType::MAX;
        } else {
            throw std::runtime_error("Unsupported aggregate function: " + func_name);
        }
        
        // 处理别名
        if (agg_match[3].matched) {
            result.alias = agg_match[3];
        }
    } else {
        // 检查普通列是否有别名
        static std::regex col_regex(R"(([a-zA-Z0-9_\.]+)(?:\s+AS\s+([a-zA-Z0-9_]+))?)", std::regex::icase);
        std::smatch col_match;
        
        if (std::regex_match(expr, col_match, col_regex)) {
            result.column = col_match[1];
            if (col_match[2].matched) {
                result.alias = col_match[2];
            }
        } else {
            // 不匹配任何模式，使用原始表达式作为列名
            result.column = expr;
        }
    }
    
    return result;
}

std::shared_ptr<ASTNode> SQLParser::parse(const std::string& query) {
    std::string trimmed_query = trim(query);
    std::string upper_query = toUpper(trimmed_query);
    
    if (upper_query.find("CREATE TABLE") == 0) {
        return parseCreateTable(trimmed_query);
    } else if (upper_query.find("INSERT INTO") == 0) {
        return parseInsert(trimmed_query);
    } else if (upper_query.find("SELECT") == 0) {
        return parseSelect(trimmed_query);
    } else if (upper_query.find("DROP TABLE") == 0) {
        return parseDropTable(trimmed_query);
    } else if (upper_query.find("SHOW TABLES") == 0) {
        return parseShowTables(trimmed_query);
    } else if (upper_query.find("DESCRIBE") == 0 || upper_query.find("DESC") == 0) {
        return parseDescribe(trimmed_query);
    }
    
    throw std::runtime_error("Unsupported SQL statement");
}

std::shared_ptr<ASTNode> SQLParser::parseCreateTable(const std::string& query) {
    // 简单的正则表达式匹配CREATE TABLE语句
    std::regex create_regex(R"(CREATE\s+TABLE\s+([a-zA-Z0-9_]+)\s*\(\s*(.*?)\s*\)(?:\s+ENGINE\s*=\s*([a-zA-Z0-9_]+))?)", std::regex::icase);
    std::smatch match;
    
    if (!std::regex_search(query, match, create_regex) || match.size() < 3) {
        throw std::runtime_error("Invalid CREATE TABLE statement");
    }
    
    auto ast = std::make_shared<CreateTableAST>();
    ast->table_name = match[1];
    
    // 解析列定义
    std::string columns_str = match[2];
    std::regex column_regex(R"(([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_]+(?:\([0-9]+(?:,[0-9]+)?\))?))", std::regex::icase);
    
    std::sregex_iterator it(columns_str.begin(), columns_str.end(), column_regex);
    std::sregex_iterator end;
    
    while (it != end) {
        std::smatch column_match = *it;
        ast->columns.emplace_back(column_match[1], column_match[2]);
        ++it;
    }
    
    // 解析存储引擎
    if (match.size() > 3 && match[3].matched) {
        ast->engine = match[3];
    }
    
    return ast;
}

std::shared_ptr<ASTNode> SQLParser::parseInsert(const std::string& query) {
    // 简化版的INSERT解析
    std::regex insert_regex(R"(INSERT\s+INTO\s+([a-zA-Z0-9_]+)(?:\s*\(\s*(.*?)\s*\))?\s+VALUES\s+(.*?)(?:;)?$)", std::regex::icase);
    std::smatch match;
    
    if (!std::regex_search(query, match, insert_regex) || match.size() < 3) {
        throw std::runtime_error("Invalid INSERT statement");
    }
    
    auto ast = std::make_shared<InsertAST>();
    ast->table_name = match[1];
    
    // 解析列名（如果有）
    if (match[2].matched) {
        std::string columns_str = match[2];
        std::regex column_name_regex(R"([a-zA-Z0-9_]+)");
        
        std::sregex_iterator it(columns_str.begin(), columns_str.end(), column_name_regex);
        std::sregex_iterator end;
        
        while (it != end) {
            std::smatch column_match = *it;
            ast->column_names.push_back(column_match[0]);
            ++it;
        }
    }
    
    // 解析VALUES
    std::string values_str = match[3];
    std::regex values_regex(R"(\(\s*(.*?)\s*\))");
    
    std::sregex_iterator it(values_str.begin(), values_str.end(), values_regex);
    std::sregex_iterator end;
    
    while (it != end) {
        std::smatch values_match = *it;
        std::string row_str = values_match[1];
        
        // 处理值中的引号 - 修复处理双引号的问题
        std::vector<std::string> row_values;
        std::regex value_regex(R"('([^']*)'|\"([^\"]*)\"|([\w\.]+))");
        
        std::sregex_iterator value_it(row_str.begin(), row_str.end(), value_regex);
        std::sregex_iterator value_end;
        
        while (value_it != value_end) {
            std::smatch value_match = *value_it;
            if (value_match[1].matched) {  // 单引号
                row_values.push_back(value_match[1]);
            } else if (value_match[2].matched) {  // 双引号
                row_values.push_back(value_match[2]);
            } else {  // 无引号
                row_values.push_back(trim(value_match[3]));
            }
            ++value_it;
        }
        
        ast->values.push_back(row_values);
        ++it;
    }
    
    return ast;
}

std::shared_ptr<ASTNode> SQLParser::parseSelect(const std::string& query) {
    // 简化版的SELECT解析
    std::regex select_regex(R"(SELECT\s+(.*?)\s+FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+(.*?))?(?:\s+GROUP\s+BY\s+(.*?))?(?:\s+ORDER\s+BY\s+(.*?))?(?:\s+LIMIT\s+([0-9]+))?(?:;)?$)", std::regex::icase);
    std::smatch match;
    
    if (!std::regex_search(query, match, select_regex) || match.size() < 3) {
        throw std::runtime_error("Invalid SELECT statement");
    }
    
    auto ast = std::make_shared<SelectAST>();
    
    // 解析选择的列
    std::string columns_str = match[1];
    if (columns_str == "*") {
        ast->select_all = true;
    } else {
        // 处理可能包含逗号的列表达式
        std::vector<std::string> column_exprs;
        
        // 分割列表达式，但要注意函数括号中的逗号
        std::string current;
        int bracket_count = 0;
        
        for (char c : columns_str) {
            if (c == '(') bracket_count++;
            else if (c == ')') bracket_count--;
            
            if (c == ',' && bracket_count == 0) {
                column_exprs.push_back(trim(current));
                current.clear();
            } else {
                current += c;
            }
        }
        
        if (!current.empty()) {
            column_exprs.push_back(trim(current));
        }
        
        // 解析每个列表达式
        for (const auto& expr : column_exprs) {
            ColumnExpression col_expr = parseColumnExpression(expr);
            ast->columns.push_back(col_expr);
        }
    }
    
    // 表名
    ast->table_name = match[2];
    
    // WHERE条件（如果有）
    if (match[3].matched) {
        ast->where_expr = match[3];
    }
    
    // GROUP BY（如果有）
    if (match[4].matched) {
        std::string group_by_str = match[4];
        std::regex column_regex(R"([a-zA-Z0-9_]+)");
        
        std::sregex_iterator it(group_by_str.begin(), group_by_str.end(), column_regex);
        std::sregex_iterator end;
        
        while (it != end) {
            std::smatch column_match = *it;
            ast->group_by_columns.push_back(column_match[0]);
            ++it;
        }
    }
    
    // ORDER BY（如果有）
    if (match[5].matched) {
        std::string order_by_str = match[5];
        std::regex order_regex(R"(([a-zA-Z0-9_]+)(?:\s+(ASC|DESC))?)");
        
        std::sregex_iterator it(order_by_str.begin(), order_by_str.end(), order_regex);
        std::sregex_iterator end;
        
        while (it != end) {
            std::smatch order_match = *it;
            std::string column_name = order_match[1];
            bool desc = order_match[2].matched && toUpper(order_match[2]) == "DESC";
            ast->order_by_columns.emplace_back(column_name, desc);
            ++it;
        }
    }
    
    // LIMIT（如果有）
    if (match[6].matched) {
        ast->limit = std::stoul(match[6]);
    }
    
    return ast;
}

std::shared_ptr<ASTNode> SQLParser::parseDropTable(const std::string& query) {
    // 简化版的DROP TABLE解析
    std::regex drop_regex(R"(DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([a-zA-Z0-9_]+))", std::regex::icase);
    std::smatch match;
    
    if (!std::regex_search(query, match, drop_regex) || match.size() < 2) {
        throw std::runtime_error("Invalid DROP TABLE statement");
    }
    
    auto ast = std::make_shared<DropTableAST>();
    ast->table_name = match[1];
    ast->if_exists = query.find("IF EXISTS") != std::string::npos;
    
    return ast;
}

std::shared_ptr<ASTNode> SQLParser::parseShowTables(const std::string& query) {
    // 简单检查SHOW TABLES语法
    std::regex show_regex(R"(SHOW\s+TABLES)", std::regex::icase);
    if (!std::regex_search(query, show_regex)) {
        throw std::runtime_error("Invalid SHOW TABLES statement");
    }
    
    return std::make_shared<ShowTablesAST>();
}

std::shared_ptr<ASTNode> SQLParser::parseDescribe(const std::string& query) {
    // 简化版的DESCRIBE解析
    std::regex describe_regex(R"((DESCRIBE|DESC)\s+([a-zA-Z0-9_]+))", std::regex::icase);
    std::smatch match;
    
    if (!std::regex_search(query, match, describe_regex) || match.size() < 3) {
        throw std::runtime_error("Invalid DESCRIBE statement");
    }
    
    auto ast = std::make_shared<DescribeAST>();
    ast->table_name = match[2];
    
    return ast;
}

} // namespace sql
} // namespace lightoladb