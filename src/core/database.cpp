#include "lightoladb/core/database.h"
#include <iostream>
#include <iomanip>
#include <sstream>
#include "lightoladb/core/column.h"

namespace lightoladb {

// 格式化查询结果为表格
std::string Database::formatQueryResult(const sql::QueryResult& result) {
    std::stringstream ss;
    
    if (!result.success()) {
        ss << "Error: " << result.errorMessage() << std::endl;
        return ss.str();
    }
    
    // 如果结果中没有数据，但有成功消息
    if (result.blocks().empty() || result.columnNames().empty()) {
        ss << "OK: " << result.errorMessage() << std::endl;
        return ss.str();
    }
    
    // 获取列宽度
    const auto& column_names = result.columnNames();
    std::vector<size_t> column_widths(column_names.size(), 0);
    
    // 初始化列宽为列名长度
    for (size_t i = 0; i < column_names.size(); ++i) {
        column_widths[i] = column_names[i].length() + 2;  // 加2为左右边距
    }
    
    // 遍历所有块和行，找出每列的最大宽度
    for (const auto& block : result.blocks()) {
        if (block.getColumnCount() != column_names.size()) {
            ss << "Warning: Column count mismatch" << std::endl;
            continue;
        }
        
        for (size_t row = 0; row < block.getRowCount(); ++row) {
            for (size_t col = 0; col < block.getColumnCount(); ++col) {
                const auto& column = block.getColumnByIndex(col).column;
                const auto& value = (*column)[row];
                
                // 获取值的字符串表示
                std::string value_str;
                if (value.isNull()) {
                    value_str = "NULL";
                } else {
                    std::stringstream value_ss;
                    
                    switch (column->getDataType()->getTypeId()) {
                        case DataTypeID::Int8:
                            value_ss << static_cast<int>(value.get<int8_t>());
                            break;
                        case DataTypeID::Int16:
                            value_ss << value.get<int16_t>();
                            break;
                        case DataTypeID::Int32:
                            value_ss << value.get<int32_t>();
                            break;
                        case DataTypeID::Int64:
                            value_ss << value.get<int64_t>();
                            break;
                        case DataTypeID::UInt8:
                            value_ss << static_cast<unsigned int>(value.get<uint8_t>());
                            break;
                        case DataTypeID::UInt16:
                            value_ss << value.get<uint16_t>();
                            break;
                        case DataTypeID::UInt32:
                            value_ss << value.get<uint32_t>();
                            break;
                        case DataTypeID::UInt64:
                            value_ss << value.get<uint64_t>();
                            break;
                        case DataTypeID::Float32:
                            value_ss << std::fixed << std::setprecision(6) << value.get<float>();
                            break;
                        case DataTypeID::Float64:
                            value_ss << std::fixed << std::setprecision(6) << value.get<double>();
                            break;
                        case DataTypeID::String:
                            value_ss << value.get<std::string>();
                            break;
                        default:
                            value_ss << "?";
                            break;
                    }
                    
                    value_str = value_ss.str();
                }
                
                // 更新列宽度
                column_widths[col] = std::max(column_widths[col], value_str.length() + 2);
            }
        }
    }
    
    // 计算总宽度（列宽度之和加上分隔符）
    size_t total_width = 1;  // 左边框
    for (size_t width : column_widths) {
        total_width += width + 1;  // 列宽度加上右边框
    }
    
    // 打印表头
    ss << std::string(total_width, '-') << std::endl;
    
    ss << "|";
    for (size_t i = 0; i < column_names.size(); ++i) {
        ss << " " << std::setw(column_widths[i] - 2) << std::left << column_names[i] << " |";
    }
    ss << std::endl;
    
    ss << std::string(total_width, '-') << std::endl;
    
    // 打印数据行
    for (const auto& block : result.blocks()) {
        for (size_t row = 0; row < block.getRowCount(); ++row) {
            ss << "|";
            
            for (size_t col = 0; col < block.getColumnCount(); ++col) {
                const auto& column = block.getColumnByIndex(col).column;
                const auto& value = (*column)[row];
                
                ss << " ";
                
                if (value.isNull()) {
                    ss << std::setw(column_widths[col] - 2) << std::left << "NULL";
                } else {
                    switch (column->getDataType()->getTypeId()) {
                        case DataTypeID::Int8:
                            ss << std::setw(column_widths[col] - 2) << std::left << 
                               static_cast<int>(value.get<int8_t>());
                            break;
                        case DataTypeID::Int16:
                            ss << std::setw(column_widths[col] - 2) << std::left << 
                               value.get<int16_t>();
                            break;
                        case DataTypeID::Int32:
                            ss << std::setw(column_widths[col] - 2) << std::left << 
                               value.get<int32_t>();
                            break;
                        case DataTypeID::Int64:
                            ss << std::setw(column_widths[col] - 2) << std::left << 
                               value.get<int64_t>();
                            break;
                        case DataTypeID::UInt8:
                            ss << std::setw(column_widths[col] - 2) << std::left << 
                               static_cast<unsigned int>(value.get<uint8_t>());
                            break;
                        case DataTypeID::UInt16:
                            ss << std::setw(column_widths[col] - 2) << std::left << 
                               value.get<uint16_t>();
                            break;
                        case DataTypeID::UInt32:
                            ss << std::setw(column_widths[col] - 2) << std::left << 
                               value.get<uint32_t>();
                            break;
                        case DataTypeID::UInt64:
                            ss << std::setw(column_widths[col] - 2) << std::left << 
                               value.get<uint64_t>();
                            break;
                        case DataTypeID::Float32:
                            ss << std::fixed << std::setprecision(6) << std::setw(column_widths[col] - 2) << 
                               std::left << value.get<float>();
                            break;
                        case DataTypeID::Float64:
                            ss << std::fixed << std::setprecision(6) << std::setw(column_widths[col] - 2) << 
                               std::left << value.get<double>();
                            break;
                        case DataTypeID::String:
                            ss << std::setw(column_widths[col] - 2) << std::left << 
                               value.get<std::string>();
                            break;
                        default:
                            ss << std::setw(column_widths[col] - 2) << std::left << "?";
                            break;
                    }
                }
                
                ss << " |";
            }
            
            ss << std::endl;
        }
    }
    
    ss << std::string(total_width, '-') << std::endl;
    
    // 打印行数统计
    ss << result.rowCount() << " row(s) in set" << std::endl;
    
    return ss.str();
}

// 实现交互式SQL终端
void Database::runInteractiveTerminal() {
    std::string query;
    
    std::cout << "LightOLAP Database Terminal" << std::endl;
    std::cout << "Enter SQL queries, or 'exit' to quit." << std::endl;
    
    while (true) {
        std::cout << "\nlightoladb> ";
        
        // 读取一行输入
        std::getline(std::cin, query);
        
        // 去除首尾空白
        query.erase(0, query.find_first_not_of(" \t\n\r"));
        query.erase(query.find_last_not_of(" \t\n\r") + 1);
        
        // 检查是否退出
        if (query == "exit" || query == "quit") {
            std::cout << "Bye!" << std::endl;
            break;
        }
        
        // 忽略空查询
        if (query.empty()) {
            continue;
        }
        
        // 执行查询并打印结果
        auto result = executeQuery(query);
        std::cout << formatQueryResult(result) << std::endl;
    }
}

} // namespace lightoladb