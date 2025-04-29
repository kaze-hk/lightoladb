#pragma once

#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include "lightoladb/core/column.h"

namespace lightoladb {

// 表示数据块中的一列
struct ColumnWithName {
    std::string name;
    std::shared_ptr<IColumn> column;
    
    ColumnWithName(const std::string& name_, std::shared_ptr<IColumn> column_)
        : name(name_), column(column_) {}
};

// Block类表示一个内存中的数据块，是列式存储的基本单位
class Block {
private:
    std::vector<ColumnWithName> columns;
    std::unordered_map<std::string, size_t> column_indices;
    
public:
    Block() = default;
    
    // 添加一列
    void addColumn(const std::string& name, std::shared_ptr<IColumn> column) {
        column_indices[name] = columns.size();
        columns.emplace_back(name, column);
    }
    
    // 获取列数
    size_t getColumnCount() const {
        return columns.size();
    }
    
    // 获取列
    const ColumnWithName& getColumnByIndex(size_t idx) const {
        return columns[idx];
    }
    
    // 根据名称获取列的索引
    size_t getColumnIndex(const std::string& name) const {
        auto it = column_indices.find(name);
        if (it == column_indices.end()) {
            throw std::runtime_error("Column '" + name + "' not found in block");
        }
        return it->second;
    }
    
    // 根据名称获取列
    const ColumnWithName& getColumnByName(const std::string& name) const {
        return columns[getColumnIndex(name)];
    }
    
    // 获取行数（所有列的长度应该相同）
    size_t getRowCount() const {
        if (columns.empty()) {
            return 0;
        }
        return columns[0].column->size();
    }
    
    // 清除所有数据
    void clear() {
        columns.clear();
        column_indices.clear();
    }
    
    // 检查数据块是否有效（所有列的长度相同）
    bool isValid() const {
        if (columns.empty()) {
            return true;
        }
        
        size_t rows = columns[0].column->size();
        for (size_t i = 1; i < columns.size(); ++i) {
            if (columns[i].column->size() != rows) {
                return false;
            }
        }
        return true;
    }
};

} // namespace lightoladb