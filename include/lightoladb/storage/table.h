#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include "lightoladb/common/data_types.h"
#include "lightoladb/core/block.h"

namespace lightoladb {

// 表结构中的列定义
struct ColumnDefinition {
    std::string name;
    std::shared_ptr<IDataType> type;
    
    ColumnDefinition(const std::string& name_, std::shared_ptr<IDataType> type_)
        : name(name_), type(type_) {}
};

// 表结构定义
class TableStructure {
private:
    std::string name;
    std::vector<ColumnDefinition> columns;
    std::unordered_map<std::string, size_t> column_indices;
    
public:
    explicit TableStructure(const std::string& table_name) : name(table_name) {}
    
    void addColumn(const std::string& name, std::shared_ptr<IDataType> type) {
        column_indices[name] = columns.size();
        columns.emplace_back(name, type);
    }
    
    const std::string& getTableName() const { return name; }
    size_t getColumnCount() const { return columns.size(); }
    
    const ColumnDefinition& getColumnByIndex(size_t idx) const {
        return columns[idx];
    }
    
    size_t getColumnIndex(const std::string& name) const {
        auto it = column_indices.find(name);
        if (it == column_indices.end()) {
            throw std::runtime_error("Column '" + name + "' not found in table structure");
        }
        return it->second;
    }
    
    const ColumnDefinition& getColumnByName(const std::string& name) const {
        return columns[getColumnIndex(name)];
    }
    
    bool hasColumn(const std::string& name) const {
        return column_indices.find(name) != column_indices.end();
    }
};

// 存储引擎接口
class IStorage {
public:
    virtual ~IStorage() = default;
    
    // 获取存储引擎名称
    virtual std::string getName() const = 0;
    
    // 获取表结构
    virtual TableStructure getTableStructure() const = 0;
    
    // 插入数据块
    virtual void insert(const Block& block) = 0;
    
    // 读取全表数据
    virtual std::vector<Block> readAll() = 0;
    
    // 读取指定列的数据
    virtual std::vector<Block> read(const std::vector<std::string>& column_names) = 0;
};

// 创建存储引擎的工厂函数
std::shared_ptr<IStorage> createStorage(
    const std::string& engine_name,
    const std::string& table_name,
    const TableStructure& structure);

} // namespace lightoladb