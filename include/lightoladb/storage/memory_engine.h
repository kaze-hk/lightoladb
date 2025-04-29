#pragma once

#include <mutex>
#include <vector>
#include "lightoladb/storage/table.h"

namespace lightoladb {

// 内存存储引擎实现
class MemoryStorage : public IStorage {
private:
    TableStructure structure;
    std::vector<Block> blocks;
    mutable std::mutex mutex; // 保护并发访问
    
public:
    explicit MemoryStorage(const TableStructure& table_structure)
        : structure(table_structure) {}
    
    std::string getName() const override {
        return "Memory";
    }
    
    TableStructure getTableStructure() const override {
        return structure;
    }
    
    void insert(const Block& block) override {
        std::lock_guard<std::mutex> lock(mutex);
        
        // 验证块与表结构是否匹配
        if (!validateBlock(block)) {
            throw std::runtime_error("Block structure doesn't match table structure");
        }
        
        blocks.push_back(block);
    }
    
    std::vector<Block> readAll() override {
        std::lock_guard<std::mutex> lock(mutex);
        return blocks;
    }
    
    std::vector<Block> read(const std::vector<std::string>& column_names) override {
        std::lock_guard<std::mutex> lock(mutex);
        
        // 验证请求的列名都存在
        for (const auto& col_name : column_names) {
            if (!structure.hasColumn(col_name)) {
                throw std::runtime_error("Column '" + col_name + "' not found in table");
            }
        }
        
        // 如果没有数据，返回空
        if (blocks.empty()) {
            return {};
        }
        
        std::vector<Block> result;
        
        // 对每个数据块，提取请求的列
        for (const auto& block : blocks) {
            Block new_block;
            
            for (const auto& col_name : column_names) {
                const auto& col = block.getColumnByName(col_name);
                new_block.addColumn(col_name, col.column);
            }
            
            result.push_back(new_block);
        }
        
        return result;
    }
    
private:
    // 验证块与表结构是否匹配
    bool validateBlock(const Block& block) const {
        // 首先检查列数量是否匹配
        if (block.getColumnCount() != structure.getColumnCount()) {
            return false;
        }
        
        // 然后检查每列的名称和类型
        for (size_t i = 0; i < block.getColumnCount(); ++i) {
            const auto& block_column = block.getColumnByIndex(i);
            const auto& table_column = structure.getColumnByName(block_column.name);
            
            // 检查列名是否存在
            if (!structure.hasColumn(block_column.name)) {
                return false;
            }
            
            // 检查数据类型是否匹配
            if (block_column.column->getDataType()->getTypeId() != table_column.type->getTypeId()) {
                return false;
            }
        }
        
        return true;
    }
};

} // namespace lightoladb