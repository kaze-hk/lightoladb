#include "lightoladb/sql/executor.h"
#include "lightoladb/common/data_types.h"
#include "lightoladb/core/column.h"
#include <algorithm>
#include <sstream>

namespace lightoladb {
namespace sql {

QueryResult SQLExecutor::execute(const std::string& query) {
    try {
        auto parser = createParser();
        std::shared_ptr<ASTNode> ast = parser->parse(query);

        switch (ast->getType()) {
            case StatementType::CREATE_TABLE:
                return executeCreateTable(static_cast<const CreateTableAST&>(*ast));
            case StatementType::INSERT:
                return executeInsert(static_cast<const InsertAST&>(*ast));
            case StatementType::SELECT:
                return executeSelect(static_cast<const SelectAST&>(*ast));
            case StatementType::DROP_TABLE:
                return executeDropTable(static_cast<const DropTableAST&>(*ast));
            case StatementType::SHOW_TABLES:
                return executeShowTables(static_cast<const ShowTablesAST&>(*ast));
            case StatementType::DESCRIBE:
                return executeDescribe(static_cast<const DescribeAST&>(*ast));
            default:
                return QueryResult(false, "Unsupported SQL statement type");
        }
    } catch (const std::exception& e) {
        return QueryResult(false, e.what());
    }
}

QueryResult SQLExecutor::executeCreateTable(const CreateTableAST& ast) {
    const std::string& table_name = ast.table_name;

    // 检查表是否已存在
    if (tables_.find(table_name) != tables_.end()) {
        return QueryResult(false, "Table '" + table_name + "' already exists");
    }

    // 创建表结构
    TableStructure structure(table_name);

    // 添加列定义
    for (const auto& col : ast.columns) {
        const std::string& col_name = col.first;
        const std::string& type_name = col.second;

        try {
            auto data_type = createDataType(type_name);
            structure.addColumn(col_name, data_type);
        } catch (const std::exception& e) {
            return QueryResult(false, std::string("Error creating column '") + col_name + "': " + e.what());
        }
    }

    // 创建存储引擎
    try {
        auto storage = createStorage(ast.engine, table_name, structure);
        tables_[table_name] = storage;
    } catch (const std::exception& e) {
        return QueryResult(false, std::string("Error creating storage engine: ") + e.what());
    }

    return QueryResult(true, "Table created successfully");
}

QueryResult SQLExecutor::executeInsert(const InsertAST& ast) {
    const std::string& table_name = ast.table_name;

    // 检查表是否存在
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return QueryResult(false, "Table '" + table_name + "' doesn't exist");
    }

    auto& storage = it->second;
    auto structure = storage->getTableStructure();

    // 准备列名列表
    std::vector<std::string> column_names;
    if (ast.column_names.empty()) {
        // 如果没有指定列名，则使用表的所有列
        for (size_t i = 0; i < structure.getColumnCount(); ++i) {
            column_names.push_back(structure.getColumnByIndex(i).name);
        }
    } else {
        column_names = ast.column_names;
        
        // 验证所有指定的列是否存在
        for (const auto& col_name : column_names) {
            if (!structure.hasColumn(col_name)) {
                return QueryResult(false, "Column '" + col_name + "' doesn't exist in table");
            }
        }
    }

    // 验证值列表
    if (ast.values.empty()) {
        return QueryResult(false, "No values to insert");
    }

    // 验证每行的列数是否匹配
    for (const auto& row : ast.values) {
        if (row.size() != column_names.size()) {
            return QueryResult(false, "Values count doesn't match columns count");
        }
    }

    // 创建数据块
    Block block;
    
    // 为每列创建列对象
    std::vector<std::shared_ptr<IColumn>> columns;
    for (const auto& col_name : column_names) {
        const auto& col_def = structure.getColumnByName(col_name);
        
        // 根据类型创建适当的列
        std::shared_ptr<IColumn> column;
        switch (col_def.type->getTypeId()) {
            case DataTypeID::Int8:
                column = std::make_shared<ColumnVector<int8_t>>(col_def.type);
                break;
            case DataTypeID::Int16:
                column = std::make_shared<ColumnVector<int16_t>>(col_def.type);
                break;
            case DataTypeID::Int32:
                column = std::make_shared<ColumnVector<int32_t>>(col_def.type);
                break;
            case DataTypeID::Int64:
                column = std::make_shared<ColumnVector<int64_t>>(col_def.type);
                break;
            case DataTypeID::UInt8:
                column = std::make_shared<ColumnVector<uint8_t>>(col_def.type);
                break;
            case DataTypeID::UInt16:
                column = std::make_shared<ColumnVector<uint16_t>>(col_def.type);
                break;
            case DataTypeID::UInt32:
                column = std::make_shared<ColumnVector<uint32_t>>(col_def.type);
                break;
            case DataTypeID::UInt64:
                column = std::make_shared<ColumnVector<uint64_t>>(col_def.type);
                break;
            case DataTypeID::Float32:
                column = std::make_shared<ColumnVector<float>>(col_def.type);
                break;
            case DataTypeID::Float64:
                column = std::make_shared<ColumnVector<double>>(col_def.type);
                break;
            case DataTypeID::String:
                column = std::make_shared<ColumnVector<std::string>>(col_def.type);
                break;
            default:
                return QueryResult(false, "Unsupported data type for column '" + col_name + "'");
        }
        
        columns.push_back(column);
        block.addColumn(col_name, column);
    }

    // 填充数据块
    for (const auto& row : ast.values) {
        for (size_t i = 0; i < row.size(); ++i) {
            const auto& value_str = row[i];
            const auto& col_name = column_names[i];
            const auto& col_def = structure.getColumnByName(col_name);
            
            // 根据列类型转换并插入值
            try {
                switch (col_def.type->getTypeId()) {
                    case DataTypeID::Int8: {
                        auto& col = static_cast<ColumnVector<int8_t>&>(*columns[i]);
                        col.insertValue(static_cast<int8_t>(std::stoi(value_str)));
                        break;
                    }
                    case DataTypeID::Int16: {
                        auto& col = static_cast<ColumnVector<int16_t>&>(*columns[i]);
                        col.insertValue(static_cast<int16_t>(std::stoi(value_str)));
                        break;
                    }
                    case DataTypeID::Int32: {
                        auto& col = static_cast<ColumnVector<int32_t>&>(*columns[i]);
                        col.insertValue(std::stoi(value_str));
                        break;
                    }
                    case DataTypeID::Int64: {
                        auto& col = static_cast<ColumnVector<int64_t>&>(*columns[i]);
                        col.insertValue(std::stoll(value_str));
                        break;
                    }
                    case DataTypeID::UInt8: {
                        auto& col = static_cast<ColumnVector<uint8_t>&>(*columns[i]);
                        col.insertValue(static_cast<uint8_t>(std::stoul(value_str)));
                        break;
                    }
                    case DataTypeID::UInt16: {
                        auto& col = static_cast<ColumnVector<uint16_t>&>(*columns[i]);
                        col.insertValue(static_cast<uint16_t>(std::stoul(value_str)));
                        break;
                    }
                    case DataTypeID::UInt32: {
                        auto& col = static_cast<ColumnVector<uint32_t>&>(*columns[i]);
                        col.insertValue(std::stoul(value_str));
                        break;
                    }
                    case DataTypeID::UInt64: {
                        auto& col = static_cast<ColumnVector<uint64_t>&>(*columns[i]);
                        col.insertValue(std::stoull(value_str));
                        break;
                    }
                    case DataTypeID::Float32: {
                        auto& col = static_cast<ColumnVector<float>&>(*columns[i]);
                        col.insertValue(std::stof(value_str));
                        break;
                    }
                    case DataTypeID::Float64: {
                        auto& col = static_cast<ColumnVector<double>&>(*columns[i]);
                        col.insertValue(std::stod(value_str));
                        break;
                    }
                    case DataTypeID::String: {
                        auto& col = static_cast<ColumnVector<std::string>&>(*columns[i]);
                        col.insertValue(value_str);
                        break;
                    }
                    default:
                        return QueryResult(false, "Unsupported data type for column '" + col_name + "'");
                }
            } catch (const std::exception& e) {
                return QueryResult(false, "Error converting value '" + value_str + "' for column '" + col_name + "': " + e.what());
            }
        }
    }

    // 插入数据
    try {
        storage->insert(block);
    } catch (const std::exception& e) {
        return QueryResult(false, std::string("Error inserting data: ") + e.what());
    }

    std::stringstream ss;
    ss << ast.values.size() << " row(s) inserted successfully";
    return QueryResult(true, ss.str());
}

QueryResult SQLExecutor::executeSelect(const SelectAST& ast) {
    const std::string& table_name = ast.table_name;

    // 检查表是否存在
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return QueryResult(false, "Table '" + table_name + "' doesn't exist");
    }

    auto& storage = it->second;
    auto structure = storage->getTableStructure();

    // 确定要查询的列和是否有聚合函数
    std::vector<std::string> column_names;
    bool has_aggregates = false;
    
    if (ast.select_all) {
        // 查询所有列
        for (size_t i = 0; i < structure.getColumnCount(); ++i) {
            column_names.push_back(structure.getColumnByIndex(i).name);
        }
    } else {
        // 查询指定列
        for (const auto& col_expr : ast.columns) {
            // 检查聚合函数
            if (col_expr.agg_type != AggregateFunctionType::NONE) {
                has_aggregates = true;
                // 对于COUNT(*)特殊处理
                if (col_expr.agg_type == AggregateFunctionType::COUNT && col_expr.column == "*") {
                    // 任何列都可以用于计数
                    column_names.push_back(structure.getColumnByIndex(0).name);
                } else {
                    // 验证列名
                    if (!structure.hasColumn(col_expr.column)) {
                        return QueryResult(false, "Column '" + col_expr.column + "' doesn't exist in table");
                    }
                    column_names.push_back(col_expr.column);
                }
            } else {
                // 常规列
                if (!structure.hasColumn(col_expr.column)) {
                    return QueryResult(false, "Column '" + col_expr.column + "' doesn't exist in table");
                }
                column_names.push_back(col_expr.column);
            }
        }
    }

    // 去重列名（为读取数据）
    std::sort(column_names.begin(), column_names.end());
    column_names.erase(std::unique(column_names.begin(), column_names.end()), column_names.end());

    // 读取数据
    std::vector<Block> blocks;
    try {
        blocks = storage->read(column_names);
    } catch (const std::exception& e) {
        return QueryResult(false, std::string("Error reading data: ") + e.what());
    }

    // 处理聚合函数
    if (has_aggregates && !blocks.empty()) {
        // 创建聚合结果
        Block agg_block;
        std::vector<std::string> result_column_names;

        for (const auto& col_expr : ast.columns) {
            std::pair<std::string, std::shared_ptr<IColumn>> agg_result;
            
            std::string col_name = col_expr.alias;
            if (col_name.empty()) {
                // 如果没有别名，使用函数表示作为列名
                if (col_expr.agg_type != AggregateFunctionType::NONE) {
                    col_name = getAggregateFunctionName(col_expr.agg_type) + "(" + col_expr.column + ")";
                } else {
                    col_name = col_expr.column;
                }
            }
            
            if (col_expr.agg_type != AggregateFunctionType::NONE) {
                // 获取列的数据类型
                DataTypeID type_id;
                if (col_expr.column == "*" && col_expr.agg_type == AggregateFunctionType::COUNT) {
                    // COUNT(*) 总是返回整数
                    type_id = DataTypeID::UInt64;
                } else {
                    const auto& col_def = structure.getColumnByName(col_expr.column);
                    type_id = col_def.type->getTypeId();
                }
                
                // 根据不同的数据类型和聚合函数类型调用相应的处理函数
                switch (type_id) {
                    case DataTypeID::Int8:
                        agg_result = computeAggregate<int8_t>(col_expr, blocks);
                        break;
                    case DataTypeID::Int16:
                        agg_result = computeAggregate<int16_t>(col_expr, blocks);
                        break;
                    case DataTypeID::Int32:
                        agg_result = computeAggregate<int32_t>(col_expr, blocks);
                        break;
                    case DataTypeID::Int64:
                        agg_result = computeAggregate<int64_t>(col_expr, blocks);
                        break;
                    case DataTypeID::UInt8:
                        agg_result = computeAggregate<uint8_t>(col_expr, blocks);
                        break;
                    case DataTypeID::UInt16:
                        agg_result = computeAggregate<uint16_t>(col_expr, blocks);
                        break;
                    case DataTypeID::UInt32:
                        agg_result = computeAggregate<uint32_t>(col_expr, blocks);
                        break;
                    case DataTypeID::UInt64:
                        agg_result = computeAggregate<uint64_t>(col_expr, blocks);
                        break;
                    case DataTypeID::Float32:
                        agg_result = computeAggregate<float>(col_expr, blocks);
                        break;
                    case DataTypeID::Float64:
                        agg_result = computeAggregate<double>(col_expr, blocks);
                        break;
                    case DataTypeID::String:
                        // 字符串只支持COUNT函数
                        if (col_expr.agg_type != AggregateFunctionType::COUNT) {
                            return QueryResult(false, "Aggregate function " + 
                                getAggregateFunctionName(col_expr.agg_type) + 
                                " not supported for String type");
                        }
                        agg_result = computeAggregate<std::string>(col_expr, blocks);
                        break;
                    default:
                        return QueryResult(false, "Unsupported data type for aggregate function");
                }
                
                agg_block.addColumn(col_name, agg_result.second);
                result_column_names.push_back(col_name);
            } else {
                // 非聚合列只有在GROUP BY中才有意义，这里暂不支持
                // 在有聚合函数的查询中，非聚合列将被忽略
                // 实际实现应该考虑GROUP BY
                continue;
            }
        }
        
        // 返回聚合结果
        return QueryResult({agg_block}, result_column_names);
    }

    // 暂时简单实现，不处理WHERE、GROUP BY、ORDER BY等子句
    // 在实际实现中，这些应该在存储引擎层面或在此处理

    // 如果是普通查询，构建结果列名
    std::vector<std::string> result_column_names;
    if (ast.select_all) {
        // 使用所有列名
        for (size_t i = 0; i < structure.getColumnCount(); ++i) {
            result_column_names.push_back(structure.getColumnByIndex(i).name);
        }
    } else {
        // 使用指定列名（或别名）
        for (const auto& col_expr : ast.columns) {
            if (!col_expr.alias.empty()) {
                result_column_names.push_back(col_expr.alias);
            } else {
                result_column_names.push_back(col_expr.column);
            }
        }
    }

    // 应用LIMIT子句
    if (ast.limit > 0 && !blocks.empty()) {
        size_t total_rows = 0;
        for (const auto& block : blocks) {
            total_rows += block.getRowCount();
        }

        if (total_rows > ast.limit) {
            // 需要限制行数
            std::vector<Block> limited_blocks;
            size_t remaining = ast.limit;

            for (const auto& block : blocks) {
                if (block.getRowCount() <= remaining) {
                    limited_blocks.push_back(block);
                    remaining -= block.getRowCount();
                } else {
                    // 需要拆分块
                    Block limited_block;
                    for (size_t i = 0; i < block.getColumnCount(); ++i) {
                        const auto& col = block.getColumnByIndex(i);
                        auto new_column = col.column->clone();
                        
                        // 仅保留指定数量的行
                        while (new_column->size() > remaining) {
                            new_column->popBack();
                        }
                        
                        limited_block.addColumn(col.name, new_column);
                    }
                    limited_blocks.push_back(limited_block);
                    break;
                }
            }

            blocks = limited_blocks;
        }
    }

    return QueryResult(blocks, result_column_names);
}

QueryResult SQLExecutor::executeDropTable(const DropTableAST& ast) {
    const std::string& table_name = ast.table_name;

    // 检查表是否存在
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        if (ast.if_exists) {
            return QueryResult(true, "Table doesn't exist, nothing to drop");
        }
        return QueryResult(false, "Table '" + table_name + "' doesn't exist");
    }

    // 删除表
    tables_.erase(it);
    return QueryResult(true, "Table '" + table_name + "' dropped successfully");
}

QueryResult SQLExecutor::executeShowTables(const ShowTablesAST& ast) {
    // 创建结果数据块
    std::vector<std::string> column_names = {"table_name"};
    std::vector<std::shared_ptr<IDataType>> types = {std::make_shared<DataTypeString>()};
    Block block = createEmptyBlock(column_names, types);

    // 填充表名
    auto& table_name_col = static_cast<ColumnVector<std::string>&>(*block.getColumnByName("table_name").column);
    for (const auto& pair : tables_) {
        table_name_col.insertValue(pair.first);
    }

    return QueryResult({block}, column_names);
}

QueryResult SQLExecutor::executeDescribe(const DescribeAST& ast) {
    const std::string& table_name = ast.table_name;

    // 检查表是否存在
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return QueryResult(false, "Table '" + table_name + "' doesn't exist");
    }

    auto& storage = it->second;
    auto structure = storage->getTableStructure();

    // 创建结果数据块
    std::vector<std::string> column_names = {"column_name", "type"};
    std::vector<std::shared_ptr<IDataType>> types = {
        std::make_shared<DataTypeString>(),
        std::make_shared<DataTypeString>()
    };
    Block block = createEmptyBlock(column_names, types);

    // 填充列信息
    auto& name_col = static_cast<ColumnVector<std::string>&>(*block.getColumnByName("column_name").column);
    auto& type_col = static_cast<ColumnVector<std::string>&>(*block.getColumnByName("type").column);

    for (size_t i = 0; i < structure.getColumnCount(); ++i) {
        const auto& col = structure.getColumnByIndex(i);
        name_col.insertValue(col.name);
        type_col.insertValue(col.type->getName());
    }

    return QueryResult({block}, column_names);
}

Block SQLExecutor::createEmptyBlock(const std::vector<std::string>& column_names, 
                                    const std::vector<std::shared_ptr<IDataType>>& types) {
    if (column_names.size() != types.size()) {
        throw std::runtime_error("Column names and types count mismatch");
    }

    Block block;
    for (size_t i = 0; i < column_names.size(); ++i) {
        std::shared_ptr<IColumn> column;
        
        switch (types[i]->getTypeId()) {
            case DataTypeID::Int8:
                column = std::make_shared<ColumnVector<int8_t>>(types[i]);
                break;
            case DataTypeID::Int16:
                column = std::make_shared<ColumnVector<int16_t>>(types[i]);
                break;
            case DataTypeID::Int32:
                column = std::make_shared<ColumnVector<int32_t>>(types[i]);
                break;
            case DataTypeID::Int64:
                column = std::make_shared<ColumnVector<int64_t>>(types[i]);
                break;
            case DataTypeID::UInt8:
                column = std::make_shared<ColumnVector<uint8_t>>(types[i]);
                break;
            case DataTypeID::UInt16:
                column = std::make_shared<ColumnVector<uint16_t>>(types[i]);
                break;
            case DataTypeID::UInt32:
                column = std::make_shared<ColumnVector<uint32_t>>(types[i]);
                break;
            case DataTypeID::UInt64:
                column = std::make_shared<ColumnVector<uint64_t>>(types[i]);
                break;
            case DataTypeID::Float32:
                column = std::make_shared<ColumnVector<float>>(types[i]);
                break;
            case DataTypeID::Float64:
                column = std::make_shared<ColumnVector<double>>(types[i]);
                break;
            case DataTypeID::String:
                column = std::make_shared<ColumnVector<std::string>>(types[i]);
                break;
            default:
                throw std::runtime_error("Unsupported data type");
        }
        
        block.addColumn(column_names[i], column);
    }
    
    return block;
}

// 获取聚合函数名称的辅助函数
std::string getAggregateFunctionName(AggregateFunctionType type) {
    switch (type) {
        case AggregateFunctionType::COUNT:
            return "COUNT";
        case AggregateFunctionType::SUM:
            return "SUM";
        case AggregateFunctionType::AVG:
            return "AVG";
        case AggregateFunctionType::MIN:
            return "MIN";
        case AggregateFunctionType::MAX:
            return "MAX";
        default:
            return "";
    }
}

// 计算聚合函数结果
template <typename T>
std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks) {
    
    std::string result_name = col_expr.alias;
    if (result_name.empty()) {
        result_name = getAggregateFunctionName(col_expr.agg_type) + "(" + col_expr.column + ")";
    }
    
    // 根据不同的聚合函数类型，选择相应的计算方法
    switch (col_expr.agg_type) {
        case AggregateFunctionType::COUNT: {
            // COUNT 总是返回 UInt64 类型
            auto result_type = std::make_shared<DataTypeUInt64>();
            auto result_column = std::make_shared<ColumnVector<uint64_t>>(result_type);
            
            uint64_t count;
            if (col_expr.column == "*") {
                // COUNT(*) 计算总行数
                count = 0;
                for (const auto& block : blocks) {
                    count += block.getRowCount();
                }
            } else {
                // COUNT(column) 计算非NULL值的数量
                count = computeCount<uint64_t>(blocks, col_expr.column);
            }
            
            // 插入结果
            static_cast<ColumnVector<uint64_t>*>(result_column.get())->insertValue(count);
            return {result_name, result_column};
        }
        
        case AggregateFunctionType::SUM: {
            // 根据列类型，选择合适的结果类型
            // 对整数类型使用更大的类型来避免溢出
            std::shared_ptr<IDataType> result_type;
            std::shared_ptr<IColumn> result_column;
            
            if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
                // 带符号整数，使用 Int64
                result_type = std::make_shared<DataTypeInt64>();
                result_column = std::make_shared<ColumnVector<int64_t>>(result_type);
                
                int64_t sum = computeSum<int64_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<int64_t>*>(result_column.get())->insertValue(sum);
            } 
            else if constexpr (std::is_integral_v<T> && std::is_unsigned_v<T>) {
                // 无符号整数，使用 UInt64
                result_type = std::make_shared<DataTypeUInt64>();
                result_column = std::make_shared<ColumnVector<uint64_t>>(result_type);
                
                uint64_t sum = computeSum<uint64_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<uint64_t>*>(result_column.get())->insertValue(sum);
            }
            else if constexpr (std::is_floating_point_v<T>) {
                // 浮点数，使用 Float64
                result_type = std::make_shared<DataTypeFloat64>();
                result_column = std::make_shared<ColumnVector<double>>(result_type);
                
                double sum = computeSum<double, T>(blocks, col_expr.column);
                static_cast<ColumnVector<double>*>(result_column.get())->insertValue(sum);
            } 
            else {
                // 不支持的类型（如字符串）
                throw std::runtime_error("SUM doesn't support this data type");
            }
            
            return {result_name, result_column};
        }
        
        case AggregateFunctionType::AVG: {
            // AVG 总是返回 Float64 类型
            auto result_type = std::make_shared<DataTypeFloat64>();
            auto result_column = std::make_shared<ColumnVector<double>>(result_type);
            
            // 计算平均值
            double avg = 0.0;
            if constexpr (std::is_arithmetic_v<T>) {
                avg = computeAvg<double, T>(blocks, col_expr.column);
            } else {
                throw std::runtime_error("AVG doesn't support this data type");
            }
            
            static_cast<ColumnVector<double>*>(result_column.get())->insertValue(avg);
            return {result_name, result_column};
        }
        
        case AggregateFunctionType::MIN: {
            std::shared_ptr<IDataType> result_type;
            std::shared_ptr<IColumn> result_column;
            
            // 保持与原始类型相同的类型
            if constexpr (std::is_same_v<T, int8_t>) {
                result_type = std::make_shared<DataTypeInt8>();
                result_column = std::make_shared<ColumnVector<int8_t>>(result_type);
                
                auto min = computeMin<int8_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<int8_t>*>(result_column.get())->insertValue(min);
            } 
            else if constexpr (std::is_same_v<T, int16_t>) {
                result_type = std::make_shared<DataTypeInt16>();
                result_column = std::make_shared<ColumnVector<int16_t>>(result_type);
                
                auto min = computeMin<int16_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<int16_t>*>(result_column.get())->insertValue(min);
            }
            else if constexpr (std::is_same_v<T, int32_t>) {
                result_type = std::make_shared<DataTypeInt32>();
                result_column = std::make_shared<ColumnVector<int32_t>>(result_type);
                
                auto min = computeMin<int32_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<int32_t>*>(result_column.get())->insertValue(min);
            }
            else if constexpr (std::is_same_v<T, int64_t>) {
                result_type = std::make_shared<DataTypeInt64>();
                result_column = std::make_shared<ColumnVector<int64_t>>(result_type);
                
                auto min = computeMin<int64_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<int64_t>*>(result_column.get())->insertValue(min);
            }
            else if constexpr (std::is_same_v<T, uint8_t>) {
                result_type = std::make_shared<DataTypeUInt8>();
                result_column = std::make_shared<ColumnVector<uint8_t>>(result_type);
                
                auto min = computeMin<uint8_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<uint8_t>*>(result_column.get())->insertValue(min);
            }
            else if constexpr (std::is_same_v<T, uint16_t>) {
                result_type = std::make_shared<DataTypeUInt16>();
                result_column = std::make_shared<ColumnVector<uint16_t>>(result_type);
                
                auto min = computeMin<uint16_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<uint16_t>*>(result_column.get())->insertValue(min);
            }
            else if constexpr (std::is_same_v<T, uint32_t>) {
                result_type = std::make_shared<DataTypeUInt32>();
                result_column = std::make_shared<ColumnVector<uint32_t>>(result_type);
                
                auto min = computeMin<uint32_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<uint32_t>*>(result_column.get())->insertValue(min);
            }
            else if constexpr (std::is_same_v<T, uint64_t>) {
                result_type = std::make_shared<DataTypeUInt64>();
                result_column = std::make_shared<ColumnVector<uint64_t>>(result_type);
                
                auto min = computeMin<uint64_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<uint64_t>*>(result_column.get())->insertValue(min);
            }
            else if constexpr (std::is_same_v<T, float>) {
                result_type = std::make_shared<DataTypeFloat32>();
                result_column = std::make_shared<ColumnVector<float>>(result_type);
                
                auto min = computeMin<float, T>(blocks, col_expr.column);
                static_cast<ColumnVector<float>*>(result_column.get())->insertValue(min);
            }
            else if constexpr (std::is_same_v<T, double>) {
                result_type = std::make_shared<DataTypeFloat64>();
                result_column = std::make_shared<ColumnVector<double>>(result_type);
                
                auto min = computeMin<double, T>(blocks, col_expr.column);
                static_cast<ColumnVector<double>*>(result_column.get())->insertValue(min);
            }
            else {
                throw std::runtime_error("MIN doesn't support this data type");
            }
            
            return {result_name, result_column};
        }
        
        case AggregateFunctionType::MAX: {
            std::shared_ptr<IDataType> result_type;
            std::shared_ptr<IColumn> result_column;
            
            // 保持与原始类型相同的类型
            if constexpr (std::is_same_v<T, int8_t>) {
                result_type = std::make_shared<DataTypeInt8>();
                result_column = std::make_shared<ColumnVector<int8_t>>(result_type);
                
                auto max = computeMax<int8_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<int8_t>*>(result_column.get())->insertValue(max);
            } 
            else if constexpr (std::is_same_v<T, int16_t>) {
                result_type = std::make_shared<DataTypeInt16>();
                result_column = std::make_shared<ColumnVector<int16_t>>(result_type);
                
                auto max = computeMax<int16_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<int16_t>*>(result_column.get())->insertValue(max);
            }
            else if constexpr (std::is_same_v<T, int32_t>) {
                result_type = std::make_shared<DataTypeInt32>();
                result_column = std::make_shared<ColumnVector<int32_t>>(result_type);
                
                auto max = computeMax<int32_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<int32_t>*>(result_column.get())->insertValue(max);
            }
            else if constexpr (std::is_same_v<T, int64_t>) {
                result_type = std::make_shared<DataTypeInt64>();
                result_column = std::make_shared<ColumnVector<int64_t>>(result_type);
                
                auto max = computeMax<int64_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<int64_t>*>(result_column.get())->insertValue(max);
            }
            else if constexpr (std::is_same_v<T, uint8_t>) {
                result_type = std::make_shared<DataTypeUInt8>();
                result_column = std::make_shared<ColumnVector<uint8_t>>(result_type);
                
                auto max = computeMax<uint8_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<uint8_t>*>(result_column.get())->insertValue(max);
            }
            else if constexpr (std::is_same_v<T, uint16_t>) {
                result_type = std::make_shared<DataTypeUInt16>();
                result_column = std::make_shared<ColumnVector<uint16_t>>(result_type);
                
                auto max = computeMax<uint16_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<uint16_t>*>(result_column.get())->insertValue(max);
            }
            else if constexpr (std::is_same_v<T, uint32_t>) {
                result_type = std::make_shared<DataTypeUInt32>();
                result_column = std::make_shared<ColumnVector<uint32_t>>(result_type);
                
                auto max = computeMax<uint32_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<uint32_t>*>(result_column.get())->insertValue(max);
            }
            else if constexpr (std::is_same_v<T, uint64_t>) {
                result_type = std::make_shared<DataTypeUInt64>();
                result_column = std::make_shared<ColumnVector<uint64_t>>(result_type);
                
                auto max = computeMax<uint64_t, T>(blocks, col_expr.column);
                static_cast<ColumnVector<uint64_t>*>(result_column.get())->insertValue(max);
            }
            else if constexpr (std::is_same_v<T, float>) {
                result_type = std::make_shared<DataTypeFloat32>();
                result_column = std::make_shared<ColumnVector<float>>(result_type);
                
                auto max = computeMax<float, T>(blocks, col_expr.column);
                static_cast<ColumnVector<float>*>(result_column.get())->insertValue(max);
            }
            else if constexpr (std::is_same_v<T, double>) {
                result_type = std::make_shared<DataTypeFloat64>();
                result_column = std::make_shared<ColumnVector<double>>(result_type);
                
                auto max = computeMax<double, T>(blocks, col_expr.column);
                static_cast<ColumnVector<double>*>(result_column.get())->insertValue(max);
            }
            else {
                throw std::runtime_error("MAX doesn't support this data type");
            }
            
            return {result_name, result_column};
        }
        
        default:
            throw std::runtime_error("Unsupported aggregate function");
    }
}

// 实现COUNT聚合函数
template <typename T>
T SQLExecutor::computeCount(const std::vector<Block>& blocks, const std::string& column_name) {
    T count = 0;
    
    for (const auto& block : blocks) {
        if (block.getColumnCount() == 0 || block.getRowCount() == 0) {
            continue;
        }
        
        const auto& column = block.getColumnByName(column_name).column;
        for (size_t i = 0; i < column->size(); ++i) {
            // 只统计非NULL值
            if (!(*column)[i].isNull()) {
                count++;
            }
        }
    }
    
    return count;
}

// 实现SUM聚合函数
template <typename ResultT, typename ValueT>
ResultT SQLExecutor::computeSum(const std::vector<Block>& blocks, const std::string& column_name) {
    ResultT sum = 0;
    
    for (const auto& block : blocks) {
        if (block.getColumnCount() == 0 || block.getRowCount() == 0) {
            continue;
        }
        
        const auto& column = block.getColumnByName(column_name).column;
        for (size_t i = 0; i < column->size(); ++i) {
            // 跳过NULL值
            if (!(*column)[i].isNull()) {
                sum += static_cast<ResultT>((*column)[i].template get<ValueT>());
            }
        }
    }
    
    return sum;
}

// 实现AVG聚合函数
template <typename ResultT, typename ValueT>
ResultT SQLExecutor::computeAvg(const std::vector<Block>& blocks, const std::string& column_name) {
    ResultT sum = 0;
    size_t count = 0;
    
    for (const auto& block : blocks) {
        if (block.getColumnCount() == 0 || block.getRowCount() == 0) {
            continue;
        }
        
        const auto& column = block.getColumnByName(column_name).column;
        for (size_t i = 0; i < column->size(); ++i) {
            // 跳过NULL值
            if (!(*column)[i].isNull()) {
                sum += static_cast<ResultT>((*column)[i].template get<ValueT>());
                count++;
            }
        }
    }
    
    return (count > 0) ? (sum / static_cast<ResultT>(count)) : 0;
}

// 实现MIN聚合函数
template <typename ResultT, typename ValueT>
ResultT SQLExecutor::computeMin(const std::vector<Block>& blocks, const std::string& column_name) {
    if (blocks.empty()) {
        throw std::runtime_error("No data to compute minimum");
    }
    
    bool found_value = false;
    ResultT min_value = 0;  // 初始值不重要，因为找到第一个非NULL值后会重新赋值
    
    for (const auto& block : blocks) {
        if (block.getColumnCount() == 0 || block.getRowCount() == 0) {
            continue;
        }
        
        const auto& column = block.getColumnByName(column_name).column;
        for (size_t i = 0; i < column->size(); ++i) {
            // 跳过NULL值
            if (!(*column)[i].isNull()) {
                ResultT current = static_cast<ResultT>((*column)[i].template get<ValueT>());
                
                if (!found_value || current < min_value) {
                    min_value = current;
                    found_value = true;
                }
            }
        }
    }
    
    if (!found_value) {
        throw std::runtime_error("No non-NULL values found for MIN calculation");
    }
    
    return min_value;
}

// 实现MAX聚合函数
template <typename ResultT, typename ValueT>
ResultT SQLExecutor::computeMax(const std::vector<Block>& blocks, const std::string& column_name) {
    if (blocks.empty()) {
        throw std::runtime_error("No data to compute maximum");
    }
    
    bool found_value = false;
    ResultT max_value = 0;  // 初始值不重要，因为找到第一个非NULL值后会重新赋值
    
    for (const auto& block : blocks) {
        if (block.getColumnCount() == 0 || block.getRowCount() == 0) {
            continue;
        }
        
        const auto& column = block.getColumnByName(column_name).column;
        for (size_t i = 0; i < column->size(); ++i) {
            // 跳过NULL值
            if (!(*column)[i].isNull()) {
                ResultT current = static_cast<ResultT>((*column)[i].template get<ValueT>());
                
                if (!found_value || current > max_value) {
                    max_value = current;
                    found_value = true;
                }
            }
        }
    }
    
    if (!found_value) {
        throw std::runtime_error("No non-NULL values found for MAX calculation");
    }
    
    return max_value;
}

// Explicit template instantiations for the types we support
// This allows the template implementations to be in the .cpp file
// COUNT
template uint64_t SQLExecutor::computeCount<uint64_t>(const std::vector<Block>& blocks, const std::string& column_name);

// SUM
template int64_t SQLExecutor::computeSum<int64_t, int8_t>(const std::vector<Block>& blocks, const std::string& column_name);
template int64_t SQLExecutor::computeSum<int64_t, int16_t>(const std::vector<Block>& blocks, const std::string& column_name);
template int64_t SQLExecutor::computeSum<int64_t, int32_t>(const std::vector<Block>& blocks, const std::string& column_name);
template int64_t SQLExecutor::computeSum<int64_t, int64_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint64_t SQLExecutor::computeSum<uint64_t, uint8_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint64_t SQLExecutor::computeSum<uint64_t, uint16_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint64_t SQLExecutor::computeSum<uint64_t, uint32_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint64_t SQLExecutor::computeSum<uint64_t, uint64_t>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeSum<double, float>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeSum<double, double>(const std::vector<Block>& blocks, const std::string& column_name);

// AVG
template double SQLExecutor::computeAvg<double, int8_t>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeAvg<double, int16_t>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeAvg<double, int32_t>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeAvg<double, int64_t>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeAvg<double, uint8_t>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeAvg<double, uint16_t>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeAvg<double, uint32_t>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeAvg<double, uint64_t>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeAvg<double, float>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeAvg<double, double>(const std::vector<Block>& blocks, const std::string& column_name);

// MIN
template int8_t SQLExecutor::computeMin<int8_t, int8_t>(const std::vector<Block>& blocks, const std::string& column_name);
template int16_t SQLExecutor::computeMin<int16_t, int16_t>(const std::vector<Block>& blocks, const std::string& column_name);
template int32_t SQLExecutor::computeMin<int32_t, int32_t>(const std::vector<Block>& blocks, const std::string& column_name);
template int64_t SQLExecutor::computeMin<int64_t, int64_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint8_t SQLExecutor::computeMin<uint8_t, uint8_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint16_t SQLExecutor::computeMin<uint16_t, uint16_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint32_t SQLExecutor::computeMin<uint32_t, uint32_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint64_t SQLExecutor::computeMin<uint64_t, uint64_t>(const std::vector<Block>& blocks, const std::string& column_name);
template float SQLExecutor::computeMin<float, float>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeMin<double, double>(const std::vector<Block>& blocks, const std::string& column_name);

// MAX
template int8_t SQLExecutor::computeMax<int8_t, int8_t>(const std::vector<Block>& blocks, const std::string& column_name);
template int16_t SQLExecutor::computeMax<int16_t, int16_t>(const std::vector<Block>& blocks, const std::string& column_name);
template int32_t SQLExecutor::computeMax<int32_t, int32_t>(const std::vector<Block>& blocks, const std::string& column_name);
template int64_t SQLExecutor::computeMax<int64_t, int64_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint8_t SQLExecutor::computeMax<uint8_t, uint8_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint16_t SQLExecutor::computeMax<uint16_t, uint16_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint32_t SQLExecutor::computeMax<uint32_t, uint32_t>(const std::vector<Block>& blocks, const std::string& column_name);
template uint64_t SQLExecutor::computeMax<uint64_t, uint64_t>(const std::vector<Block>& blocks, const std::string& column_name);
template float SQLExecutor::computeMax<float, float>(const std::vector<Block>& blocks, const std::string& column_name);
template double SQLExecutor::computeMax<double, double>(const std::vector<Block>& blocks, const std::string& column_name);

// computeAggregate instantiations
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<int8_t>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<int16_t>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<int32_t>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<int64_t>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<uint8_t>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<uint16_t>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<uint32_t>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<uint64_t>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<float>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<double>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);
template std::pair<std::string, std::shared_ptr<IColumn>> SQLExecutor::computeAggregate<std::string>(
    const ColumnExpression& col_expr, const std::vector<Block>& blocks);

} // namespace sql
} // namespace lightoladb