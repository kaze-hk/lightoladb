#pragma once

#include <vector>
#include <memory>
#include <string>
#include "lightoladb/common/types.h"

namespace lightoladb {

// 抽象列接口
class IColumn {
public:
    virtual ~IColumn() = default;
    
    virtual size_t size() const = 0;
    virtual bool empty() const = 0;
    virtual void clear() = 0;
    virtual std::shared_ptr<IColumn> clone() const = 0;
    
    virtual void insertDefault() = 0;
    virtual void popBack() = 0;
    
    virtual std::shared_ptr<const IDataType> getDataType() const = 0;
    virtual void insertFrom(const IColumn& src, size_t n) = 0;
    
    virtual Field operator[](size_t n) const = 0;
};

// 基本列实现模板类
template <typename T>
class ColumnVector : public IColumn {
private:
    std::vector<T> data;
    std::shared_ptr<const IDataType> type;
    
public:
    explicit ColumnVector(std::shared_ptr<const IDataType> type_) : type(type_) {}
    
    size_t size() const override { return data.size(); }
    bool empty() const override { return data.empty(); }
    void clear() override { data.clear(); }
    
    std::shared_ptr<IColumn> clone() const override {
        auto res = std::make_shared<ColumnVector<T>>(type);
        res->data = data;
        return res;
    }
    
    void insertDefault() override { data.push_back(T()); }
    void popBack() override { data.pop_back(); }
    
    void insertValue(const T& value) { data.push_back(value); }
    const T& getValue(size_t n) const { return data[n]; }
    
    std::shared_ptr<const IDataType> getDataType() const override { return type; }
    
    void insertFrom(const IColumn& src, size_t n) override {
        data.push_back(static_cast<const ColumnVector<T>&>(src).data[n]);
    }
    
    Field operator[](size_t n) const override { return Field(data[n]); }
    
    // 获取原始数据引用
    const std::vector<T>& getData() const { return data; }
    std::vector<T>& getData() { return data; }
};

// 字符串列的特化
template <>
class ColumnVector<std::string> : public IColumn {
private:
    std::vector<std::string> data;
    std::shared_ptr<const IDataType> type;
    
public:
    explicit ColumnVector(std::shared_ptr<const IDataType> type_) : type(type_) {}
    
    size_t size() const override { return data.size(); }
    bool empty() const override { return data.empty(); }
    void clear() override { data.clear(); }
    
    std::shared_ptr<IColumn> clone() const override {
        auto res = std::make_shared<ColumnVector<std::string>>(type);
        res->data = data;
        return res;
    }
    
    void insertDefault() override { data.emplace_back(); }
    void popBack() override { data.pop_back(); }
    
    void insertValue(const std::string& value) { data.push_back(value); }
    const std::string& getValue(size_t n) const { return data[n]; }
    
    std::shared_ptr<const IDataType> getDataType() const override { return type; }
    
    void insertFrom(const IColumn& src, size_t n) override {
        data.push_back(static_cast<const ColumnVector<std::string>&>(src).data[n]);
    }
    
    Field operator[](size_t n) const override { return Field(data[n]); }
    
    // 获取原始数据引用
    const std::vector<std::string>& getData() const { return data; }
    std::vector<std::string>& getData() { return data; }
};

// 可空列的实现
template <typename T>
class ColumnNullable : public IColumn {
private:
    std::shared_ptr<ColumnVector<T>> nested_column;
    std::shared_ptr<ColumnVector<uint8_t>> null_map; // 0 - not null, 1 - null
    std::shared_ptr<const IDataType> type;
    
public:
    ColumnNullable(
        std::shared_ptr<ColumnVector<T>> nested_,
        std::shared_ptr<ColumnVector<uint8_t>> null_map_,
        std::shared_ptr<const IDataType> type_
    ) : nested_column(nested_), null_map(null_map_), type(type_) {}
    
    size_t size() const override { return nested_column->size(); }
    bool empty() const override { return nested_column->empty(); }
    void clear() override { 
        nested_column->clear();
        null_map->clear();
    }
    
    std::shared_ptr<IColumn> clone() const override {
        return std::make_shared<ColumnNullable<T>>(
            std::static_pointer_cast<ColumnVector<T>>(nested_column->clone()),
            std::static_pointer_cast<ColumnVector<uint8_t>>(null_map->clone()),
            type
        );
    }
    
    void insertDefault() override {
        nested_column->insertDefault();
        null_map->insertValue(1); // By default insert NULL
    }
    
    void popBack() override {
        nested_column->popBack();
        null_map->popBack();
    }
    
    std::shared_ptr<const IDataType> getDataType() const override { return type; }
    
    void insertFrom(const IColumn& src, size_t n) override {
        const auto& src_nullable = static_cast<const ColumnNullable<T>&>(src);
        nested_column->insertFrom(*src_nullable.nested_column, n);
        null_map->insertFrom(*src_nullable.null_map, n);
    }
    
    Field operator[](size_t n) const override {
        if (isNull(n)) {
            return Field();
        }
        return (*nested_column)[n];
    }
    
    bool isNull(size_t n) const { return null_map->getValue(n) != 0; }
    
    void insertValue(const T& value, bool is_null) {
        nested_column->insertValue(value);
        null_map->insertValue(is_null ? 1 : 0);
    }
};

} // namespace lightoladb