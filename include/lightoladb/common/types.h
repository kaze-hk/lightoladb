#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <variant>
#include <optional>

namespace lightoladb {

// 基础数据类型定义
enum class DataTypeID {
    Null,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    Date,
    DateTime,
    Array,
    Nullable
};

// 前向声明
class IDataType;
class Column;
class Block;

// 基础数据类型接口
class IDataType {
public:
    virtual ~IDataType() = default;
    
    virtual DataTypeID getTypeId() const = 0;
    virtual std::string getName() const = 0;
    virtual size_t getSize() const = 0;
    virtual bool isNullable() const { return false; }
    
    // 序列化/反序列化接口
    virtual void serializeBinary(const void* value, std::vector<char>& buffer) const = 0;
    virtual void deserializeBinary(void* value, const char* buffer, size_t size) const = 0;
};

// 字段值类型，使用variant存储不同类型的值
using FieldValue = std::variant<
    std::monostate,  // 用于表示NULL
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    float,
    double,
    std::string
>;

// Field类表示一个数据值
class Field {
private:
    FieldValue value;

public:
    Field() : value(std::monostate{}) {}
    
    template<typename T>
    Field(const T& v) : value(v) {}
    
    template<typename T>
    T get() const {
        return std::get<T>(value);
    }
    
    bool isNull() const {
        return std::holds_alternative<std::monostate>(value);
    }
    
    DataTypeID getTypeId() const;
    std::string toString() const;
};

} // namespace lightoladb