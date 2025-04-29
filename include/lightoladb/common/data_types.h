#pragma once

#include <memory>
#include <string>
#include <vector>
#include <cstring>
#include <sstream>
#include "lightoladb/common/types.h"

namespace lightoladb {

// 整型数据类型基类
template <typename T>
class DataTypeNumber : public IDataType {
public:
    size_t getSize() const override { return sizeof(T); }
    
    void serializeBinary(const void* value, std::vector<char>& buffer) const override {
        const T* val = static_cast<const T*>(value);
        size_t old_size = buffer.size();
        buffer.resize(old_size + sizeof(T));
        memcpy(buffer.data() + old_size, val, sizeof(T));
    }
    
    void deserializeBinary(void* value, const char* buffer, size_t size) const override {
        if (size < sizeof(T)) {
            throw std::runtime_error("Not enough data for deserialization");
        }
        memcpy(value, buffer, sizeof(T));
    }
};

// Int8 数据类型
class DataTypeInt8 : public DataTypeNumber<int8_t> {
public:
    DataTypeID getTypeId() const override { return DataTypeID::Int8; }
    std::string getName() const override { return "Int8"; }
};

// Int16 数据类型
class DataTypeInt16 : public DataTypeNumber<int16_t> {
public:
    DataTypeID getTypeId() const override { return DataTypeID::Int16; }
    std::string getName() const override { return "Int16"; }
};

// Int32 数据类型
class DataTypeInt32 : public DataTypeNumber<int32_t> {
public:
    DataTypeID getTypeId() const override { return DataTypeID::Int32; }
    std::string getName() const override { return "Int32"; }
};

// Int64 数据类型
class DataTypeInt64 : public DataTypeNumber<int64_t> {
public:
    DataTypeID getTypeId() const override { return DataTypeID::Int64; }
    std::string getName() const override { return "Int64"; }
};

// UInt8 数据类型
class DataTypeUInt8 : public DataTypeNumber<uint8_t> {
public:
    DataTypeID getTypeId() const override { return DataTypeID::UInt8; }
    std::string getName() const override { return "UInt8"; }
};

// UInt16 数据类型
class DataTypeUInt16 : public DataTypeNumber<uint16_t> {
public:
    DataTypeID getTypeId() const override { return DataTypeID::UInt16; }
    std::string getName() const override { return "UInt16"; }
};

// UInt32 数据类型
class DataTypeUInt32 : public DataTypeNumber<uint32_t> {
public:
    DataTypeID getTypeId() const override { return DataTypeID::UInt32; }
    std::string getName() const override { return "UInt32"; }
};

// UInt64 数据类型
class DataTypeUInt64 : public DataTypeNumber<uint64_t> {
public:
    DataTypeID getTypeId() const override { return DataTypeID::UInt64; }
    std::string getName() const override { return "UInt64"; }
};

// Float32 数据类型
class DataTypeFloat32 : public DataTypeNumber<float> {
public:
    DataTypeID getTypeId() const override { return DataTypeID::Float32; }
    std::string getName() const override { return "Float32"; }
};

// Float64 数据类型
class DataTypeFloat64 : public DataTypeNumber<double> {
public:
    DataTypeID getTypeId() const override { return DataTypeID::Float64; }
    std::string getName() const override { return "Float64"; }
};

// 字符串数据类型
class DataTypeString : public IDataType {
public:
    DataTypeID getTypeId() const override { return DataTypeID::String; }
    std::string getName() const override { return "String"; }
    size_t getSize() const override { return sizeof(std::string); }
    
    void serializeBinary(const void* value, std::vector<char>& buffer) const override {
        const std::string* str = static_cast<const std::string*>(value);
        uint32_t size = static_cast<uint32_t>(str->size());
        
        size_t old_size = buffer.size();
        buffer.resize(old_size + sizeof(uint32_t) + size);
        
        // 写入字符串长度
        memcpy(buffer.data() + old_size, &size, sizeof(uint32_t));
        // 写入字符串内容
        memcpy(buffer.data() + old_size + sizeof(uint32_t), str->data(), size);
    }
    
    void deserializeBinary(void* value, const char* buffer, size_t size) const override {
        if (size < sizeof(uint32_t)) {
            throw std::runtime_error("Not enough data for deserialization");
        }
        
        uint32_t str_size;
        memcpy(&str_size, buffer, sizeof(uint32_t));
        
        if (size < sizeof(uint32_t) + str_size) {
            throw std::runtime_error("Not enough data for deserialization");
        }
        
        std::string* str = static_cast<std::string*>(value);
        str->assign(buffer + sizeof(uint32_t), str_size);
    }
};

// 可空包装类型
class DataTypeNullable : public IDataType {
private:
    std::shared_ptr<const IDataType> nested_type;
    
public:
    explicit DataTypeNullable(std::shared_ptr<const IDataType> nested)
        : nested_type(nested) {}
    
    DataTypeID getTypeId() const override { return DataTypeID::Nullable; }
    
    std::string getName() const override {
        return "Nullable(" + nested_type->getName() + ")";
    }
    
    size_t getSize() const override {
        return 1 + nested_type->getSize(); // 1 byte for null flag
    }
    
    bool isNullable() const override { return true; }
    
    std::shared_ptr<const IDataType> getNestedType() const {
        return nested_type;
    }
    
    void serializeBinary(const void* value, std::vector<char>& buffer) const override {
        throw std::runtime_error("Not implemented");
    }
    
    void deserializeBinary(void* value, const char* buffer, size_t size) const override {
        throw std::runtime_error("Not implemented");
    }
};

// 创建数据类型的工厂函数
std::shared_ptr<IDataType> createDataType(const std::string& type_name);

} // namespace lightoladb