#include "lightoladb/common/types.h"
#include <sstream>
#include <iomanip>

namespace lightoladb {

// 获取字段值的类型ID
DataTypeID Field::getTypeId() const {
    if (std::holds_alternative<std::monostate>(value)) {
        return DataTypeID::Null;
    } else if (std::holds_alternative<int8_t>(value)) {
        return DataTypeID::Int8;
    } else if (std::holds_alternative<int16_t>(value)) {
        return DataTypeID::Int16;
    } else if (std::holds_alternative<int32_t>(value)) {
        return DataTypeID::Int32;
    } else if (std::holds_alternative<int64_t>(value)) {
        return DataTypeID::Int64;
    } else if (std::holds_alternative<uint8_t>(value)) {
        return DataTypeID::UInt8;
    } else if (std::holds_alternative<uint16_t>(value)) {
        return DataTypeID::UInt16;
    } else if (std::holds_alternative<uint32_t>(value)) {
        return DataTypeID::UInt32;
    } else if (std::holds_alternative<uint64_t>(value)) {
        return DataTypeID::UInt64;
    } else if (std::holds_alternative<float>(value)) {
        return DataTypeID::Float32;
    } else if (std::holds_alternative<double>(value)) {
        return DataTypeID::Float64;
    } else if (std::holds_alternative<std::string>(value)) {
        return DataTypeID::String;
    }
    
    return DataTypeID::Null;  // 默认情况
}

// 将字段值转换为字符串表示
std::string Field::toString() const {
    if (isNull()) {
        return "NULL";
    }
    
    std::stringstream ss;
    
    switch (getTypeId()) {
        case DataTypeID::Int8:
            ss << static_cast<int>(get<int8_t>());  // 避免作为字符输出
            break;
        case DataTypeID::Int16:
            ss << get<int16_t>();
            break;
        case DataTypeID::Int32:
            ss << get<int32_t>();
            break;
        case DataTypeID::Int64:
            ss << get<int64_t>();
            break;
        case DataTypeID::UInt8:
            ss << static_cast<unsigned int>(get<uint8_t>());  // 避免作为字符输出
            break;
        case DataTypeID::UInt16:
            ss << get<uint16_t>();
            break;
        case DataTypeID::UInt32:
            ss << get<uint32_t>();
            break;
        case DataTypeID::UInt64:
            ss << get<uint64_t>();
            break;
        case DataTypeID::Float32:
            ss << std::fixed << std::setprecision(6) << get<float>();
            break;
        case DataTypeID::Float64:
            ss << std::fixed << std::setprecision(6) << get<double>();
            break;
        case DataTypeID::String:
            ss << get<std::string>();
            break;
        default:
            return "???";  // 未知类型
    }
    
    return ss.str();
}

} // namespace lightoladb