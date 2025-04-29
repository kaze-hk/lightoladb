#include "lightoladb/common/data_types.h"
#include <regex>
#include <stdexcept>

namespace lightoladb {

std::shared_ptr<IDataType> createDataType(const std::string& type_name) {
    // 处理简单类型
    if (type_name == "Int8") return std::make_shared<DataTypeInt8>();
    if (type_name == "Int16") return std::make_shared<DataTypeInt16>();
    if (type_name == "Int32") return std::make_shared<DataTypeInt32>();
    if (type_name == "Int64") return std::make_shared<DataTypeInt64>();
    if (type_name == "UInt8") return std::make_shared<DataTypeUInt8>();
    if (type_name == "UInt16") return std::make_shared<DataTypeUInt16>();
    if (type_name == "UInt32") return std::make_shared<DataTypeUInt32>();
    if (type_name == "UInt64") return std::make_shared<DataTypeUInt64>();
    if (type_name == "Float32") return std::make_shared<DataTypeFloat32>();
    if (type_name == "Float64") return std::make_shared<DataTypeFloat64>();
    if (type_name == "String") return std::make_shared<DataTypeString>();
    
    // 处理Nullable类型
    std::regex nullable_regex(R"(^Nullable\((.+)\)$)");
    std::smatch match;
    
    if (std::regex_match(type_name, match, nullable_regex)) {
        if (match.size() == 2) {
            std::string nested_type_name = match[1];
            auto nested_type = createDataType(nested_type_name);
            return std::make_shared<DataTypeNullable>(nested_type);
        }
    }
    
    throw std::runtime_error("Unknown data type: " + type_name);
}

} // namespace lightoladb