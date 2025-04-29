#include "lightoladb/storage/table.h"
#include "lightoladb/storage/memory_engine.h"
#include <stdexcept>

namespace lightoladb {

std::shared_ptr<IStorage> createStorage(
    const std::string& engine_name,
    const std::string& table_name,
    const TableStructure& structure) {
    
    // 目前只支持内存引擎，后续可以添加更多引擎类型
    if (engine_name == "Memory") {
        return std::make_shared<MemoryStorage>(structure);
    }
    
    throw std::runtime_error("Unknown storage engine: " + engine_name);
}

} // namespace lightoladb