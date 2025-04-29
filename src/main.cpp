#include <iostream>
#include <stdexcept>
#include "lightoladb/core/database.h"

int main() {
    try {
        // 创建数据库实例
        lightoladb::Database db;
        
        std::cout << "LightOLAP Database - A Lightweight OLAP Database Engine" << std::endl;
        std::cout << "Version: 0.1.0" << std::endl;
        std::cout << std::endl;
        
        // 运行交互式终端
        db.runInteractiveTerminal();
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown error occurred" << std::endl;
        return 1;
    }
}