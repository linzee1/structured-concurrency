#!/bin/bash

# parallel-in-scope-demo 运行脚本
# 用于演示各种示例

set -e

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DEMO_DIR"

echo "=== parallel-in-scope-demo 运行脚本 ==="
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 显示帮助
show_help() {
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  basic        运行基础示例 (BasicParDemo)"
    echo "  cancel       运行取消机制示例 (CancellationDemo)"
    echo "  deadlock     运行死锁检测示例 (DeadlockDetectionDemo)"
    echo "  batch        运行批量处理示例 (BatchProcessingDemo)"
    echo "  all          运行所有示例"
    echo "  test         运行测试"
    echo "  compile      编译项目"
    echo "  clean        清理构建产物"
    echo "  help         显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 basic"
    echo "  $0 all"
    echo "  $0 test"
}

# 运行基础示例
run_basic() {
    echo -e "${GREEN}运行基础示例...${NC}"
    mvn -q exec:java -Dexec.mainClass=demo.basic.BasicParDemo
}

# 运行取消机制示例
run_cancel() {
    echo -e "${GREEN}运行取消机制示例...${NC}"
    mvn -q exec:java -Dexec.mainClass=demo.basic.CancellationDemo
}

# 运行死锁检测示例
run_deadlock() {
    echo -e "${GREEN}运行死锁检测示例...${NC}"
    mvn -q exec:java -Dexec.mainClass=demo.advanced.DeadlockDetectionDemo
}

# 运行批量处理示例
run_batch() {
    echo -e "${GREEN}运行批量处理示例...${NC}"
    mvn -q exec:java -Dexec.mainClass=demo.integration.BatchProcessingDemo
}

# 运行所有示例
run_all() {
    echo -e "${GREEN}运行所有示例...${NC}"
    echo ""
    run_basic
    echo ""
    echo "----------------------------------------"
    echo ""
    run_cancel
    echo ""
    echo "----------------------------------------"
    echo ""
    run_deadlock
    echo ""
    echo "----------------------------------------"
    echo ""
    run_batch
}

# 运行测试
run_test() {
    echo -e "${GREEN}运行测试...${NC}"
    mvn test
}

# 编译项目
compile_project() {
    echo -e "${GREEN}编译项目...${NC}"
    mvn clean compile
}

# 清理构建产物
clean_project() {
    echo -e "${GREEN}清理构建产物...${NC}"
    mvn clean
}

# 主逻辑
case "${1:-help}" in
    basic)
        run_basic
        ;;
    cancel)
        run_cancel
        ;;
    deadlock)
        run_deadlock
        ;;
    batch)
        run_batch
        ;;
    all)
        run_all
        ;;
    test)
        run_test
        ;;
    compile)
        compile_project
        ;;
    clean)
        clean_project
        ;;
    help|*)
        show_help
        ;;
esac
