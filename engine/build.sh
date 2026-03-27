#!/bin/bash
# QuantEngine 一键构建脚本
# 依赖：cmake >= 3.20, conan >= 2.0, gcc >= 12 或 clang >= 15

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── 激活 Python 虚拟环境（conan 装在里面）──────────────────
VENV_DIR="$SCRIPT_DIR/../.venv"
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
fi

# ── 把 Conan 配置目录指向项目内（避免 ~/.conan2 权限问题）──
export CONAN_HOME="$SCRIPT_DIR/.conan2"
mkdir -p "$CONAN_HOME"

# ── 检查必要工具 ────────────────────────────────────────────
for cmd in conan cmake; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: '$cmd' not found. Install it first."
        echo "  conan:  pip install conan"
        echo "  cmake:  sudo apt install cmake"
        exit 1
    fi
done

echo "conan: $(conan --version 2>&1 | head -1)"
echo "cmake: $(cmake --version | head -1)"

BUILD_TYPE=${1:-Release}   # Release 或 Debug
BUILD_DIR="build/${BUILD_TYPE}"

echo "=== QuantEngine Build: ${BUILD_TYPE} ==="

# ────────────────────────────────────────────────────────────
# Step 1: Conan 安装依赖
# ────────────────────────────────────────────────────────────
echo "[1/4] Installing Conan dependencies..."
conan install . \
    --output-folder="${BUILD_DIR}" \
    --build=missing \
    --settings=build_type="${BUILD_TYPE}"

# ────────────────────────────────────────────────────────────
# Step 2: CMake 配置
# ────────────────────────────────────────────────────────────
echo "[2/4] CMake configure..."
cmake -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DCMAKE_TOOLCHAIN_FILE="${BUILD_DIR}/conan_toolchain.cmake" \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -S .

# 软链 compile_commands.json 到根目录（供 clangd 使用）
ln -sf "${BUILD_DIR}/compile_commands.json" compile_commands.json

# ────────────────────────────────────────────────────────────
# Step 3: 编译
# ────────────────────────────────────────────────────────────
echo "[3/4] Building..."
cmake --build "${BUILD_DIR}" --parallel "$(nproc)"

# ────────────────────────────────────────────────────────────
# Step 4: 运行测试
# ────────────────────────────────────────────────────────────
echo "[4/4] Running tests..."
cd "${BUILD_DIR}"
ctest --output-on-failure --parallel "$(nproc)"
cd ../..

echo ""
echo "=== Build SUCCESS ==="
echo "Binary:     ${BUILD_DIR}/quant_engine"
echo "Tests:      ${BUILD_DIR}/tests/quant_tests"
echo "Benchmarks: ${BUILD_DIR}/benchmarks/quant_benchmarks"
echo ""
echo "Run engine:     ./${BUILD_DIR}/quant_engine config/engine.yaml"
echo "Run benchmarks: ./${BUILD_DIR}/benchmarks/quant_benchmarks"
