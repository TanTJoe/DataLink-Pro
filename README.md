# 数链通 DataLink Pro V50

> **告别VLOOKUP地狱** — 一键完成多表关联、批量透视、智能导出的桌面数据处理工具

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Windows-lightgrey.svg)]()

---

## 为什么需要 DataLink Pro？

| 你现在的痛 | DataLink Pro 帮你做 |
|---|---|
| 每天手动VLOOKUP几十个表 | 配一次规则，以后一键跑 |
| 百万行数据Excel卡死 | Polars + Calamine引擎，百万行秒级处理 |
| 老板要的报表每月重复做 | 方案存档，下次点一下就出 |
| 多Sheet/多文件要手动合并 | 文件夹丢进去自动合并 |
| FineBI太贵，Power Query太难 | 免费、中文、零代码上手 |

## V50 新特性

- **Parquet 原生支持** — 主表/目标表直接读取 .parquet 文件，大数据量读取速度提升 10-50 倍
- **Excel 批量转 Parquet** — 一键将文件夹内的 Excel/CSV 转为 Parquet 格式（xlsx→csv→parquet 流水线）
- **Calamine 读取引擎** — Excel 读取速度比 openpyxl 快 10 倍+
- **可视化筛选构建器** — 下拉选列→选运算符→填值，支持 in list / not in / 区间 / 前缀后缀 / Excel粘贴多值
- **紧急停止按钮** — 运行中安全中断，已完成结果不丢失
- **右键快速排序** — 输出列列表右键直接设升序/降序/选中倒序
- **方案导出导入** — 导出 .json 方案文件，同事导入即可零配置复用
- **列名搜索** — 列名预览窗口和可用数据源列表均支持搜索过滤
- **窗口自由切换** — 列名预览、规则配置等窗口不再互相锁定，自由切换
- **透视表多列重命名** — 列标签模式下批量替换所有相关列名
- **复制为新任务** — 基于现有任务快速创建副本
- **日志清空** — 一键清理历史日志
- **方案覆盖确认** — 同名方案保存时提示是否覆盖

## 核心功能

- **多表智能关联** — 支持多键、多表、带前缀防重名，替代 VLOOKUP
- **文件夹批量合并** — 结构相同的 Excel 丢进文件夹，自动合并，脏表自动跳过
- **灵活计算字段** — 50+ 内置公式模板，支持条件判断、文本处理、日期计算
- **透视表引擎** — 分组聚合、交叉透视，Polars/Pandas 双引擎自动切换
- **多任务批量输出** — 一次运行输出多个 Sheet，每个独立筛选/排序/选列
- **方案一键复用** — 保存完整配置方案，换数据后一键重跑
- **Parquet 缓存加速** — 首次读取后自动缓存，重复操作秒级响应
- **OOM 智能降级** — 内存不足时自动保留已完成结果

## 快速开始

### 方式一：直接运行
```bash
pip install -r requirements.txt
python src/main.py
```

### 方式二：打包为 EXE
```bash
pip install pyinstaller
python scripts/build_exe.py
# 输出: dist/DataLinkPro.exe
```

### 方式三：直接下载 EXE
从 [Releases](../../releases) 页面下载最新版本，双击运行，无需 Python 环境。

## 使用三步走

```
Step 1: 选择主表 → 添加目标匹配规则 → 点击"加载数据池"
Step 2: 新建输出任务 → 配置筛选/计算/选列/排序
Step 3: 选择输出路径 → 点击"启动任务" → 完成!
```

详细使用说明请参见 [用户手册](docs/DataLinkPro_V50_使用手册.md)

## 项目结构

```
datalink-pro/
├── src/
│   ├── main.py              # 程序入口
│   ├── core/
│   │   ├── config.py         # 全局配置与常量
│   │   ├── io_engine.py      # 文件读写引擎 (Polars/Calamine/Pandas)
│   │   ├── cache.py          # Parquet 缓存系统
│   │   ├── models.py         # 数据模型
│   │   ├── parser.py         # 列名解析器
│   │   └── processor.py      # 核心处理引擎
│   ├── ui/
│   │   ├── theme.py          # 主题与样式
│   │   ├── app.py            # 主窗口
│   │   ├── main_frame.py     # 主界面框架
│   │   ├── dialogs.py        # 业务对话框
│   │   └── components.py     # 可复用 UI 组件
│   └── utils/
│       └── helpers.py        # 工具函数
├── assets/
│   └── icon.ico              # 应用图标
├── scripts/
│   └── build_exe.py          # PyInstaller 打包脚本
├── docs/
│   ├── DataLinkPro_V50_使用手册.md
│   └── formula_guide.md
├── requirements.txt
├── LICENSE
└── README.md
```

## 技术栈

| 组件 | 技术 |
|------|------|
| 数据引擎 | [Polars](https://pola.rs/)（高性能列式计算） |
| Excel 读取 | Calamine（Rust实现，极速）→ xlsx2csv → openpyxl（三级降级） |
| CSV 读取 | Polars scan_csv 流式读取（低内存） |
| 缓存格式 | Apache Parquet |
| GUI 框架 | Tkinter（Python 原生，零依赖） |
| 打包 | PyInstaller |

## 系统要求

- Windows 10/11（推荐 64 位）
- Python 3.10+（打包后无需 Python 环境）
- 内存：4GB+（百万级数据建议 8GB+）

## 依赖安装

```bash
pip install polars pandas numpy openpyxl xlsxwriter chardet pyarrow fastexcel pyinstaller
```

## 版本历史

| 版本 | 主要更新 |
|------|---------|
| V50 | Parquet原生支持、Calamine引擎、可视化筛选、紧急停止、方案导出导入、右键排序 |
| V48 | 架构重构（模块化拆分）、Bug修复、界面优化 |
| V47 | 文件夹批量合并、缓存机制升级、主表结构配置器、脉冲进度条 |

## License

MIT License - 自由使用、修改、分发。

## 致谢

- [Polars](https://pola.rs/) — 高性能数据引擎
- [Calamine](https://github.com/tafia/calamine) — Rust实现的Excel读取器
- [PyArrow](https://arrow.apache.org/) — Apache Arrow 的 Python 实现
