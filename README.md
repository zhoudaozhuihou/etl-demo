# 订单数据生成与ETL处理系统

这是一个用于生成模拟订单数据并进行ETL处理的系统。系统包含两个主要组件：数据生成器和ETL处理器。

## 项目结构

```
├── data/                   # 生成的订单数据文件存储目录
├── logs/                   # 日志文件目录
├── src/                    # 源代码目录
│   ├── data_generator/     # 数据生成器模块
│   │   ├── generator.py    # 数据生成核心逻辑
│   │   └── scheduler.py    # 定时调度管理
│   ├── etl/                # ETL处理模块
│   │   ├── extractor.py    # 数据提取组件
│   │   ├── transformer.py  # 数据转换组件
│   │   └── loader.py       # 数据加载组件
│   ├── config.py           # 配置管理
│   └── models.py           # 数据模型定义
├── main.py                 # 主程序入口
├── etl_processor.py        # ETL处理器入口
└── requirements.txt        # 项目依赖
```

### 核心模块说明

1. 数据生成器（Data Generator）
   - 负责生成模拟订单数据
   - 支持定时批量生成CSV文件
   - 通过调度器控制生成频率和批次

2. ETL处理器（ETL Processor）
   - 监控数据目录，实时处理新生成的文件
   - 提取（Extract）：读取CSV文件内容
   - 转换（Transform）：数据清洗和格式转换
   - 加载（Load）：将处理后的数据存入数据库

### 数据流转过程

1. 数据生成
   - 调度器定时触发数据生成
   - 生成器创建模拟订单数据
   - 将数据保存为CSV文件

2. 数据处理
   - ETL处理器监控data目录
   - 检测到新文件后启动处理流程
   - 执行数据清洗和转换
   - 批量导入数据库

## 功能特点

- 自动生成模拟订单数据
- 定时批量生成CSV文件
- 实时监控并处理新生成的文件
- 数据清洗和转换
- PostgreSQL数据库存储

## 环境要求

- Python 3.8+
- PostgreSQL 数据库

## 安装步骤

1. 克隆项目到本地

2. 安装依赖包：
   ```bash
   pip install -r requirements.txt
   ```

3. 配置环境变量
   创建 `.env` 文件，添加以下配置：
   ```env
   DB_NAME=your_database_name
   DB_USER=your_database_user
   DB_PASSWORD=your_database_password
   DB_HOST=your_database_host
   DB_PORT=your_database_port
   ```

## 运行说明

### 1. 启动数据生成器

数据生成器用于生成模拟订单数据，并将其保存为CSV文件。

```bash
# 在项目根目录下运行
python -m src.data_generator.scheduler
```

默认配置：
- 每60秒生成一批文件
- 每批生成10个CSV文件
- 文件保存在配置的监控目录中

### 2. 启动ETL处理器

ETL处理器用于监控指定目录，当有新的CSV文件生成时，自动进行处理并导入数据库。

```bash
# 在项目根目录下运行
python etl_processor.py
```

## 配置说明

### 数据生成器配置

在 `src/config.py` 中可以修改以下配置：

- `FILE_MONITOR_CONFIG['watch_path']`：CSV文件的输出目录
- `interval`：生成文件的时间间隔（秒）
- `files_per_batch`：每批生成的文件数量

### ETL处理器配置

主要通过环境变量进行配置，包括：

- 数据库连接信息
- 文件监控目录
- 日志配置

## 日志查看

- 数据生成器的日志会输出到控制台
- ETL处理器的日志保存在 `etl.log` 文件中

## 常见问题

1. 如何修改生成数据的频率？
   - 修改 `DataGeneratorScheduler` 类中的 `interval` 参数

2. 如何调整每批生成的文件数量？
   - 修改 `DataGeneratorScheduler` 类中的 `files_per_batch` 参数

3. 数据库连接失败怎么办？
   - 检查 `.env` 文件中的数据库配置是否正确
   - 确保数据库服务正在运行
   - 验证数据库用户权限

## 注意事项

- 确保监控目录具有正确的读写权限
- 定期检查日志文件大小，避免占用过多磁盘空间
- 建议在生产环境中配置适当的日志轮转策略



在 GitHub Copilot 的对话模式（Copilot Chat）中，编写 Prompt（提示词）是与 AI 交互的关键，直接影响生成代码或回答的质量。以下是编写 Prompt 时需要注意的事项，结合实用建议，帮助你更好地利用 GitHub Copilot：

编写 Prompt 的注意事项
清晰明确地描述需求
为什么：模糊的 Prompt 会导致 AI 输出无关或错误的代码。
建议：用简洁的语言说明目标，例如“写一个 Python 函数计算斐波那契数列第 n 项”比“给我写点代码”更有效。
示例：
差：帮我写个排序东西
好：用 JavaScript 写一个函数，对数字数组按升序排序
提供上下文信息
为什么：Copilot 对上下文敏感，提供相关信息能提高准确性。
建议：说明编程语言、框架、输入输出格式或使用的库。
示例：
差：写个 API 请求
好：用 Python 的 requests 库写一个 GET 请求，访问 https://api.example.com/data，返回 JSON 数据
指定期望的代码风格或约束
为什么：Copilot 默认可能生成通用代码，不一定符合你的偏好。
建议：说明代码风格（如简洁、可读性优先）或限制（如不使用某库）。
示例：
用 TypeScript 写一个类，遵循 SOLID 原则，避免使用全局变量
逐步引导复杂任务
为什么：长而复杂的 Prompt 可能让 Copilot 遗漏细节。
建议：将任务拆分为小步骤，逐步提问。
示例：
不要一次性说：写一个完整的 React 应用
而是：
用 React 创建一个函数组件，显示用户姓名
为这个组件添加一个表单，提交时更新姓名
使用具体的术语和例子
为什么：技术术语和示例能减少歧义。
建议：直接引用函数名、方法，或给出输入输出示例。
示例：
写一个函数，输入 [1, 2, 3]，输出 [1, 4, 9]（暗示平方运算）
避免过于开放或主观的问题
为什么：Copilot 在处理“最佳实践”或“应该如何”时可能给出泛泛答案。
建议：聚焦具体实现，而不是哲学讨论。
示例：
差：写代码最好的方法是什么？
好：用 Go 实现一个并发处理 100 个 HTTP 请求的程序
检查语言和语法
为什么：拼写错误或语法混乱可能误导 AI。
建议：保持 Prompt 语法正确，尤其是用英文时（Copilot 对英文更优化）。
示例：
差：Writ a funciton to sum numbers
好：Write a function to sum a list of numbers in C#
利用迭代式提问
为什么：首次生成可能不完美，迭代能逐步修正。
建议：基于 Copilot 的输出，继续提问调整或补充。
示例：
初始：写一个 JavaScript 函数过滤数组中的偶数
调整：在之前的函数中添加参数，只保留大于 10 的偶数
区分生成与解释需求
为什么：Copilot 既能生成代码也能解释，意图不清会混淆输出。
建议：明确是否需要代码、解释还是调试。
示例：
生成：写一个 SQL 查询，从 users 表中选出年龄大于 18 的记录
解释：解释这段 SQL 代码：SELECT * FROM users WHERE age > 18
注意敏感信息
为什么：Prompt 可能会上传到云端处理，避免泄露私有数据。
建议：用占位符替代敏感内容，如用 API_KEY 代替真实密钥。
实用技巧
用命令式语气：如“写一个函数”“解释这段代码”，简洁有力。
测试多个表述：如果结果不理想，换个角度重述 Prompt。
结合代码上下文：在 IDE 中直接注释+提问，效果优于单独 Chat。
中文 Prompt 可行但优化不足：若用中文，尽量简洁，避免复杂句式，必要时切换英文。
示例 Prompt
基础：用 Python 写一个函数，接受字符串参数，返回反转后的字符串
进阶：用 Java 写一个 REST API 控制器，处理 POST 请求，接收 JSON 数据并保存到 MySQL，包含异常处理
通过以上注意事项，你可以更高效地与 GitHub Copilot 交互，得到符合预期的代码或解答。如果有具体场景或 Prompt 示例需要优化，可以告诉我，我帮你进一步调整！