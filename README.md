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


Create a modern dashboard application using React 17 and Node.js 14, implementing the following specifications:

Technical Requirements:
- React 17.x
- Node.js 14.x
- Material UI (latest compatible version)
- Tailwind CSS for custom styling
- TypeScript (optional but recommended)

Core Features:
1. Dashboard Layout:
   - Responsive navigation sidebar
   - Top header with user profile and notifications
   - Main content area with grid layout
   - Footer with relevant links

2. Interactive Components:
   - Data visualization charts (using recharts or chart.js)
   - Sortable and filterable data tables
   - CRUD forms with validation
   - Real-time data updates
   - Loading states and error handling

3. Authentication & Authorization:
   - JWT-based authentication
   - Role-based access control
   - Protected routes
   - Secure session management

4. Data Management:
   - Implement Redux or Context API for state management
   - RESTful API integration
   - Efficient data caching
   - Error boundary implementation

Design Requirements:
- Follow Material Design principles
- Ensure WCAG 2.1 accessibility compliance
- Support dark/light theme modes
- Implement responsive breakpoints for mobile, tablet, and desktop
- Use consistent spacing and typography

Performance Considerations:
- Implement code splitting
- Optimize component rendering
- Use proper loading strategies for assets
- Implement error tracking

Deliverables:
1. Functional dashboard with all specified features
2. Clean, documented code following React best practices
3. Basic unit tests for critical components
4. README with setup and deployment instructions

Please provide the implementation focusing on maintainable, scalable, and well-documented code.



