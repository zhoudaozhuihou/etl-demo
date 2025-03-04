import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 数据库配置
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# 文件监控配置
FILE_MONITOR_CONFIG = {
    'watch_path': os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data'),
    'recursive': False,
    'patterns': ['*.csv']
}

# 日志配置
LOG_CONFIG = {
    'log_file': 'etl.log',
    'rotation': '500 MB'
}