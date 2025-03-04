import os
import asyncio
import pandas as pd
import psycopg2
import pytz
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from loguru import logger
from typing import List, Dict, Any
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

import sys

# 配置日志输出
logger.remove()  # 移除默认的日志处理器
logger.add(sys.stderr, level="INFO")  # 添加控制台输出

# 设置日志文件路径
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
logger.add(os.path.join(log_dir, "etl.log"), rotation="500 MB", level="INFO", encoding="utf-8")  # 添加文件输出

class CSVProcessor:
    def __init__(self):
        self.db_params = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }
        
    async def process_file(self, file_path: str) -> pd.DataFrame:
        """异步处理单个CSV文件"""
        try:
            start_time = time.time()
            
            # 使用pandas读取CSV文件
            extract_start = time.time()
            df = pd.read_csv(file_path, dtype_backend='pyarrow')
            extract_time = time.time() - extract_start
            initial_records = len(df)
            logger.info(f"开始处理文件 {os.path.basename(file_path)}，初始记录数: {initial_records}")
            
            # 数据转换处理
            transform_start = time.time()
            df = await self.transform_data(df)
            transform_time = time.time() - transform_start
            valid_records = len(df)
            logger.info(f"数据转换完成，有效记录数: {valid_records}，过滤掉 {initial_records - valid_records} 条无效记录")
            
            # 数据加载到PostgreSQL
            load_start = time.time()
            await self.load_to_postgres(df)
            load_time = time.time() - load_start
            
            total_time = time.time() - start_time
            logger.info(f"文件 {os.path.basename(file_path)} 处理完成:\n"
                       f"- 总处理时间: {total_time:.2f}秒\n"
                       f"- 数据提取时间: {extract_time:.2f}秒\n"
                       f"- 数据转换时间: {transform_time:.2f}秒\n"
                       f"- 数据加载时间: {load_time:.2f}秒\n"
                       f"- 成功导入记录数: {valid_records}")
            
            return df
        except Exception as e:
            logger.error(f"处理文件 {file_path} 时发生错误: {str(e)}")
            raise
    
    async def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """执行所有数据转换操作"""
        # 去重
        df = df.drop_duplicates(subset=['user_id', 'order_id'])
        
        # 计算折扣率和订单平均价格
        df['discount_rate'] = df['discount'] / df['total_price']
        df['avg_price'] = df.groupby('user_id')['total_price'].transform('mean')
        
        # 从日志字段提取设备型号
        df['device_model'] = df['log_info'].str.extract(r'device_model[":]\s*([^,"\}]+)')
        
        # 日期格式转换
        df['order_date'] = pd.to_datetime(df['order_date'], format='mixed')
        
        # 合并地址字段
        address_columns = ['province', 'city', 'district', 'street']
        df['full_address'] = df[address_columns].fillna('').agg(' '.join, axis=1).str.strip()
        
        # 过滤有效订单
        df = df[df['order_status'].isin(['completed', 'paid'])]
        
        return df
    
    async def load_to_postgres(self, df: pd.DataFrame) -> None:
        """异步批量写入PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            
            # 准备批量插入数据
            data_tuples = [tuple(x) for x in df.to_numpy()]
            columns = ','.join(df.columns)
            values = ','.join(['%s'] * len(df.columns))
            
            # 执行批量插入
            query = f"INSERT INTO orders ({columns}) VALUES ({values})"
            cur.executemany(query, data_tuples)
            
            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"数据库写入错误: {str(e)}")
            raise

class FileHandler(FileSystemEventHandler):
    def __init__(self):
        self.processor = CSVProcessor()
        self.processing_queue = asyncio.Queue()
    
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith('.csv'):
            return
        
        asyncio.create_task(self.process_new_file(event.src_path))
    
    async def process_new_file(self, file_path: str):
        await self.processing_queue.put(file_path)

async def main():
    # 设置文件监控
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, path='/mnt/nas/data', recursive=False)
    observer.start()
    
    try:
        while True:
            # 从队列中获取文件并处理
            file_path = await event_handler.processing_queue.get()
            await event_handler.processor.process_file(file_path)
            event_handler.processing_queue.task_done()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == '__main__':
    logger.add("etl.log", rotation="500 MB")
    asyncio.run(main())