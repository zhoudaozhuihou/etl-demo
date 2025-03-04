import asyncio
import time
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from loguru import logger
from src.etl.extractor import CSVExtractor
from src.etl.transformer import DataTransformer
from src.etl.loader import PostgresLoader
from src.config import FILE_MONITOR_CONFIG, LOG_CONFIG
import os
from datetime import datetime

class FileHandler(FileSystemEventHandler):
    def __init__(self):
        self.extractor = CSVExtractor()
        self.transformer = DataTransformer()
        self.loader = PostgresLoader()
        self.processing_queue = asyncio.Queue()
        self.processed_files = set()
        self.start_time = datetime.now()
    
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith('.csv'):
            return
        
        file_path = os.path.abspath(event.src_path)
        if file_path not in self.processed_files:
            logger.info(f"检测到新文件: {file_path}")
            self.processing_queue.put_nowait(file_path)
        else:
            logger.debug(f"文件已处理过，跳过: {file_path}")
    
    async def process_file(self, file_path: str):
        start_time = time.time()
        try:
            logger.info(f"开始处理文件: {file_path}")
            
            # 提取数据
            logger.debug(f"正在提取数据: {file_path}")
            df = await self.extractor.extract(file_path)
            if df is None:
                logger.warning(f"文件提取失败: {file_path}")
                return
            
            # 转换数据
            logger.debug(f"正在转换数据: {file_path}")
            df = await self.transformer.transform(df)
            if df is None:
                logger.warning(f"数据转换失败: {file_path}")
                return
            
            # 加载数据
            logger.debug(f"正在加载数据到数据库: {file_path}")
            await self.loader.load(df)
            
            # 记录处理成功
            self.processed_files.add(file_path)
            process_time = time.time() - start_time
            logger.success(f"文件处理完成: {file_path}, 耗时: {process_time:.2f}秒")
            
            # 统计信息
            total_files = len(self.processed_files)
            total_time = (datetime.now() - self.start_time).total_seconds()
            avg_time = total_time / total_files if total_files > 0 else 0
            logger.info(f"处理统计 - 总文件数: {total_files}, 平均处理时间: {avg_time:.2f}秒")
            
        except Exception as e:
            logger.error(f"处理文件 {file_path} 时发生错误: {str(e)}")
            logger.exception(e)

async def main():
    # 设置文件监控
    event_handler = FileHandler()
    observer = Observer()
    watch_path = os.path.abspath(FILE_MONITOR_CONFIG['watch_path'])
    logger.info(f"开始监控目录: {watch_path}")
    observer.schedule(
        event_handler,
        path=watch_path,
        recursive=FILE_MONITOR_CONFIG['recursive']
    )
    observer.start()
    
    try:
        while True:
            try:
                # 从队列中获取文件路径
                file_path = await event_handler.processing_queue.get()
                # 处理文件
                await event_handler.process_file(file_path)
                event_handler.processing_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"处理队列任务时发生错误: {str(e)}")
    except KeyboardInterrupt:
        observer.stop()
    
    observer.join()

if __name__ == '__main__':
    # 配置日志输出
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # 移除所有已存在的日志处理器
    logger.remove()
    
    # 添加控制台输出
    logger.add(
        sink=sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    
    # 添加文件输出
    logger.add(
        sink=os.path.join(log_dir, LOG_CONFIG['log_file']),
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        rotation=LOG_CONFIG['rotation'],
        encoding='utf-8',
        level="DEBUG"
    )
    
    logger.info("ETL处理器启动")
    logger.info(f"监控目录: {FILE_MONITOR_CONFIG['watch_path']}")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("ETL处理器停止")
    except Exception as e:
        logger.exception("ETL处理器发生错误")
        raise