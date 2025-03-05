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
from src.utils.file_index import FileIndexManager
import os
from datetime import datetime

# 配置日志输出
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

# 移除所有已存在的日志处理器
logger.remove()

# 添加控制台输出
logger.add(
    sink=sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="DEBUG"  # 将日志级别改为DEBUG以显示更多信息
)

# 添加文件输出
logger.add(
    sink=os.path.join(log_dir, LOG_CONFIG['log_file']),
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    rotation=LOG_CONFIG['rotation'],
    encoding='utf-8',
    level="DEBUG"
)

class FileHandler(FileSystemEventHandler):
    def __init__(self):
        self.extractor = CSVExtractor()
        self.transformer = DataTransformer()
        self.loader = PostgresLoader()
        self.processing_queue = asyncio.Queue()
        self.start_time = datetime.now()
        self.file_index = FileIndexManager()
        logger.debug("FileHandler初始化完成")
    
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith('.csv'):
            return
        
        file_path = os.path.abspath(event.src_path)
        if not self.file_index.is_file_processed(file_path):
            logger.info(f"检测到新文件: {file_path}")
            # 使用put_nowait直接添加到队列
            self.processing_queue.put_nowait(file_path)
            logger.debug(f"文件已添加到处理队列: {file_path}")
        else:
            logger.debug(f"文件已处理过，跳过: {file_path}")
    
    async def process_new_file(self, file_path: str):
        logger.debug(f"将文件添加到处理队列: {file_path}")
        await self.processing_queue.put(file_path)
    
    async def process_file(self, file_path: str):
        start_time = time.time()
        df = None
        try:
            logger.info(f"开始处理文件: {file_path}")
            logger.debug(f"文件处理开始时间: {datetime.fromtimestamp(start_time)}")
            
            # 提取数据
            extract_start = time.time()
            logger.info(f"正在提取数据: {file_path}")
            try:
                df = await self.extractor.extract(file_path)
                if df is None:
                    logger.warning(f"文件提取失败: {file_path}")
                    return
                extract_time = time.time() - extract_start
                logger.debug(f"数据提取完成，耗时: {extract_time:.2f}秒，数据行数: {len(df)}")
            except Exception as e:
                logger.error(f"数据提取过程发生错误: {str(e)}")
                return
            
            # 转换数据
            transform_start = time.time()
            logger.info(f"正在转换数据: {file_path}")
            try:
                df = await self.transformer.transform(df)
                if df is None:
                    logger.warning(f"数据转换失败: {file_path}")
                    return
                transform_time = time.time() - transform_start
                logger.debug(f"数据转换完成，耗时: {transform_time:.2f}秒，转换后数据行数: {len(df)}")
            except Exception as e:
                logger.error(f"数据转换过程发生错误: {str(e)}")
                return
            
            # 加载数据
            load_start = time.time()
            logger.info(f"正在加载数据到数据库: {file_path}")
            try:
                await self.loader.load(df)
                load_time = time.time() - load_start
                logger.debug(f"数据加载完成，耗时: {load_time:.2f}秒")
            except Exception as e:
                logger.error(f"数据加载到数据库过程发生错误: {str(e)}")
                return
            
            # 数据处理结果统计
            logger.info("数据处理结果统计:")
            logger.info(f"- 总记录数: {len(df)}")
            logger.info(f"- 订单状态分布:\n{df['order_status'].value_counts().to_string()}")
            logger.info(f"- 支付方式分布:\n{df['payment_method'].value_counts().to_string()}")
            logger.info(f"- 订单金额统计:\n{df['total_price'].describe().to_string()}")
            
            # 记录处理成功
            self.file_index.mark_file_processed(file_path)
            process_time = time.time() - start_time
            logger.success(f"文件处理完成: {file_path}")
            logger.info(f"处理详情:\n"
                      f"- 总处理时间: {process_time:.2f}秒\n"
                      f"- 数据提取时间: {extract_time:.2f}秒\n"
                      f"- 数据转换时间: {transform_time:.2f}秒\n"
                      f"- 数据加载时间: {load_time:.2f}秒")
            
            # 统计信息
            total_files = len(self.processed_files)
            total_time = (datetime.now() - self.start_time).total_seconds()
            avg_time = total_time / total_files if total_files > 0 else 0
            logger.info(f"处理统计 - 总文件数: {total_files}, 平均处理时间: {avg_time:.2f}秒")
            
        except Exception as e:
            logger.error(f"处理文件 {file_path} 时发生错误: {str(e)}")
            logger.exception(e)
            raise  # 重新抛出异常，让上层处理

async def process_queue(event_handler):
    """处理文件队列的协程"""
    logger.debug("队列处理协程启动")
    while True:
        try:
            # 从队列中获取文件路径
            logger.debug("等待队列中的新文件...")
            file_path = await event_handler.processing_queue.get()
            logger.debug(f"从队列中获取到文件: {file_path}")
            
            # 处理文件
            try:
                await event_handler.process_file(file_path)
                logger.debug(f"文件处理完成: {file_path}")
            except Exception as e:
                logger.error(f"处理文件时发生错误: {file_path}")
                logger.exception(e)
            finally:
                # 无论处理是否成功，都标记任务完成
                event_handler.processing_queue.task_done()
                logger.debug(f"队列任务已标记完成: {file_path}")
                
        except asyncio.CancelledError:
            logger.info("队列处理协程被取消")
            break
        except Exception as e:
            logger.error(f"队列处理过程中发生错误: {str(e)}")
            logger.exception(e)

async def main():
    # 设置文件监控
    event_handler = FileHandler()
    observer = Observer()
    watch_path = os.path.abspath(FILE_MONITOR_CONFIG['watch_path'])
    logger.info(f"开始监控目录: {watch_path}")
    
    # 扫描目录中的未处理文件
    for file_path in event_handler.file_index.scan_directory(watch_path):
        event_handler.processing_queue.put_nowait(file_path)
        logger.debug(f"添加未处理的文件到队列: {file_path}")
    
    observer.schedule(
        event_handler,
        path=watch_path,
        recursive=FILE_MONITOR_CONFIG['recursive']
    )
    observer.start()
    
    # 创建多个处理队列的任务
    queue_workers = []
    for _ in range(10):  # 增加并发处理任务数量到10个
        worker = asyncio.create_task(process_queue(event_handler))
        queue_workers.append(worker)
    
    try:
        # 等待直到被中断
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        # 停止所有工作任务
        for worker in queue_workers:
            worker.cancel()
        await asyncio.gather(*queue_workers, return_exceptions=True)
        observer.stop()
    
    observer.join()

if __name__ == '__main__':
    logger.info("ETL处理器启动")
    logger.info(f"监控目录: {FILE_MONITOR_CONFIG['watch_path']}")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("ETL处理器停止")
    except Exception as e:
        logger.exception("ETL处理器发生错误")
        raise