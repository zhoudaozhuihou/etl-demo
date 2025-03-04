import os
import time
import asyncio
from datetime import datetime
from loguru import logger
from .generator import OrderDataGenerator
from ..config import FILE_MONITOR_CONFIG

# 配置日志输出
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
os.makedirs(log_dir, exist_ok=True)
logger.add(os.path.join(log_dir, "data_generator.log"), rotation="500 MB", level="INFO", encoding="utf-8")

class DataGeneratorScheduler:
    def __init__(self):
        self.generator = OrderDataGenerator()
        self.output_dir = FILE_MONITOR_CONFIG['watch_path']
        self.interval = 60  # 每60秒执行一次
        self.files_per_batch = 10  # 每次生成10个文件
    
    async def generate_batch_files(self):
        """生成一批CSV文件"""
        try:
            tasks = []
            for _ in range(self.files_per_batch):
                # 使用异步方式生成文件
                task = asyncio.to_thread(
                    self.generator.save_to_csv,
                    self.output_dir
                )
                tasks.append(task)
            
            # 等待所有文件生成完成
            await asyncio.gather(*tasks)
            logger.info(f"成功生成 {self.files_per_batch} 个CSV文件")
            
        except Exception as e:
            logger.error(f"批量生成文件时发生错误: {str(e)}")
    
    async def run(self):
        """启动定时任务"""
        logger.info("数据生成器定时任务已启动")
        
        while True:
            start_time = time.time()
            
            # 生成一批文件
            await self.generate_batch_files()
            
            # 计算需要等待的时间
            elapsed_time = time.time() - start_time
            wait_time = max(0, self.interval - elapsed_time)
            
            # 等待到下一个时间间隔
            await asyncio.sleep(wait_time)

async def main():
    scheduler = DataGeneratorScheduler()
    await scheduler.run()

if __name__ == '__main__':
    asyncio.run(main())