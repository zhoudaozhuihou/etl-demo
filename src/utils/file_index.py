import os
import shelve
from typing import Set
from loguru import logger
from pybloom_live import BloomFilter

class FileIndexManager:
    def __init__(self, cache_file: str = 'file_index.db', expected_items: int = 100000, false_positive_rate: float = 0.001):
        self.cache_file = cache_file
        self.bloom_filter = BloomFilter(capacity=expected_items, error_rate=false_positive_rate)
        self.processed_files: Set[str] = set()
        self._load_cache()
    
    def _load_cache(self) -> None:
        """从缓存文件加载已处理的文件索引"""
        try:
            with shelve.open(self.cache_file) as db:
                if 'processed_files' in db:
                    self.processed_files = db['processed_files']
                    for file_path in self.processed_files:
                        self.bloom_filter.add(file_path)
                    logger.info(f"成功加载文件索引缓存，共 {len(self.processed_files)} 个文件记录")
        except Exception as e:
            logger.error(f"加载文件索引缓存时发生错误: {str(e)}")
            self.processed_files = set()
    
    def save_cache(self) -> None:
        """保存文件索引到缓存文件"""
        try:
            with shelve.open(self.cache_file) as db:
                db['processed_files'] = self.processed_files
            logger.info(f"成功保存文件索引缓存，共 {len(self.processed_files)} 个文件记录")
        except Exception as e:
            logger.error(f"保存文件索引缓存时发生错误: {str(e)}")
    
    def is_file_processed(self, file_path: str) -> bool:
        """检查文件是否已经处理过"""
        return file_path in self.bloom_filter
    
    def mark_file_processed(self, file_path: str) -> None:
        """标记文件为已处理"""
        self.bloom_filter.add(file_path)
        self.processed_files.add(file_path)
        self.save_cache()
    
    def scan_directory(self, directory: str) -> None:
        """扫描目录并更新文件索引"""
        try:
            for root, _, files in os.walk(directory):
                for file in files:
                    if file.endswith('.csv'):
                        file_path = os.path.abspath(os.path.join(root, file))
                        if not self.is_file_processed(file_path):
                            logger.info(f"发现未处理的文件: {file_path}")
                            yield file_path
        except Exception as e:
            logger.error(f"扫描目录时发生错误: {str(e)}")
    
    def __del__(self):
        """确保在对象销毁时保存缓存"""
        self.save_cache()