import pandas as pd
from loguru import logger
from typing import Optional

class CSVExtractor:
    def __init__(self):
        pass

    async def extract(self, file_path: str) -> Optional[pd.DataFrame]:
        """从CSV文件中提取数据"""
        try:
            logger.info(f"开始读取文件: {file_path}")
            df = pd.read_csv(file_path, dtype_backend='pyarrow')
            logger.info(f"成功读取文件: {file_path}, 共 {len(df)} 行数据")
            return df
        except Exception as e:
            logger.error(f"读取文件 {file_path} 时发生错误: {str(e)}")
            raise