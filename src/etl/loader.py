import psycopg2
from loguru import logger
import pandas as pd
from typing import Optional
from tortoise import Tortoise
from ..models import Order, DATABASE_CONFIG

class PostgresLoader:
    def __init__(self):
        self.initialized = False

    async def _ensure_db_initialized(self):
        if not self.initialized:
            await Tortoise.init(config=DATABASE_CONFIG)
            self.initialized = True

    async def load(self, df: pd.DataFrame) -> None:
        """使用 Tortoise ORM 将数据加载到数据库"""
        try:
            await self._ensure_db_initialized()
            logger.info("开始数据库写入操作")
            
            # 将 DataFrame 转换为字典列表
            records = df.to_dict('records')
            
            # 批量创建记录
            orders = [Order(**record) for record in records]
            await Order.bulk_create(orders)
            
            logger.info(f"成功写入 {len(df)} 条数据到数据库")
            
        except Exception as e:
            logger.error(f"数据库写入错误: {str(e)}")
            raise
            
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()