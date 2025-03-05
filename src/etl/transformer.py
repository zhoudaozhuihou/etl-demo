import pandas as pd
from loguru import logger
from typing import Optional

class DataTransformer:
    def __init__(self):
        pass

    async def transform(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """执行数据转换操作"""
        try:
            logger.info("开始数据转换处理")
            
            # 记录原始数据统计
            total_records = len(df)
            logger.info(f"原始数据统计:\n- 总记录数: {total_records}条")

            # 去重统计
            df_deduped = df.drop_duplicates(subset=['user_id', 'order_id'])
            duplicates_count = total_records - len(df_deduped)
            logger.info(f"数据去重:\n- 重复记录数: {duplicates_count}条\n- 去重后记录数: {len(df_deduped)}条")
            df = df_deduped
            
            # 计算折扣率和订单平均价格
            df['discount_rate'] = df['discount'] / df['total_price']
            df['avg_price'] = df.groupby('user_id')['total_price'].transform('mean')
            logger.info(f"价格统计:\n- 平均订单金额: {df['total_price'].mean():.2f}\n- 平均折扣率: {df['discount_rate'].mean():.2%}")
            
            # 从日志字段提取设备型号
            df['device_model'] = df['log_info'].str.extract(r'device_model[":]\s*(?P<model>[^,"\}]+)')
            device_stats = df['device_model'].value_counts().head()
            logger.info(f"设备统计:\n- 设备型号分布(Top 5):\n{device_stats.to_string()}")
            
            # 日期格式转换
            df['order_date'] = pd.to_datetime(df['order_date'], format='mixed')
            date_stats = df['order_date'].dt.date.value_counts().sort_index().head()
            logger.info(f"日期统计:\n- 订单日期分布(Top 5):\n{date_stats.to_string()}")
            
            # 合并地址字段
            address_columns = ['province', 'city', 'district', 'street']
            df['full_address'] = df[address_columns].fillna('').agg(' '.join, axis=1).str.strip()
            province_stats = df['province'].value_counts().head()
            logger.info(f"地区统计:\n- 省份分布(Top 5):\n{province_stats.to_string()}")
            
            # 过滤有效订单
            valid_orders = df[df['order_status'].isin(['completed', 'paid'])]
            status_stats = df['order_status'].value_counts()
            logger.info(f"订单状态统计:\n- 状态分布:\n{status_stats.to_string()}\n- 有效订单数: {len(valid_orders)}条")
            df = valid_orders
            
            logger.info(f"数据转换完成，最终数据行数: {len(df)}条")
            return df
            
        except Exception as e:
            logger.error(f"数据转换过程中发生错误: {str(e)}")
            raise