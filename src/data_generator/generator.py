import os
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict
from faker import Faker
from loguru import logger

# 配置日志输出
logger.add("data_generator.log", rotation="500 MB", level="INFO")

class OrderDataGenerator:
    def __init__(self):
        self.fake = Faker('zh_CN')
        logger.info("初始化OrderDataGenerator，设置语言为zh_CN")
        self.device_models = ['iPhone 14', 'iPhone 14 Pro', 'iPhone 15', 'iPhone 15 Pro',
                            'Samsung S23', 'Samsung S23 Ultra', 'Huawei P60', 'Xiaomi 14',
                            'OPPO Find X6', 'vivo X100']
        self.order_status = ['pending', 'paid', 'completed', 'cancelled', 'refunded']
        self.payment_methods = ['Alipay', 'WeChat Pay', 'Credit Card', 'Debit Card']
        
    def generate_order_data(self, num_records: int = 100) -> pd.DataFrame:
        data = {
            'order_id': [self.fake.unique.uuid4() for _ in range(num_records)],
            'user_id': [self.fake.uuid4() for _ in range(num_records)],
            'order_date': [self.fake.date_time_between(start_date='-30d', end_date='now') for _ in range(num_records)],
            'total_price': [round(random.uniform(100, 10000), 2) for _ in range(num_records)],
            'discount': [round(random.uniform(0, 500), 2) for _ in range(num_records)],
            'payment_method': [random.choice(self.payment_methods) for _ in range(num_records)],
            'order_status': [random.choice(self.order_status) for _ in range(num_records)],
            'province': [self.fake.province() for _ in range(num_records)],
            'city': [self.fake.city() for _ in range(num_records)],
            'district': [self.fake.district() for _ in range(num_records)],
            'street': [self.fake.street_address() for _ in range(num_records)],
            'customer_name': [self.fake.name() for _ in range(num_records)],
            'phone_number': [self.fake.phone_number() for _ in range(num_records)],
            'email': [self.fake.email() for _ in range(num_records)],
            'product_count': [random.randint(1, 10) for _ in range(num_records)],
            'shipping_fee': [round(random.uniform(0, 50), 2) for _ in range(num_records)],
            'tax': [round(random.uniform(0, 100), 2) for _ in range(num_records)],
            'delivery_time': [self.fake.future_datetime(end_date='+7d') for _ in range(num_records)],
            'log_info': [self._generate_log_info() for _ in range(num_records)]
        }
        
        return pd.DataFrame(data)
    
    def _generate_log_info(self) -> str:
        device_model = random.choice(self.device_models)
        os_version = f"iOS {random.randint(14,17)}" if 'iPhone' in device_model else f"Android {random.randint(10,14)}"
        browser = random.choice(['Chrome', 'Safari', 'Firefox'])
        browser_version = f"{random.randint(80,120)}.0.{random.randint(1000,9999)}.{random.randint(10,99)}"
        
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'device_model': device_model,
            'os_version': os_version,
            'browser': browser,
            'browser_version': browser_version,
            'ip_address': self.fake.ipv4(),
            'user_agent': f"{browser}/{browser_version}"
        }
        
        return str(log_data)
    
    def save_to_csv(self, output_dir: str, file_prefix: str = 'order_data') -> str:
        try:
            # 生成随机数据
            df = self.generate_order_data(random.randint(50, 200))
            
            # 确保输出目录存在
            os.makedirs(output_dir, exist_ok=True)
            
            # 生成文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            random_suffix = ''.join(random.choices('0123456789', k=4))
            filename = f"{file_prefix}_{timestamp}_{random_suffix}.csv"
            file_path = os.path.join(output_dir, filename)
            
            # 保存到CSV文件
            df.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"成功生成CSV文件: {filename}")
            
            return file_path
            
        except Exception as e:
            logger.error(f"生成CSV文件时发生错误: {str(e)}")
            raise