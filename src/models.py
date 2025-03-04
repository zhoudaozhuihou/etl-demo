from tortoise import Model, fields
from typing import Optional

class Order(Model):
    """订单数据模型"""
    id = fields.IntField(pk=True)
    order_id = fields.CharField(max_length=36, unique=True)
    user_id = fields.UUIDField()
    order_date = fields.DatetimeField()
    total_price = fields.DecimalField(max_digits=10, decimal_places=2)
    discount = fields.DecimalField(max_digits=10, decimal_places=2)
    discount_rate = fields.DecimalField(max_digits=5, decimal_places=4, null=True)
    avg_price = fields.DecimalField(max_digits=10, decimal_places=2, null=True)
    payment_method = fields.CharField(max_length=50)
    order_status = fields.CharField(max_length=20)
    province = fields.CharField(max_length=50)
    city = fields.CharField(max_length=50)
    district = fields.CharField(max_length=50)
    street = fields.CharField(max_length=255)
    full_address = fields.CharField(max_length=500, null=True)
    customer_name = fields.CharField(max_length=100)
    phone_number = fields.CharField(max_length=20)
    email = fields.CharField(max_length=100)
    product_count = fields.IntField()
    shipping_fee = fields.DecimalField(max_digits=10, decimal_places=2)
    tax = fields.DecimalField(max_digits=10, decimal_places=2)
    delivery_time = fields.DatetimeField()
    device_model = fields.CharField(max_length=50, null=True)
    log_info = fields.TextField()
    
    class Meta:
        table = "orders"

DATABASE_CONFIG = {
    'connections': {
        'default': {
            'engine': 'tortoise.backends.asyncpg',
            'credentials': {
                'host': 'localhost',
                'port': '5432',
                'user': 'postgres',
                'password': 'postgres',
                'database': 'etl_db',
            }
        }
    },
    'apps': {
        'models': {
            'models': ['src.models'],
            'default_connection': 'default',
        }
    }
}