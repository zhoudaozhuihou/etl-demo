import os
import re
import time
from datetime import datetime, timedelta
from collections import defaultdict
from loguru import logger

class LogAnalyzer:
    def __init__(self):
        self.log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
        self.etl_log_path = os.path.join(self.log_dir, 'etl.log')
        self.data_generator_log_path = os.path.join(self.log_dir, 'data_generator.log')
        self.last_read_position = {}
        self.stats = defaultdict(lambda: defaultdict(int))
        self.current_minute = datetime.now().replace(second=0, microsecond=0)
        
        # 初始化日志文件位置
        for log_file in [self.etl_log_path, self.data_generator_log_path]:
            if os.path.exists(log_file):
                self.last_read_position[log_file] = os.path.getsize(log_file)
    
    def parse_log_line(self, line):
        """解析单行日志"""
        try:
            # 解析日志时间戳
            timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3})', line)
            if not timestamp_match:
                return None
            
            timestamp = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S.%f')
            
            # 提取关键信息
            stats = {}
            if '成功生成CSV文件' in line:
                stats['generated_files'] = 1
            elif '文件处理完成' in line:
                stats['processed_files'] = 1
            elif '数据行数' in line:
                match = re.search(r'数据行数: (\d+)', line)
                if match:
                    stats['processed_records'] = int(match.group(1))
            elif '处理时间' in line:
                match = re.search(r'总处理时间: ([\d.]+)秒', line)
                if match:
                    stats['processing_time'] = float(match.group(1))
            
            return timestamp, stats
        
        except Exception as e:
            logger.error(f'解析日志行时发生错误: {str(e)}')
            return None
    
    def analyze_logs(self):
        """分析日志文件并更新统计信息"""
        current_time = datetime.now()
        logger.debug("开始分析日志文件")
        
        for log_file in [self.etl_log_path, self.data_generator_log_path]:
            if not os.path.exists(log_file):
                continue
            
            current_size = os.path.getsize(log_file)
            last_position = self.last_read_position.get(log_file, 0)
            
            if current_size < last_position:
                # 日志文件被轮转，从头开始读取
                last_position = 0
            
            if current_size > last_position:
                with open(log_file, 'r', encoding='utf-8') as f:
                    f.seek(last_position)
                    for line in f:
                        result = self.parse_log_line(line)
                        if result:
                            timestamp, stats = result
                            minute_key = timestamp.replace(second=0, microsecond=0)
                            
                            for key, value in stats.items():
                                self.stats[minute_key][key] += value
                
                self.last_read_position[log_file] = current_size
    
    def generate_report(self):
        """生成性能报告"""
        current_minute = datetime.now().replace(second=0, microsecond=0)
        last_minute = current_minute - timedelta(minutes=1)
        
        stats = self.stats[last_minute]
        total_stats = defaultdict(int)
        
        # 计算累计统计数据
        for minute, minute_stats in self.stats.items():
            if minute <= last_minute:
                for key, value in minute_stats.items():
                    total_stats[key] += value
        
        report = [
            f"性能报告 ({last_minute.strftime('%Y-%m-%d %H:%M')})",
            "-" * 50,
            f"本分钟数据:",
            f"- 生成的文件数: {stats.get('generated_files', 0)}",
            f"- 处理的文件数: {stats.get('processed_files', 0)}",
            f"- 处理的记录数: {stats.get('processed_records', 0)}",
            f"- 平均处理时间: {stats.get('processing_time', 0):.2f}秒",
            "",
            f"累计统计:",
            f"- 总生成文件数: {total_stats['generated_files']}",
            f"- 总处理文件数: {total_stats['processed_files']}",
            f"- 总处理记录数: {total_stats['processed_records']}",
            f"- 平均处理时间: {total_stats['processing_time']/total_stats['processed_files'] if total_stats['processed_files'] > 0 else 0:.2f}秒"
        ]
        
        # 输出报告
        logger.info("\n".join(report))
        
        # 计算处理效率
        if stats.get('processed_files', 0) > 0:
            avg_time = stats.get('processing_time', 0) / stats.get('processed_files', 1)
            report.extend([
                "\n处理效率:",
                f"- 平均处理时间: {avg_time:.2f}秒/文件",
                f"- 文件处理速率: {stats.get('processed_files', 0)/60:.2f}个/秒",
                f"- 数据处理速率: {stats.get('processed_records', 0)/60:.2f}行/秒"
            ])
        
        # 输出报告
        logger.info('\n'.join(report))
        
        # 清理旧数据
        old_minute = current_minute - timedelta(minutes=60)
        for minute in list(self.stats.keys()):
            if minute < old_minute:
                del self.stats[minute]
    
    async def run(self):
        """启动监控程序"""
        logger.info("日志分析监控程序已启动")
        
        while True:
            try:
                self.analyze_logs()
                
                current_minute = datetime.now().replace(second=0, microsecond=0)
                if current_minute > self.current_minute:
                    self.generate_report()
                    self.current_minute = current_minute
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"监控程序运行时发生错误: {str(e)}")
                await asyncio.sleep(5)

if __name__ == '__main__':
    import asyncio
    analyzer = LogAnalyzer()
    asyncio.run(analyzer.run())