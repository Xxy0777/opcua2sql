import asyncio
import logging
from datetime import datetime
from asyncua import Client
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# 配置日志
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

# 数据库配置 (这里使用SQLite作为示例，你可以替换为MySQL/PostgreSQL等)
DATABASE_URL = "sqlite:///opcua_data.db"  # 替换为你的数据库连接字符串

# 创建数据库引擎
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# 定义数据表模型
class OPCUAData(Base):
    __tablename__ = 'opcua_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(String(500), nullable=False)
    value = Column(Text, nullable=False)  # 使用Text类型以容纳各种数据类型
    data_type = Column(String(100), nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    server_timestamp = Column(DateTime, nullable=True)
    source_timestamp = Column(DateTime, nullable=True)
    status_code = Column(String(100), nullable=False)

# 创建表
Base.metadata.create_all(engine)

# 创建会话工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# OPC UA 配置
OPCUA_SERVER_URL = "opc.tcp://localhost:4840"  # 替换为你的OPC UA服务器地址
NODE_IDS = [
    "ns=2;i=1",  # 替换为你要读取的节点ID
    "ns=2;i=2",
    "ns=2;i=3"
]

class OPCUAToDatabase:
    def __init__(self, server_url, node_ids):
        self.server_url = server_url
        self.node_ids = node_ids
        self.client = None
    
    async def connect_to_opcua(self):
        """连接到OPC UA服务器"""
        try:
            self.client = Client(url=self.server_url)
            await self.client.connect()
            _logger.info(f"成功连接到OPC UA服务器: {self.server_url}")
            return True
        except Exception as e:
            _logger.error(f"连接OPC UA服务器失败: {e}")
            return False
    
    async def read_node_data(self, node_id):
        """读取指定节点的数据"""
        try:
            node = self.client.get_node(node_id)
            value = await node.read_value()
            data_type = await node.read_data_type_as_variant_type()
            server_timestamp = await node.read_server_timestamp()
            source_timestamp = await node.read_source_timestamp()
            status_code = await node.read_status_code()
            
            return {
                "node_id": node_id,
                "value": str(value),  # 转换为字符串以便存储
                "data_type": str(data_type),
                "server_timestamp": server_timestamp,
                "source_timestamp": source_timestamp,
                "status_code": str(status_code)
            }
        except Exception as e:
            _logger.error(f"读取节点 {node_id} 数据失败: {e}")
            return None
    
    def save_to_database(self, data):
        """将数据保存到数据库"""
        if data is None:
            return False
            
        try:
            session = SessionLocal()
            record = OPCUAData(
                node_id=data["node_id"],
                value=data["value"],
                data_type=data["data_type"],
                server_timestamp=data["server_timestamp"],
                source_timestamp=data["source_timestamp"],
                status_code=data["status_code"]
            )
            session.add(record)
            session.commit()
            session.close()
            _logger.info(f"成功保存节点 {data['node_id']} 的数据到数据库")
            return True
        except Exception as e:
            _logger.error(f"保存数据到数据库失败: {e}")
            return False
    
    async def read_and_save_all_nodes(self):
        """读取所有节点数据并保存到数据库"""
        if not await self.connect_to_opcua():
            return False
        
        try:
            for node_id in self.node_ids:
                data = await self.read_node_data(node_id)
                self.save_to_database(data)
                await asyncio.sleep(0.1)  # 短暂延迟，避免过快请求
            
            return True
        finally:
            await self.disconnect()
    
    async def continuous_monitoring(self, interval=10):
        """持续监控节点并保存数据（每隔指定间隔）"""
        if not await self.connect_to_opcua():
            return
        
        _logger.info(f"开始持续监控，间隔: {interval}秒")
        
        try:
            while True:
                _logger.info("开始读取节点数据...")
                for node_id in self.node_ids:
                    data = await self.read_node_data(node_id)
                    self.save_to_database(data)
                    await asyncio.sleep(0.1)
                
                _logger.info(f"本轮数据读取完成，等待 {interval} 秒...")
                await asyncio.sleep(interval)
        except KeyboardInterrupt:
            _logger.info("监控被用户中断")
        except Exception as e:
            _logger.error(f"监控过程中发生错误: {e}")
        finally:
            await self.disconnect()
    
    async def disconnect(self):
        """断开OPC UA连接"""
        if self.client:
            await self.client.disconnect()
            _logger.info("已断开OPC UA服务器连接")

async def main():
    """主函数"""
    opcua_to_db = OPCUAToDatabase(OPCUA_SERVER_URL, NODE_IDS)
    
    # 方式1: 单次读取并保存
    # await opcua_to_db.read_and_save_all_nodes()
    
    # 方式2: 持续监控（每隔30秒读取一次）
    await opcua_to_db.continuous_monitoring(interval=30)

if __name__ == "__main__":
    # 运行主程序
    asyncio.run(main())