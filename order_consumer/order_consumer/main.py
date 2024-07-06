import asyncio
import logging
from fastapi import FastAPI, HTTPException
from sqlmodel import SQLModel, Field, create_engine, Session, select
from typing import List, Generator, AsyncGenerator
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from order_consumer.models import OrderItem
from order_consumer.proto import order_pb2, operation_pb2
from order_consumer.settings import BOOTSTRAP_SERVER, KAFKA_CONSUMER_GROUP_ID, KAFKA_ORDER_TOPIC, KAFKA_CONFIRMATION_TOPIC, KAFKA_INVENTORY_TOPIC
from order_consumer.db import create_tables, engine, get_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Order Consumer Service", version='1.0.0')

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info('Creating Tables')
    create_tables()
    logger.info("Tables Created")

    loop = asyncio.get_event_loop()
    task = [
        loop.create_task(consume_orders()),
        loop.create_task(consume_inventory())
    ]
    
    yield

    for t in task:
        t.cancel()
    await asyncio.gather(*task, return_exceptions=True)

MAX_RETRIES = 5
RETRY_INTERVAL = 10

async def create_topic():
    admin_client = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER)

    retries = 0

    while retries < MAX_RETRIES:
        try:
            await admin_client.start()
            topic_list = [NewTopic(name=KAFKA_CONFIRMATION_TOPIC, num_partitions=2, replication_factor=1)]
            try:
                await admin_client.create_topics(new_topics=topic_list, validate_only=False)
                logger.info(f"Topic '{KAFKA_CONFIRMATION_TOPIC}' created successfully")
            except Exception as e:
                logger.error(f"Failed to create topic '{KAFKA_CONFIRMATION_TOPIC}': {e}")
            finally:
                await admin_client.close()
            return
        
        except KafkaConnectionError:
            retries += 1 
            logger.error(f"Kafka connection failed. Retrying {retries}/{MAX_RETRIES}...")
            await asyncio.sleep(RETRY_INTERVAL)
        
    raise Exception("Failed to connect to kafka broker after several retries")

async def create_consumer(topic: str):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=BOOTSTRAP_SERVER,
                group_id=KAFKA_CONSUMER_GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000
            )
            await consumer.start()
            logger.info(f"Consumer for topic {topic} started successfully.")
            return consumer
        except Exception as e:
            retries += 1
            logger.error(f"Error starting consumer for topic {topic}, retry {retries}/{MAX_RETRIES}: {e}")
            if retries < MAX_RETRIES:
                await asyncio.sleep(RETRY_INTERVAL)
            else:
                logger.error(f"Max retries reached. Could not start consumer for topic {topic}.")
                return None

async def consume_orders():
    consumer = await create_consumer(KAFKA_ORDER_TOPIC)
    if not consumer:
        logger.error("Failed to create kafka order consumer")
        return

    try:
        async for msg in consumer:
            try:
                order = order_pb2.Order()
                order.ParseFromString(msg.value)
                logger.info(f"Received Order Message: {order}")

                with Session(engine) as session:
                    if order.operation == operation_pb2.OperationType.CREATE:
                        new_order = OrderItem(
                            product_id=order.product_id,
                            quantity=order.quantity
                        )
                        session.add(new_order)
                        session.commit()
                        session.refresh(new_order)
                        logger.info(f'Order added to db: {new_order}')
                    
                    elif order.operation == operation_pb2.OperationType.UPDATE:
                        existing_order = session.exec(select(OrderItem).where(OrderItem.id == order.id)).first()
                        if existing_order:
                            existing_order.product_id = order.product_id
                            existing_order.quantity = order.quantity
                            session.add(existing_order)
                            session.commit()
                            session.refresh(existing_order)
                            logger.info(f'Order updated in db: {existing_order}')
                        else:
                            logger.warning(f"Order with ID {order.id} not found")

                    elif order.operation == operation_pb2.OperationType.DELETE:
                        existing_order = session.exec(select(OrderItem).where(OrderItem.id == order.id)).first()
                        if existing_order:
                            session.delete(existing_order)
                            session.commit()
                            logger.info(f"Order with ID {order.id} successfully deleted")
                        else:
                            logger.warning(f"Order with ID {order.id} not found for deletion")

            except Exception as e:
                logger.error(f"Error processing order message: {e}")

    finally:
        await consumer.stop()
        logger.info("Order consumer stopped")

async def produce_to_confirmation_topic(message):
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    await producer.start()
    try:
        await producer.send_and_wait(KAFKA_CONFIRMATION_TOPIC, message)
    finally:
        await producer.stop()

async def consume_inventory():
    consumer = await create_consumer(KAFKA_INVENTORY_TOPIC)
    if not consumer:
        logger.error("Failed to create kafka inventory consumer")
        return

    try:
        async for msg in consumer:
            try:
                inventory = order_pb2.Inventory()
                inventory.ParseFromString(msg.value)
                logger.info(f"Received Inventory Message: {inventory}")

                if inventory.operation == operation_pb2.OperationType.CREATE:
                    with Session(engine) as session:
                        existing_order = session.exec(select(OrderItem).where(OrderItem.product_id == inventory.product_id)).first()
                        if existing_order:
                            await produce_to_confirmation_topic(msg.value)
                            logger.info(f"Valid Inventory message forwarded to {KAFKA_CONFIRMATION_TOPIC}")
                        else:
                            logger.error(f"Product with ID {inventory.product_id} does not exist. Inventory creation rejected.")

            except Exception as e:
                logger.error(f"Error processing inventory message: {e}")

    finally:
        await consumer.stop()
        logger.info("Inventory consumer stopped")

app = FastAPI(lifespan=lifespan, title="Order Consumer Service", version='1.0.0')

@app.get("/orders/", response_model=List[OrderItem])
async def get_orders():
    with Session(engine) as session:
        orders = session.exec(select(OrderItem)).all()
        return orders

@app.get("/orders/{order_id}", response_model=OrderItem)
async def get_order(order_id: int):
    with Session(engine) as session:
        order = session.exec(select(OrderItem).where(OrderItem.id == order_id)).first()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        return order
