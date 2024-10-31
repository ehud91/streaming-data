from sqlalchemy import create_engine, Column, Integer, String, Float, MetaData, Table
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.sql import func
from random import randint
import time

from json import dumps
from kafka import kafkaProducer

# Replace 'your_postgres_user', 'your_postgres_password', 'your_postgres_database'
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@postgres5432/postgres"

kafka_nodes = "kafka:9092"
myTopic = "order"

def gen_data(order):

    prod = kafkaProducer(bootstrap_servers=kafka_nodes, value_serializer=lambda x:dump)
    my_data = {
        'category': order.category,
        'cost': order.cost
    }

    prod.send(topic=myTopic, value=my_data)
    print(order.customer_id, order.category, order.cost, order.item_name)
    prod.flush()

    try:
        time.sleep(20)

        # SQLAlchemy engine
        engine = create_engine(DATABASE_URL)

        # Create a base
        Base = declarative_base()

        # Declare the 'orders' table
        class Order(base):
            __tablename__ = "orders"
            customer_id = Column(Integer, primary_key=True)
            category = Column(String(255))
            cost = Column(Float)
            item_name = Column(String(255))

        # Query and print the first 5 rows
        orders = session.query(Order).all()
        for order in orders:
            gen_data(order)
    
        # Close the session
        Session.close()
    except Exception as e:
        print(e)