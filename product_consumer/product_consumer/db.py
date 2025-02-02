from sqlmodel import SQLModel, create_engine, Session
from product_consumer import settings




connection_string: str = str(settings.DATABASE_URL).replace("postgresql", "postgresql+psycopg")




engine = create_engine(connection_string, pool_recycle=300, pool_size=10, echo=True) 
 


def create_tables():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session