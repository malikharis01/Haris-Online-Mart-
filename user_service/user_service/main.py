import asyncio
from datetime import timedelta
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated, Dict, Any
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlmodel import Session, select
from user_service.auth import EXPIRY_TIME, authenticate_user, create_access_token, current_user, validate_refresh_token, create_refresh_token
from user_service.model import Register_User, Token, get_session, Todo, create_tables, Todo_Create, Todo_Edit, User
from user_service.router import user
from user_service.setting import BOOTSTRAP_SERVER, KAFKA_USER_TOPIC

MAX_RETRIES = 5
RETRY_INTERVAL = 10

async def create_topic() -> None:
    admin_client = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER)
    retries = 0

    while retries < MAX_RETRIES:
        try:
            await admin_client.start()
            topic_list = [NewTopic(name=KAFKA_USER_TOPIC, num_partitions=2, replication_factor=1)]
            try:
                await admin_client.create_topics(new_topics=topic_list, validate_only=False)
                print(f"Topic '{KAFKA_USER_TOPIC}' created successfully")
            except Exception as e:
                print(f"Failed to create topic '{KAFKA_USER_TOPIC}': {e}")
            finally:
                await admin_client.close()
            return
        except KafkaConnectionError:
            retries += 1
            print(f"Kafka connection failed. Retrying {retries}/{MAX_RETRIES}...")
            await asyncio.sleep(RETRY_INTERVAL)
    raise Exception("Failed to connect to Kafka broker after several retries")

@asynccontextmanager
async def kafka_producer() -> AIOKafkaProducer: # type: ignore
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print('Creating Tables')
    create_tables()
    print("Tables Created")
    await create_topic()
    yield

app = FastAPI(lifespan=lifespan, title="dailyDo Todo App", version='1.0.0')

@app.get('/')
async def root():
    return {"message": "Welcome to dailyDo todo app"}

@app.post('/token', response_model=Token)
async def login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    user = authenticate_user(form_data.username, form_data.password, session)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    
    expire_time = timedelta(minutes=EXPIRY_TIME)
    access_token = create_access_token({"sub": form_data.username}, expire_time)

    refresh_expire_time = timedelta(days=7)
    refresh_token = create_refresh_token({"sub": user.email}, refresh_expire_time)

    user_event = {
        'event': 'user_login',
        'username': form_data.username,
        'password': form_data.password  # This is just for demonstration. Do not log or transmit passwords in real systems.
    }
    await producer.send_and_wait(BOOTSTRAP_SERVER, bytes(str(user_event), 'utf-8'))

    return Token(access_token=access_token, token_type="bearer", refresh_token=refresh_token)

@app.post("/token/refresh")
async def refresh_token(
    old_refresh_token: str,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    credential_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid token, Please login again",
        headers={"www-Authenticate": "Bearer"}
    )
    
    user = validate_refresh_token(old_refresh_token, session)
    if not user:
        raise credential_exception
    
    expire_time = timedelta(minutes=EXPIRY_TIME)
    access_token = create_access_token({"sub": user.username}, expire_time)

    refresh_expire_time = timedelta(days=7)
    refresh_token = create_refresh_token({"sub": user.email}, refresh_expire_time)

    refresh_event = {
        'event': 'refresh_token',
        'refresh_token': old_refresh_token
    }
    await producer.send_and_wait(KAFKA_USER_TOPIC, bytes(str(refresh_event), 'utf-8'))

    return Token(access_token=access_token, token_type="bearer", refresh_token=refresh_token)

@app.post('/users/', response_model=Dict[str, Any])
async def create_user(
    user: Register_User,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    user_event = {
        'event': 'user_created',
        'username': user.username,
        'email': user.email,
        'password': user.password  # Ideally, hash the password
    }
    await producer.send_and_wait(KAFKA_USER_TOPIC, bytes(str(user_event), 'utf-8'))
    return {"message": "User creation event sent to Kafka"}

@app.post('/todos/', response_model=Todo)
async def create_todo(
    current_user: Annotated[User, Depends(current_user)],
    todo: Todo_Create,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    new_todo = Todo(content=todo.content, user_id=current_user.id)
    
    session.add(new_todo)
    session.commit()
    session.refresh(new_todo)
    
    todo_event = {
        'event': 'todo_created',
        'user_id': current_user.id,
        'content': todo.content
    }
    await producer.send_and_wait(KAFKA_USER_TOPIC, bytes(str(todo_event), 'utf-8'))
    
    return new_todo

@app.get('/todos/', response_model=list[Todo])
async def get_all(
    current_user: Annotated[User, Depends(current_user)],
    session: Annotated[Session, Depends(get_session)]
):
    todos = session.exec(select(Todo).where(Todo.user_id == current_user.id)).all()
    if todos:
        return todos
    else:
        raise HTTPException(status_code=404, detail="No Task found")

@app.get('/todos/{id}', response_model=Todo)
async def get_single_todo(
    id: int,
    current_user: Annotated[User, Depends(current_user)],
    session: Annotated[Session, Depends(get_session)]
):
    user_todos = session.exec(select(Todo).where(Todo.user_id == current_user.id)).all()
    matched_todo = next((todo for todo in user_todos if todo.id == id), None)
    if matched_todo:
        return matched_todo
    else:
        raise HTTPException(status_code=404, detail="No Task found")

@app.put('/todos/{id}')
async def edit_todo(
    id: int,
    todo: Todo_Edit,
    current_user: Annotated[User, Depends(current_user)],
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    user_todos = session.exec(select(Todo).where(Todo.user_id == current_user.id)).all()
    existing_todo = next((todo for todo in user_todos if todo.id == id), None)
    if existing_todo:
        existing_todo.content = todo.content
        existing_todo.is_completed = todo.is_completed
        session.add(existing_todo)
        session.commit()
        session.refresh(existing_todo)
        
        edit_event = {
            'event': 'todo_edited',
            'user_id': current_user.id,
            'todo_id': id,
            'content': todo.content,
            'is_completed': todo.is_completed
        }
        await producer.send_and_wait(KAFKA_USER_TOPIC, bytes(str(edit_event), 'utf-8'))
        
        return existing_todo
    else:
        raise HTTPException(status_code=404, detail="No task found")

@app.delete('/todos/{id}')
async def delete_todo(
    id: int,
    current_user: Annotated[User, Depends(current_user)],
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    user_todos = session.exec(select(Todo).where(Todo.user_id == current_user.id)).all()
    todo = next((todo for todo in user_todos if todo.id == id), None)
    if todo:
        session.delete(todo)
        session.commit()
        
        delete_event = {
            'event': 'todo_deleted',
            'user_id': current_user.id,
            'todo_id': id
        }
        await producer.send_and_wait(KAFKA_USER_TOPIC, bytes(str(delete_event), 'utf-8'))
        
        return {"message": "Task successfully deleted"}
    else:
        raise HTTPException(status_code=404, detail="No task found")

# Include your user router
app.include_router(user.user_router)
