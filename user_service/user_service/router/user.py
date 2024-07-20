# from typing import Annotated
# from fastapi import APIRouter, Depends, HTTPException
# from sqlmodel import Session
# from user_service.auth import current_user, get_user_from_db, hash_password, oauth_scheme
# from user_service.model import get_session
# from user_service.model import Register_User, User



# user_router = APIRouter(
#     prefix="/user",
#     tags=["user"],
#     responses={404: {"description": "Not found"}}
# )

# @user_router.get("/")
# async def read_user():
#     return {"message": "Welcome to dailyDo todo app User Page"}

# @user_router.post("/register")
# async def regiser_user (new_user:Annotated[Register_User, Depends()],
#                         session:Annotated[Session, Depends(get_session)]):
    
#     db_user = get_user_from_db(session, new_user.username, new_user.email)
#     if db_user:
#         raise HTTPException(status_code=409, detail="User with these credentials already exists")
#     user = User(username = new_user.username,
#                 email = new_user.email,
#                 password = hash_password(new_user.password))
#     session.add(user)
#     session.commit()
#     session.refresh(user)
#     return {"message": f""" User with {user.username} successfully registered """}


# @user_router.get('/me')
# async def user_profile (current_user:Annotated[User, Depends(current_user)]):

#     return current_user


from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException
from user_service.auth import current_user, hash_password
from user_service.model import Register_User, User
from aiokafka import AIOKafkaProducer
from user_service.setting import KAFKA_USER_TOPIC, BOOTSTRAP_SERVER
from contextlib import asynccontextmanager

user_router = APIRouter(
    prefix="/user",
    tags=["user"],
    responses={404: {"description": "Not found"}}
)

@asynccontextmanager
async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

@user_router.get("/")
async def read_user():
    return {"message": "Welcome to the User Service"}

@user_router.post("/register")
async def register_user(
    new_user: Annotated[Register_User, Depends()],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    user_event = {
        'event': 'user_created',
        'username': new_user.username,
        'email': new_user.email,
        'password': hash_password(new_user.password)
    }
    await producer.send_and_wait(KAFKA_USER_TOPIC, bytes(str(user_event), 'utf-8'))
    return {"message": f"User with {new_user.username} successfully registered"}

@user_router.get('/me')
async def user_profile(current_user: Annotated[User, Depends(current_user)]):
    return current_user
