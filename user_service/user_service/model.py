from fastapi import Form
from pydantic import BaseModel

class User(BaseModel):
    username: str
    email: str
    password: str

class Register_User(BaseModel):
    username: str = Form()
    email: str = Form()
    password: str = Form()

class User(BaseModel):
    access_token: str
    token_type: str
    refresh_token: str

class UserData(BaseModel):
    username: str

class RefreshTokenData(BaseModel):
    email: str
