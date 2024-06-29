from fastapi import Form
from pydantic import BaseModel
from sqlmodel import SQLModel, Field
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated


class Order (BaseModel):
    id : int 
    quantity : InterruptedError


