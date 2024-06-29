from fastapi import Form
from pydantic import BaseModel
from sqlmodel import SQLModel, Field
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated


class Product (BaseModel):
    name: str 
    price: float
    quantity: int


class Product_Update (BaseModel):
    id : int
  