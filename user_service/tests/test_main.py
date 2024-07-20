from fastapi.testclient import TestClient
from sqlmodel import Field, Session, SQLModel, create_engine, select
from todo_app.main import app, get_session, Todo
from todo_app import setting


def test_read_main():
    client = TestClient(app=app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"Hello": "World"}

def test_write_main():

    connection_string = str(setting.TEST_DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg")

    engine = create_engine(
        connection_string, connect_args={"sslmode": "require"}, pool_recycle=300)

    SQLModel.metadata.create_all(engine)  

    with Session(engine) as session:  

        def get_session_override():  
                return session  

        app.dependency_overrides[get_session] = get_session_override 

        client = TestClient(app=app)

        todo_content = "buy bread"

        response = client.post("/todos/",
            json={"content": todo_content}
        )

        data = response.json()

        assert response.status_code == 200
        assert data["content"] == todo_content

def test_read_list_main():

    connection_string = str(setting.TEST_DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg")

    engine = create_engine(
        connection_string, connect_args={"sslmode": "require"}, pool_recycle=300)

    SQLModel.metadata.create_all(engine)  

    with Session(engine) as session:  

        def get_session_override():  
                return session  

        app.dependency_overrides[get_session] = get_session_override 
        client = TestClient(app=app)

        response = client.get("/todos/")
        assert response.status_code == 200

def test_update_todo():
    connection_string = str(setting.TEST_DATABASE_URL).replace(
        "postgresql", "postgresql+psycopg")

    engine = create_engine(
        connection_string, connect_args={"sslmode": "require"}, pool_recycle=300)

    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:

        def get_session_override():
            return session

        app.dependency_overrides[get_session] = get_session_override

        client = TestClient(app=app)

        # Create a todo item
        todo_content = "buy bread"
        response = client.post(
            "/todos/",
            json={"content": todo_content}
        )
        data = response.json()
        assert response.status_code == 200
        assert data["content"] == todo_content
        todo_id = data["id"]

        # Update the todo item
        updated_todo_content = "buy milk"
        response = client.put(
            f"/todos/{todo_id}",
            json={"content": updated_todo_content}
        )
        updated_data = response.json()
        assert response.status_code == 200
        assert updated_data["content"] == updated_todo_content

def test_delete_todo():
    connection_string = str(setting.TEST_DATABASE_URL).replace(
        "postgresql", "postgresql+psycopg")

    engine = create_engine(
        connection_string, connect_args={"sslmode": "require"}, pool_recycle=300)

    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:

        def get_session_override():
            return session

        app.dependency_overrides[get_session] = get_session_override

        client = TestClient(app=app)

        # Create a todo item
        todo_content = "buy bread"
        response = client.post(
            "/todos/",
            json={"content": todo_content}
        )
        data = response.json()
        assert response.status_code == 200
        assert data["content"] == todo_content
        todo_id = data["id"]

        # Delete the todo item
        response = client.delete(f"/todos/{todo_id}")
        assert response.status_code == 200

