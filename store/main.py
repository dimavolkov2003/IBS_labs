import asyncio
import json
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body, Depends
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import select
from datetime import datetime
from pydantic import BaseModel, field_validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)


# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}


# FastAPI WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


# FastAPI CRUDL endpoints
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData], db: Session = Depends(get_db)):
    # Insert data to database
    # Send data to subscribers

    for item in data:
         query = processed_agent_data.insert().values(
            road_state = item.road_state,
            user_id = item.agent_data.user_id,
            x = item.agent_data.accelerometer.x,
            y = item.agent_data.accelerometer.y,
            z = item.agent_data.accelerometer.z,
            latitude = item.agent_data.gps.latitude,
            longitude = item.agent_data.gps.longitude,
            timestamp = item.agent_data.timestamp
    )
    db.execute(query)
    db.commit()
    return "Ok!"



@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def read_processed_agent_data(processed_agent_data_id: int, db: Session = Depends(get_db)):
    # Get data by id
    query = select(processed_agent_data).where(
        processed_agent_data.c.id == processed_agent_data_id
    )

    result = db.execute(query).first()

    if not result:
        raise HTTPException(status_code=404, detail="Data not found")

    return result




@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data(db: Session = Depends(get_db)):
    # Get list of data
    query = select(processed_agent_data)
    result = db.execute(query)

    if result is None:
        raise HTTPException(status_code=404, detail="Data not found")

    return result



@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData, db: Session = Depends(get_db)):
    # Update data

    query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
    result = db.execute(query).first()

    if not result:
        raise HTTPException(status_code=404, detail="Data not found")

    update_query = (processed_agent_data.update().where(processed_agent_data.c.id == processed_agent_data_id)
    .values(
        road_state=data.road_state,
        user_id=data.agent_data.user_id,
        x=data.agent_data.accelerometer.x,
        y=data.agent_data.accelerometer.y,
        z=data.agent_data.accelerometer.z,
        latitude=data.agent_data.gps.latitude,
        longitude=data.agent_data.gps.latitude,
        timestamp=data.agent_data.timestamp,
    )
    )
    db.execute(update_query)
    db.commit()

    query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
    result = db.execute(query).first()

    return result




@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int, db: Session = Depends(get_db)):
    # Delete by id
    query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
    result = db.execute(query).first()

    if not result:
        raise HTTPException(status_code=404, detail="Data not found")

    delete_query = processed_agent_data.delete().where(processed_agent_data.c.id == processed_agent_data_id)
    db.execute(delete_query)
    db.commit()

    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)