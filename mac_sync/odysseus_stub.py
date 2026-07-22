"""Placeholder standing in for the real Odysseus service during local
testing of the sync pipeline. Implements the contract documented in sync.py.
Replace with the actual Odysseus service — not part of this task."""

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class IncomingMessage(BaseModel):
    id: int
    user_id: int
    text: str
    created_at: str


@app.post("/odysseus/ingest")
async def ingest(message: IncomingMessage):
    return {"confirmed": True}


@app.get("/odysseus/generate")
async def generate(user_id: int, questions: int = 5, replies: int = 5):
    return {
        "questions": [f"Стаб-вопрос {i} (user {user_id})" for i in range(1, questions + 1)],
        "replies": [f"Стаб-ответ {i} (user {user_id})" for i in range(1, replies + 1)],
    }
