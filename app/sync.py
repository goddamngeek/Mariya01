from typing import Literal

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from app.config import SYNC_BEARER_TOKEN
from app.db import ack_incoming_messages, insert_outgoing_messages, pull_unconfirmed_incoming

router = APIRouter(prefix="/sync")


def require_bearer(authorization: str = Header(default="")) -> None:
    if not SYNC_BEARER_TOKEN or authorization != f"Bearer {SYNC_BEARER_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


class AckRequest(BaseModel):
    ids: list[int]


class PushItem(BaseModel):
    user_id: int
    category: Literal["question", "reply"]
    text: str


class PushRequest(BaseModel):
    items: list[PushItem]


@router.get("/pull", dependencies=[Depends(require_bearer)])
async def pull():
    rows = await pull_unconfirmed_incoming()
    return [
        {"id": r["id"], "user_id": r["user_id"], "text": r["text"], "created_at": r["created_at"]}
        for r in rows
    ]


@router.post("/ack", dependencies=[Depends(require_bearer)])
async def ack(body: AckRequest):
    await ack_incoming_messages(body.ids)
    return {"ok": True}


@router.post("/push", dependencies=[Depends(require_bearer)])
async def push(body: PushRequest):
    await insert_outgoing_messages(
        [(item.user_id, item.category, item.text) for item in body.items]
    )
    return {"ok": True}
