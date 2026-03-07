"""Queries router."""

from fastapi import APIRouter, Depends, HTTPException

from backend.dependencies import DBSession, get_current_user_email, get_db
from backend.models import BulkQueryRequest, QueryIn, QueryUpdate
from backend.services.entity_service import EntityService
from backend.services.users_service import UsersService

router = APIRouter(prefix="/queries", tags=["queries"])


@router.get("")
async def list_queries(
    q: str | None = None,
    db: DBSession = Depends(get_db),
):
    service = EntityService(db, "", "query")
    return await service.list(q)


@router.post("")
async def create_query(
    body: QueryIn,
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    users = UsersService(db)
    await users.require_role(user_email, "CAN_RUN", "CAN_EDIT", "CAN_MANAGE")

    service = EntityService(db, user_email, "query")
    return await service.create(body.model_dump())


@router.get("/{id}")
async def get_query(
    id: int,
    db: DBSession = Depends(get_db),
):
    service = EntityService(db, "", "query")
    return await service.get(id)


@router.put("/{id}")
async def update_query(
    id: int,
    body: QueryUpdate,
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    users = UsersService(db)
    if not await users.can_edit_object(user_email, "queries", id):
        raise HTTPException(403, "You don't have permission to edit this query")

    service = EntityService(db, user_email, "query")
    return await service.update(id, body.model_dump(exclude_unset=True))


@router.delete("/{id}")
async def delete_query(
    id: int,
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    users = UsersService(db)
    if not await users.can_edit_object(user_email, "queries", id):
        raise HTTPException(403, "You don't have permission to delete this query")

    service = EntityService(db, user_email, "query")
    return await service.delete(id)


@router.post("/bulk")
async def bulk_create_queries(
    body: BulkQueryRequest,
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    users = UsersService(db)
    await users.require_role(user_email, "CAN_RUN", "CAN_EDIT", "CAN_MANAGE")

    service = EntityService(db, user_email, "query")
    items = [item.model_dump() for item in body.items]
    return await service.bulk_create(body.src_system_id, body.tgt_system_id, items)


@router.post("/{id}/fetch-lineage")
async def fetch_lineage_for_query(id: int):
    """Lineage fetch for queries is not supported at this time."""
    raise HTTPException(
        status_code=400, detail="Lineage is only supported for tables at this time. Query lineage is not available."
    )


@router.patch("/{id}/lineage")
async def update_query_lineage(
    id: int,
    body: dict,
    db: DBSession = Depends(get_db),
):
    service = EntityService(db, "", "query")
    return await service.update_lineage(id, body.get("lineage"))
