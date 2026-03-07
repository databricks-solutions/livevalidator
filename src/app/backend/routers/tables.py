"""Tables router."""

from fastapi import APIRouter, Depends, HTTPException

from backend.dependencies import DBSession, get_current_user_email, get_db
from backend.models import BulkTableRequest, TableIn, TableUpdate
from backend.services.entity_service import EntityService
from backend.services.users_service import UsersService

router = APIRouter(prefix="/tables", tags=["tables"])


@router.get("")
async def list_tables(
    q: str | None = None,
    db: DBSession = Depends(get_db),
):
    service = EntityService(db, "", "table")
    return await service.list(q)


@router.post("")
async def create_table(
    body: TableIn,
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    users = UsersService(db)
    await users.require_role(user_email, "CAN_RUN", "CAN_EDIT", "CAN_MANAGE")

    service = EntityService(db, user_email, "table")
    return await service.create(body.model_dump())


@router.get("/{id}")
async def get_table(
    id: int,
    db: DBSession = Depends(get_db),
):
    service = EntityService(db, "", "table")
    return await service.get(id)


@router.put("/{id}")
async def update_table(
    id: int,
    body: TableUpdate,
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    users = UsersService(db)
    if not await users.can_edit_object(user_email, "tables", id):
        raise HTTPException(403, "You don't have permission to edit this table")

    service = EntityService(db, user_email, "table")
    return await service.update(id, body.model_dump(exclude_unset=True))


@router.delete("/{id}")
async def delete_table(
    id: int,
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    users = UsersService(db)
    if not await users.can_edit_object(user_email, "tables", id):
        raise HTTPException(403, "You don't have permission to delete this table")

    service = EntityService(db, user_email, "table")
    return await service.delete(id)


@router.post("/bulk")
async def bulk_create_tables(
    body: BulkTableRequest,
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    users = UsersService(db)
    await users.require_role(user_email, "CAN_RUN", "CAN_EDIT", "CAN_MANAGE")

    service = EntityService(db, user_email, "table")
    items = [item.model_dump() for item in body.items]
    return await service.bulk_create(body.src_system_id, body.tgt_system_id, items)


@router.post("/{id}/fetch-lineage")
async def fetch_lineage_for_table(
    id: int,
    system: str = "source",
    db: DBSession = Depends(get_db),
):
    from backend.services.validation_history_service import ValidationHistoryService

    service = ValidationHistoryService(db)
    return await service.fetch_lineage_for_table(id, system)


@router.patch("/{id}/lineage")
async def update_table_lineage(
    id: int,
    body: dict,
    db: DBSession = Depends(get_db),
):
    service = EntityService(db, "", "table")
    return await service.update_lineage(id, body.get("lineage"))
