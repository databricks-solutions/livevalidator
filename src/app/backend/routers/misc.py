"""Miscellaneous routes (timezones, bindings, queue status, current user)."""

from fastapi import APIRouter, Depends

from backend.dependencies import DBSession, get_current_user_email, get_db
from backend.models import BindingIn
from backend.services.schedules_service import SchedulesService
from backend.services.triggers_service import TriggersService
from backend.services.users_service import UsersService

router = APIRouter(tags=["misc"])


@router.get("/current_user")
async def get_current_user(
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    users = UsersService(db)
    return await users.get_current_user_info(user_email)


@router.get("/timezones")
async def list_timezones():
    service = SchedulesService(None, "")
    return service.list_timezones()


@router.get("/queue-status")
async def get_queue_status(
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    service = TriggersService(db, user_email)
    return await service.get_queue_status()


@router.post("/bindings")
async def bind_schedule(
    body: BindingIn,
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    service = SchedulesService(db, user_email)
    return await service.create_binding(body.schedule_id, body.entity_type, body.entity_id)


@router.get("/bindings/{entity_type}/{entity_id}")
async def list_bindings(
    entity_type: str,
    entity_id: int,
    db: DBSession = Depends(get_db),
):
    service = SchedulesService(db, "")
    return await service.list_bindings(entity_type, entity_id)


@router.get("/bindings/all")
async def list_all_bindings(db: DBSession = Depends(get_db)):
    service = SchedulesService(db, "")
    return await service.list_all_bindings()


@router.get("/bindings_by_sched/{schedule_id}")
async def list_bindings_by_schedule(
    schedule_id: int,
    db: DBSession = Depends(get_db),
):
    service = SchedulesService(db, "")
    return await service.list_bindings_by_schedule(schedule_id)


@router.delete("/bindings/{id}")
async def delete_binding(
    id: int,
    db: DBSession = Depends(get_db),
):
    service = SchedulesService(db, "")
    return await service.delete_binding(id)
