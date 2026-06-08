"""Tags router."""

from fastapi import APIRouter, Depends

from backend.dependencies import DBSession, get_db
from backend.services.tags_service import TagsService

router = APIRouter(prefix="/tags", tags=["tags"])


@router.get("")
async def list_tags(db: DBSession = Depends(get_db)):
    service = TagsService(db)
    return await service.list_tags()


@router.post("")
async def create_tag(
    body: dict,
    db: DBSession = Depends(get_db),
):
    service = TagsService(db)
    return await service.create_tag(body.get("name", ""))


@router.get("/{tag_id}/entities")
async def get_tag_entities(
    tag_id: int,
    db: DBSession = Depends(get_db),
):
    service = TagsService(db)
    return await service.get_tag_entities(tag_id)


@router.get("/entity/{entity_type}/{entity_id}")
async def get_entity_tags(
    entity_type: str,
    entity_id: int,
    db: DBSession = Depends(get_db),
):
    service = TagsService(db)
    return await service.get_entity_tags(entity_type, entity_id)


@router.post("/entity/{entity_type}/{entity_id}")
async def set_entity_tags(
    entity_type: str,
    entity_id: int,
    body: dict,
    db: DBSession = Depends(get_db),
):
    service = TagsService(db)
    return await service.set_entity_tags(entity_type, entity_id, body.get("tags", []))


@router.post("/entity/bulk-add")
async def bulk_add_tags(
    body: dict,
    db: DBSession = Depends(get_db),
):
    service = TagsService(db)
    return await service.bulk_add_tags(body.get("entity_type"), body.get("entity_ids", []), body.get("tags", []))


@router.post("/entity/bulk-remove")
async def bulk_remove_tags(
    body: dict,
    db: DBSession = Depends(get_db),
):
    service = TagsService(db)
    return await service.bulk_remove_tags(body.get("entity_type"), body.get("entity_ids", []), body.get("tags", []))
