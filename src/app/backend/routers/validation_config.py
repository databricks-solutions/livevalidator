"""Validation config router."""

from fastapi import APIRouter, Depends, HTTPException

from backend.dependencies import DBSession, get_current_user_email, get_db
from backend.models import ValidatePythonCode
from backend.services.type_transformations_service import TypeTransformationsService
from backend.services.validation_config_service import ValidationConfigService

router = APIRouter(tags=["validation-config"])


@router.get("/validation-config")
async def get_validation_config(db: DBSession = Depends(get_db)):
    service = ValidationConfigService(db)
    return await service.get_validation_config()


@router.put("/validation-config")
async def update_validation_config(
    body: dict,
    db: DBSession = Depends(get_db),
    user_email: str = Depends(get_current_user_email),
):
    service = ValidationConfigService(db, user_email)
    return await service.update_validation_config(body)


@router.get("/validation-config/effective")
async def get_effective_config(
    entity_type: str,
    entity_id: int,
    db: DBSession = Depends(get_db),
):
    """Get effective config for an entity (table/compare_query) by ID."""
    if entity_type not in ("table", "compare_query"):
        raise HTTPException(status_code=400, detail="entity_type must be 'table' or 'compare_query'")
    service = ValidationConfigService(db)
    return await service.get_effective_config(entity_type, entity_id)


@router.post("/validate-python")
async def validate_python_code(
    body: ValidatePythonCode,
    db: DBSession = Depends(get_db),
):
    service = TypeTransformationsService(db, "")
    return service.validate_python_code(body.code)
