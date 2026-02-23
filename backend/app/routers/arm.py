from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..config import settings

router = APIRouter(prefix="/api/arm", tags=["safety"])


class ArmStatus(BaseModel):
    dry_run: bool
    armed: bool


class ArmRequest(BaseModel):
    armed: bool


@router.get("", response_model=ArmStatus)
def get_arm_status():
    # Note: settings values are loaded from env at process start.
    # This endpoint reports the current in-memory config values.
    return ArmStatus(dry_run=settings.dry_run, armed=settings.armed)


@router.post("", response_model=ArmStatus)
def set_arm_status(req: ArmRequest):
    """
    ARM/DISARM toggle.
    IMPORTANT: This only changes the in-memory value for the running process.
    If you restart Uvicorn, it will revert to the value in .env (ARMED=...).
    """
    # Safety: you cannot arm if DRY_RUN is still true.
    if req.armed and settings.dry_run:
        raise HTTPException(
            status_code=400,
            detail="Cannot ARM while DRY_RUN=true. Set DRY_RUN=false in .env, restart backend, then ARM.",
        )

    # Update runtime state
    settings.armed = bool(req.armed)
    return ArmStatus(dry_run=settings.dry_run, armed=settings.armed)
