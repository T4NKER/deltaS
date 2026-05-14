import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from src.marketplace.auth import get_current_seller, get_current_user
from src.marketplace.schemas import DatasetCreate, DatasetResponse
from src.models.database import Dataset, User, get_db
from src.utils.metadata_signing import validate_metadata_signature

router = APIRouter()


@router.get("/datasets", response_model=list[DatasetResponse])
async def list_datasets(
    q: Optional[str] = None,
    risk_level: Optional[str] = None,
    seller_id: Optional[int] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if current_user.role == "seller":
        query = db.query(Dataset).filter(Dataset.seller_id == current_user.id)
    else:
        query = db.query(Dataset).filter(Dataset.is_public == True)

    if q:
        pattern = f"%{q.strip()}%"
        query = query.filter(or_(
            Dataset.name.ilike(pattern),
            Dataset.description.ilike(pattern),
            Dataset.table_name.ilike(pattern),
        ))
    if risk_level:
        query = query.filter(Dataset.risk_level == risk_level)
    if seller_id:
        query = query.filter(Dataset.seller_id == seller_id)
    return query.all()


@router.post("/datasets", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(
    dataset_data: DatasetCreate,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db),
):
    if not dataset_data.metadata_bundle:
        raise HTTPException(
            status_code=400,
            detail="metadata_bundle is required. Publish via the seller /seller/publish-metadata endpoint to obtain a signed bundle.",
        )

    metadata = dataset_data.metadata_bundle.model_dump(by_alias=True)

    if not current_user.public_key:
        raise HTTPException(
            status_code=400,
            detail="Seller must register a metadata signing public key before publishing datasets.",
        )

    if not validate_metadata_signature(metadata, current_user.public_key):
        raise HTTPException(
            status_code=400,
            detail="Invalid metadata signature. Metadata bundle must be signed by the seller.",
        )

    if metadata.get("seller_id") != current_user.id:
        raise HTTPException(
            status_code=400,
            detail="Metadata bundle seller_id does not match authenticated seller.",
        )

    anchor_columns = ','.join(metadata.get("anchor_columns", []))
    pii_analysis = metadata.get("pii_analysis", {})
    risk_score = pii_analysis.get("risk_score", 0.0)
    risk_level = pii_analysis.get("risk_level", "low")
    sensitive_columns_dict = pii_analysis.get("sensitive_columns", {})
    sensitive_columns = json.dumps(sensitive_columns_dict)
    detected_pii_types = ','.join(pii_analysis.get("pii_types", {}).keys())

    pii_types = pii_analysis.get("pii_types", {})
    direct_ids = {"email", "phone", "isikukood", "credit_card"}
    has_direct = bool(set(pii_types.keys()) & direct_ids)
    if has_direct:
        privacy_status = "blocked"
    elif risk_score > 0:
        privacy_status = "review_required"
    else:
        privacy_status = "clear"

    if privacy_status == "blocked":
        raise HTTPException(
            status_code=400,
            detail="This dataset contains direct identifier signals and cannot be published. Generate a synthetic or de-identified dataset instead.",
        )

    dataset = Dataset(
        name=metadata.get("name") or dataset_data.name,
        description=metadata.get("description") or dataset_data.description,
        table_path=metadata.get("table_path") or dataset_data.table_path,
        price=dataset_data.price,
        license_name=metadata.get("license_name") or dataset_data.license_name,
        license_terms=metadata.get("license_terms") or dataset_data.license_terms,
        is_public=dataset_data.is_public,
        seller_id=current_user.id,
        anchor_columns=anchor_columns,
        risk_score=risk_score,
        risk_level=risk_level,
        privacy_status=privacy_status,
        sensitive_columns=sensitive_columns,
        detected_pii_types=detected_pii_types,
        requires_approval=privacy_status == "review_required",
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    return dataset


@router.get("/my-datasets", response_model=list[DatasetResponse])
async def get_my_datasets(
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db),
):
    return db.query(Dataset).filter(Dataset.seller_id == current_user.id).all()
