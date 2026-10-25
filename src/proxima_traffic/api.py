from collections.abc import AsyncIterator
from secrets import compare_digest
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from sqlalchemy import delete, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from proxima_traffic.account_state import ACCOUNT_PROTECTED_PAUSE_REASONS, locked_account
from proxima_traffic.bot.repository import AdminRepository
from proxima_traffic.content import (
    BroadcastContentVariant,
    ContentError,
    parse_content_variants,
    serialize_content_variants,
    validate_telegram_text,
)
from proxima_traffic.dialogues import (
    parse_dialogue_steps,
    serialize_dialogue_steps,
    validate_dialogue_steps,
)
from proxima_traffic.models import (
    Account,
    AccountStatus,
    ActionLog,
    AuditEvent,
    BroadcastSenderMode,
    Campaign,
    CampaignAccount,
    CampaignMode,
    CampaignRun,
    MessageTemplate,
    ProxyEndpoint,
    ProxyStatus,
    RunStatus,
    Target,
    TargetList,
    TargetListItem,
    ensure_utc,
    utc_now,
)
from proxima_traffic.proxies import (
    ProxyCheckResult,
    ProxyFilter,
    ProxyInputError,
    ProxyService,
    ProxyView,
)
from proxima_traffic.schemas import (
    AccountCreate,
    AccountRead,
    AccountUpdate,
    ActiveLaunchUpdate,
    ActiveLaunchUpdateRead,
    AuditEventRead,
    CampaignCreate,
    CampaignPreflightRead,
    CampaignRead,
    CampaignUpdate,
    DialogueStepPayload,
    LaunchArchiveRead,
    MessageTemplateCreate,
    MessageTemplateRead,
    MessageTemplateUpdate,
    PageRead,
    ProxyAllocationCreate,
    ProxyAssignmentCreate,
    ProxyBatchCreate,
    ProxyCheckRead,
    ProxyCreate,
    ProxyEndpointUpdate,
    ProxyGroupCreate,
    ProxyGroupRead,
    ProxyImportRead,
    ProxyMetadataUpdate,
    ProxyRead,
    ProxyRotationUpdate,
    RunCreate,
    RunDetails,
    RunRead,
    StatusRead,
    TargetCooldownUpdate,
    TargetCreate,
    TargetListCreate,
    TargetListRead,
    TargetRead,
    WorkerStatusRead,
)
from proxima_traffic.services import (
    ConflictError,
    NotFoundError,
    RunQueue,
    add_audit_event,
)
from proxima_traffic.target_import import (
    TargetReferenceError,
    TargetReferenceKind,
    parse_target_batch,
    parse_target_reference,
)


async def require_admin_key(
    request: Request,
    x_admin_key: str | None = Header(default=None),
) -> None:
    expected = request.app.state.settings.admin_api_key.get_secret_value()
    if x_admin_key is None or not compare_digest(x_admin_key, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid admin key")


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.database.session_factory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_admin_key)])


@router.get("/status", response_model=StatusRead)
async def get_status(request: Request, session: SessionDep) -> StatusRead:
    async def count(model: type[object]) -> int:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)

    async def count_runs(run_status: RunStatus) -> int:
        return int(
            await session.scalar(
                select(func.count())
                .select_from(CampaignRun)
                .where(CampaignRun.status == run_status)
            )
            or 0
        )

    approved_targets = int(
        await session.scalar(
            select(func.count(func.distinct(Target.id)))
            .select_from(Target)
            .outerjoin(TargetListItem, TargetListItem.target_id == Target.id)
            .where(
                or_(
                    Target.is_standalone.is_(True) & Target.approved_by_operator.is_(True),
                    TargetListItem.enabled.is_(True),
                )
            )
        )
        or 0
    )
    return StatusRead(
        dry_run=request.app.state.settings.dry_run,
        accounts=await count(Account),
        approved_targets=approved_targets,
        target_lists=await count(TargetList),
        templates=await count(MessageTemplate),
        campaigns=await count(Campaign),
        runs=await count(CampaignRun),
        queued_runs=await count_runs(RunStatus.QUEUED),
        failed_runs=await count_runs(RunStatus.FAILED),
        proxies=await count(ProxyEndpoint),
        healthy_proxies=int(
            await session.scalar(
                select(func.count())
                .select_from(ProxyEndpoint)
                .where(ProxyEndpoint.status == ProxyStatus.HEALTHY)
            )
            or 0
        ),
    )


@router.get("/worker", response_model=WorkerStatusRead)
async def get_worker_status(request: Request, session: SessionDep) -> WorkerStatusRead:
    snapshot = request.app.state.queue_worker.snapshot()
    background_tasks: dict[str, object] = getattr(request.app.state, "background_tasks", {})
    background_workers = {
        name: not task.done()  # type: ignore[union-attr]
        for name, task in background_tasks.items()
    }
    supervisor = getattr(request.app.state, "worker_supervisor", None)
    if supervisor is not None:
        background_workers.update(
            {item.name: item.state == "running" for item in supervisor.snapshots()}
        )
    background_healthy = not request.app.state.settings.api_worker_enabled or (
        bool(background_workers) and all(background_workers.values())
    )

    async def count_runs(status_value: RunStatus) -> int:
        return int(
            await session.scalar(
                select(func.count())
                .select_from(CampaignRun)
                .where(CampaignRun.status == status_value)
            )
            or 0
        )

    expired = int(
        await session.scalar(
            select(func.count())
            .select_from(CampaignRun)
            .where(
                CampaignRun.status == RunStatus.RUNNING,
                CampaignRun.lease_until.is_not(None),
                CampaignRun.lease_until < utc_now(),
            )
        )
        or 0
    )
    return WorkerStatusRead(
        worker_id=snapshot.worker_id,
        running=snapshot.running,
        healthy=background_healthy,
        background_workers=background_workers,
        active_steps=snapshot.active_steps,
        processed_steps=snapshot.processed_steps,
        failed_steps=snapshot.failed_steps,
        last_error=snapshot.last_error,
        last_processed_at=snapshot.last_processed_at,
        queued_runs=await count_runs(RunStatus.QUEUED),
        running_runs=await count_runs(RunStatus.RUNNING),
        expired_leases=expired,
    )


@router.post("/proxy-groups", response_model=ProxyGroupRead, status_code=status.HTTP_201_CREATED)
async def create_proxy_group(payload: ProxyGroupCreate, request: Request):
    service: ProxyService = request.app.state.proxy_service
    try:
        return await service.create_group(payload.name, payload.description)
    except (ProxyInputError, ConflictError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/proxy-groups", response_model=list[ProxyGroupRead])
async def list_proxy_groups(request: Request):
    service: ProxyService = request.app.state.proxy_service
    return await service.list_groups()


@router.delete("/proxy-groups/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_proxy_group(group_id: int, request: Request) -> None:
    service: ProxyService = request.app.state.proxy_service
    try:
        await service.delete_group(group_id, actor="admin-api")
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/proxies", response_model=ProxyRead, status_code=status.HTTP_201_CREATED)
async def create_proxy(payload: ProxyCreate, request: Request) -> ProxyRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        view = await service.add_proxy(
            payload.url,
            label=payload.label,
            group_name=payload.group_name,
            rotation_url=payload.rotation_url,
            actor="admin-api",
        )
    except ProxyInputError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _proxy_read(view)


@router.post("/proxies/import", response_model=ProxyImportRead)
async def import_proxies(payload: ProxyBatchCreate, request: Request) -> ProxyImportRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        result = await service.import_proxies(
            payload.values, group_name=payload.group_name, actor="admin-api"
        )
    except ProxyInputError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return ProxyImportRead(
        added=result.added,
        skipped=result.skipped,
        invalid=result.invalid,
        errors=list(result.errors),
    )


@router.get("/proxies", response_model=PageRead[ProxyRead])
async def list_proxies(
    request: Request,
    proxy_status: ProxyStatus | None = Query(default=None, alias="status"),
    proxy_filter: ProxyFilter = Query(default=ProxyFilter.ALL, alias="filter"),
    group_id: int | None = None,
    page: int = Query(default=0, ge=0, le=1_000_000),
    page_size: int = Query(default=100, ge=1, le=500),
) -> PageRead[ProxyRead]:
    service: ProxyService = request.app.state.proxy_service
    results = await service.list_proxies(
        page,
        page_size,
        status=proxy_status,
        group_id=group_id,
        filter_name=proxy_filter,
    )
    return PageRead(
        items=[_proxy_read(item) for item in results.items],
        total=results.total,
        page=page,
        page_size=page_size,
    )


@router.get("/proxies/{proxy_id}", response_model=ProxyRead)
async def get_proxy(proxy_id: int, request: Request) -> ProxyRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        return _proxy_read(await service.get_proxy(proxy_id))
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/proxies/{proxy_id}/toggle", response_model=ProxyRead)
async def toggle_proxy(proxy_id: int, request: Request) -> ProxyRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        view = await service.toggle_proxy(proxy_id, actor="admin-api")
        await _close_rotated_account_client(view, request.app.state.gateway)
        return _proxy_read(view)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/proxies/{proxy_id}/check", response_model=ProxyCheckRead)
async def check_proxy(proxy_id: int, request: Request) -> ProxyCheckRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        result = await service.check_proxy(proxy_id, actor="admin-api")
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ConflictError, ProxyInputError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _proxy_check_read(result)


@router.post("/proxy-checks", response_model=list[ProxyCheckRead])
async def check_all_proxies(request: Request) -> list[ProxyCheckRead]:
    service: ProxyService = request.app.state.proxy_service
    results = await service.check_all(actor="admin-api")
    return [_proxy_check_read(result) for result in results]


@router.post("/proxy-allocations", response_model=ProxyRead)
async def allocate_proxy(payload: ProxyAllocationCreate, request: Request) -> ProxyRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        view = await service.allocate_proxy(
            payload.assignment_key,
            payload.purpose,
            group_name=payload.group_name,
            actor="admin-api",
        )
        await _close_assignment_account_client(payload.assignment_key, request.app.state.gateway)
    except (ConflictError, ProxyInputError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _proxy_read(view)


@router.put("/proxies/{proxy_id}/endpoint", response_model=ProxyRead)
async def replace_proxy_endpoint(
    proxy_id: int, payload: ProxyEndpointUpdate, request: Request
) -> ProxyRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        view = await service.replace_proxy_url(proxy_id, payload.url, actor="admin-api")
        await _close_rotated_account_client(view, request.app.state.gateway)
        return _proxy_read(view)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ConflictError, ProxyInputError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.put("/proxies/{proxy_id}/rotation-url", response_model=ProxyRead)
async def set_proxy_rotation_url(
    proxy_id: int, payload: ProxyRotationUpdate, request: Request
) -> ProxyRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        return _proxy_read(
            await service.set_proxy_rotation_url(proxy_id, payload.rotation_url, actor="admin-api")
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ProxyInputError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/proxies/{proxy_id}/rotate", response_model=ProxyRead)
async def rotate_proxy(proxy_id: int, request: Request) -> ProxyRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        before = await service.get_proxy(proxy_id)
        await _close_rotated_account_client(before, request.app.state.gateway)
        view = await service.rotate_proxy(proxy_id, actor="admin-api")
        return _proxy_read(view)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.put("/proxies/{proxy_id}/metadata", response_model=ProxyRead)
async def update_proxy_metadata(
    proxy_id: int, payload: ProxyMetadataUpdate, request: Request
) -> ProxyRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        return _proxy_read(
            await service.update_proxy_metadata(
                proxy_id,
                label=payload.label,
                group_name=payload.group_name,
                actor="admin-api",
            )
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ProxyInputError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.put("/proxies/{proxy_id}/assignment", response_model=ProxyRead)
async def assign_proxy(
    proxy_id: int, payload: ProxyAssignmentCreate, request: Request
) -> ProxyRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        view = await service.assign_proxy(
            proxy_id, payload.assignment_key, payload.purpose, actor="admin-api"
        )
        await _close_assignment_account_client(payload.assignment_key, request.app.state.gateway)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ConflictError, ProxyInputError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _proxy_read(view)


@router.delete("/proxies/{proxy_id}/assignment", response_model=ProxyRead)
async def release_proxy(proxy_id: int, request: Request) -> ProxyRead:
    service: ProxyService = request.app.state.proxy_service
    try:
        before = await service.get_proxy(proxy_id)
        view = await service.release_proxy(proxy_id, actor="admin-api")
        await _close_rotated_account_client(before, request.app.state.gateway)
        return _proxy_read(view)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete("/proxies/{proxy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_proxy(proxy_id: int, request: Request) -> None:
    service: ProxyService = request.app.state.proxy_service
    try:
        await service.delete_proxy(proxy_id, actor="admin-api")
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/accounts", response_model=AccountRead, status_code=status.HTTP_201_CREATED)
async def create_account(payload: AccountCreate, session: SessionDep) -> Account:
    account = Account(**payload.model_dump())
    session.add(account)
    await _flush_or_conflict(session)
    add_audit_event(
        session,
        event_type="account.created",
        entity_type="account",
        entity_id=account.id,
        details={"label": account.label, "ownership_confirmed": account.ownership_confirmed},
    )
    await _commit_or_conflict(session)
    await session.refresh(account)
    return account


@router.get("/accounts", response_model=PageRead[AccountRead])
async def list_accounts(
    session: SessionDep,
    page: int = Query(default=0, ge=0, le=1_000_000),
    page_size: int = Query(default=100, ge=1, le=500),
) -> PageRead[AccountRead]:
    total = int(await session.scalar(select(func.count(Account.id))) or 0)
    items = list(
        await session.scalars(
            select(Account).order_by(Account.id).offset(page * page_size).limit(page_size)
        )
    )
    return PageRead(items=items, total=total, page=page, page_size=page_size)


@router.patch("/accounts/{account_id}", response_model=AccountRead)
async def update_account(
    account_id: int,
    payload: AccountUpdate,
    session: SessionDep,
) -> Account:
    account = await locked_account(session, account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="account not found")
    changes = payload.model_dump(exclude_unset=True)
    if changes.get("status") == AccountStatus.ACTIVE:
        cooldown = ensure_utc(account.cooldown_until)
        if account.cooldown_reason in ACCOUNT_PROTECTED_PAUSE_REASONS or (
            cooldown and cooldown > utc_now()
        ):
            raise HTTPException(
                status_code=409, detail="Telegram protective pause requires a fresh account check"
            )
        if account.status == AccountStatus.DISABLED and not account.authorized:
            raise HTTPException(
                status_code=409, detail="Telegram session must be checked or reimported first"
            )
    if "status" in changes:
        account.operator_paused = changes["status"] != AccountStatus.ACTIVE
    for field, value in changes.items():
        setattr(account, field, value)
    add_audit_event(
        session,
        event_type="account.updated",
        entity_type="account",
        entity_id=account.id,
        details=changes,
    )
    await session.commit()
    await session.refresh(account)
    return account


@router.post("/targets", response_model=TargetRead, status_code=status.HTTP_201_CREATED)
async def create_target(payload: TargetCreate, session: SessionDep) -> Target:
    try:
        parsed = parse_target_reference(payload.chat_ref)
    except TargetReferenceError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if parsed.kind == TargetReferenceKind.CHAT_FOLDER:
        raise HTTPException(
            status_code=422,
            detail="chat folder links must be expanded by an authorized user connector",
        )
    existing = await session.scalar(select(Target).where(Target.chat_ref == parsed.canonical))
    requested_title = (payload.title or "").strip()[:150]
    if existing is not None:
        if existing.is_standalone:
            raise HTTPException(status_code=409, detail="target already exists")
        existing.is_standalone = True
        if requested_title:
            existing.title = requested_title
        add_audit_event(
            session,
            event_type="target.promoted_to_standalone",
            entity_type="target",
            entity_id=existing.id,
            details={"chat_ref": existing.chat_ref},
        )
        await session.commit()
        await session.refresh(existing)
        return existing
    target = Target(
        chat_ref=parsed.canonical,
        title=requested_title or parsed.canonical,
        is_standalone=True,
    )
    session.add(target)
    await _flush_or_conflict(session)
    add_audit_event(
        session,
        event_type="target.created",
        entity_type="target",
        entity_id=target.id,
        details={"chat_ref": target.chat_ref},
    )
    await _commit_or_conflict(session)
    await session.refresh(target)
    return target


@router.get("/targets", response_model=PageRead[TargetRead])
async def list_targets(
    session: SessionDep,
    include_list_members: bool = Query(default=False),
    target_list_id: int | None = Query(default=None),
    page: int = Query(default=0, ge=0, le=1_000_000),
    page_size: int = Query(default=100, ge=1, le=500),
) -> PageRead[TargetRead]:
    if target_list_id is not None:
        list_exists = await session.get(TargetList, target_list_id)
        if list_exists is None:
            raise HTTPException(status_code=404, detail="target list not found")
        statement = (
            select(Target, TargetListItem.enabled)
            .join(TargetListItem, TargetListItem.target_id == Target.id)
            .where(TargetListItem.target_list_id == target_list_id)
            .order_by(Target.id.desc())
        )
        total = int(
            await session.scalar(
                select(func.count(TargetListItem.target_id)).where(
                    TargetListItem.target_list_id == target_list_id
                )
            )
            or 0
        )
        rows = (await session.execute(statement.offset(page * page_size).limit(page_size))).all()
        return PageRead(
            items=[
                TargetRead.model_validate(target).model_copy(update={"list_enabled": bool(enabled)})
                for target, enabled in rows
            ],
            total=total,
            page=page,
            page_size=page_size,
        )
    statement = select(Target).order_by(Target.id.desc())
    count_statement = select(func.count(Target.id))
    if not include_list_members:
        statement = statement.where(Target.is_standalone.is_(True))
        count_statement = count_statement.where(Target.is_standalone.is_(True))
    total = int(await session.scalar(count_statement) or 0)
    items = list(await session.scalars(statement.offset(page * page_size).limit(page_size)))
    return PageRead(items=items, total=total, page=page, page_size=page_size)


@router.post(
    "/target-lists",
    response_model=TargetListRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_target_list(payload: TargetListCreate, request: Request) -> TargetListRead:
    parsed = parse_target_batch(payload.chat_refs)
    if parsed.folders:
        raise HTTPException(
            status_code=422,
            detail=(
                "chat folder links require an authorized user connector "
                "and cannot be expanded by Bot API"
            ),
        )
    if parsed.issues:
        issue = parsed.issues[0]
        raise HTTPException(
            status_code=422,
            detail=f"line {issue.line}: {issue.reason}",
        )
    if not parsed.targets:
        raise HTTPException(status_code=422, detail="target list contains no valid targets")
    repository = AdminRepository(request.app.state.database.session_factory)
    try:
        target_list, _, _ = await repository.create_target_list(
            name=payload.name,
            chat_refs=parsed.canonical_targets,
        )
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    view = await repository.get_target_list_view(target_list.id)
    return TargetListRead(
        id=view.id,
        name=view.name,
        item_count=view.item_count,
        approved_count=view.approved_count,
    )


@router.get("/target-lists", response_model=PageRead[TargetListRead])
async def list_target_lists(
    request: Request,
    page: int = Query(default=0, ge=0, le=1_000_000),
    page_size: int = Query(default=100, ge=1, le=500),
) -> PageRead[TargetListRead]:
    repository = AdminRepository(request.app.state.database.session_factory)
    result = await repository.list_target_lists(page, page_size)
    return PageRead(
        items=[
            TargetListRead(
                id=item.id,
                name=item.name,
                item_count=item.item_count,
                approved_count=item.approved_count,
            )
            for item in result.items
        ],
        total=result.total,
        page=result.page,
        page_size=result.page_size,
    )


@router.post("/target-lists/{target_list_id}/approve", response_model=TargetListRead)
async def approve_target_list(target_list_id: int, request: Request) -> TargetListRead:
    return await _set_target_list_approval(request, target_list_id, approved=True)


@router.post("/target-lists/{target_list_id}/revoke", response_model=TargetListRead)
async def revoke_target_list(target_list_id: int, request: Request) -> TargetListRead:
    return await _set_target_list_approval(request, target_list_id, approved=False)


@router.post("/targets/{target_id}/approve", response_model=TargetRead)
async def approve_target(target_id: int, session: SessionDep) -> Target:
    return await _set_target_approval(session, target_id, approved=True)


@router.post("/targets/{target_id}/revoke", response_model=TargetRead)
async def revoke_target(target_id: int, session: SessionDep) -> Target:
    return await _set_target_approval(session, target_id, approved=False)


@router.patch("/targets/{target_id}/cooldown", response_model=TargetRead)
async def update_target_cooldown(
    target_id: int,
    payload: TargetCooldownUpdate,
    request: Request,
) -> Target:
    repository = AdminRepository(request.app.state.database.session_factory)
    try:
        return await repository.set_target_manual_cooldown(
            target_id,
            payload.manual_cooldown_seconds,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post(
    "/templates",
    response_model=MessageTemplateRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_template(
    payload: MessageTemplateCreate,
    session: SessionDep,
) -> MessageTemplate:
    template = MessageTemplate(**payload.model_dump())
    session.add(template)
    await _flush_or_conflict(session)
    add_audit_event(
        session,
        event_type="template.created",
        entity_type="message_template",
        entity_id=template.id,
        details={"name": template.name},
    )
    await _commit_or_conflict(session)
    await session.refresh(template)
    return template


@router.get("/templates", response_model=PageRead[MessageTemplateRead])
async def list_templates(
    session: SessionDep,
    page: int = Query(default=0, ge=0, le=1_000_000),
    page_size: int = Query(default=100, ge=1, le=500),
) -> PageRead[MessageTemplateRead]:
    total = int(await session.scalar(select(func.count(MessageTemplate.id))) or 0)
    items = list(
        await session.scalars(
            select(MessageTemplate)
            .order_by(MessageTemplate.id)
            .offset(page * page_size)
            .limit(page_size)
        )
    )
    return PageRead(items=items, total=total, page=page, page_size=page_size)


@router.patch("/templates/{template_id}", response_model=MessageTemplateRead)
async def update_template(
    template_id: int,
    payload: MessageTemplateUpdate,
    session: SessionDep,
) -> MessageTemplate:
    template = await session.get(MessageTemplate, template_id)
    if template is None:
        raise HTTPException(status_code=404, detail="template not found")
    changes = payload.model_dump(exclude_unset=True)
    for field, value in changes.items():
        setattr(template, field, value)
    add_audit_event(
        session,
        event_type="template.updated",
        entity_type="message_template",
        entity_id=template.id,
        details=changes,
    )
    await _commit_or_conflict(session)
    await session.refresh(template)
    return template


@router.post("/campaigns", response_model=CampaignRead, status_code=status.HTTP_201_CREATED)
async def create_campaign(payload: CampaignCreate, session: SessionDep) -> CampaignRead:
    values = payload.model_dump(exclude={"sender_account_ids", "dialogue_steps"})
    if payload.mode == CampaignMode.DIALOGUE and payload.dialogue_steps:
        steps = validate_dialogue_steps([step.model_dump() for step in payload.dialogue_steps])
        values["dialogue_steps_json"] = serialize_dialogue_steps(steps)
    campaign = Campaign(**values)
    session.add(campaign)
    try:
        await session.flush()
        if payload.mode == CampaignMode.BROADCAST:
            sender_ids = _broadcast_sender_ids(
                payload.primary_account_id, payload.sender_account_ids
            )
            session.add_all(
                CampaignAccount(campaign_id=campaign.id, account_id=account_id)
                for account_id in sender_ids
            )
            await session.flush()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail="invalid campaign relations") from exc
    add_audit_event(
        session,
        event_type="campaign.created",
        entity_type="campaign",
        entity_id=campaign.id,
        details={
            "name": campaign.name,
            "mode": campaign.mode.value,
            "target_id": campaign.target_id,
            "target_list_id": campaign.target_list_id,
            "spread_seconds": campaign.spread_seconds,
            "repeat_count": campaign.repeat_count,
            "repeat_interval_seconds": campaign.repeat_interval_seconds,
            "smart_timer_mode": campaign.smart_timer_mode,
            "repeat_forever": campaign.repeat_forever,
            "smart_duration_seconds": campaign.smart_duration_seconds,
            "smart_no_timer_interval_seconds": campaign.smart_no_timer_interval_seconds,
            "auto_sender_failover": campaign.auto_sender_failover,
            "sender_mode": campaign.sender_mode.value,
            "activity_aware": campaign.activity_aware,
            "min_messages_between_posts": campaign.min_messages_between_posts,
        },
    )
    await _commit_or_conflict(session)
    await session.refresh(campaign)
    return await _campaign_read(session, campaign)


@router.get("/campaigns", response_model=PageRead[CampaignRead])
async def list_campaigns(
    session: SessionDep,
    page: int = Query(default=0, ge=0, le=1_000_000),
    page_size: int = Query(default=100, ge=1, le=500),
) -> PageRead[CampaignRead]:
    total = int(await session.scalar(select(func.count(Campaign.id))) or 0)
    campaigns = list(
        await session.scalars(
            select(Campaign).order_by(Campaign.id).offset(page * page_size).limit(page_size)
        )
    )
    broadcast_ids = [
        campaign.id for campaign in campaigns if campaign.mode == CampaignMode.BROADCAST
    ]
    sender_ids_by_campaign: dict[int, list[int]] = {}
    if broadcast_ids:
        rows = await session.execute(
            select(CampaignAccount.campaign_id, CampaignAccount.account_id)
            .where(CampaignAccount.campaign_id.in_(broadcast_ids))
            .order_by(CampaignAccount.campaign_id, CampaignAccount.id)
        )
        for campaign_id, account_id in rows:
            sender_ids_by_campaign.setdefault(campaign_id, []).append(account_id)
    return PageRead(
        items=[
            await _campaign_read(
                session,
                campaign,
                sender_ids=sender_ids_by_campaign.get(campaign.id),
            )
            for campaign in campaigns
        ],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post("/campaigns/{campaign_id}/preflight", response_model=CampaignPreflightRead)
async def preflight_campaign(campaign_id: int, request: Request) -> CampaignPreflightRead:
    try:
        summary = await request.app.state.preflight_service.inspect_campaign(campaign_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return CampaignPreflightRead(
        campaign_id=summary.campaign_id,
        account_count=summary.account_count,
        target_count=summary.target_count,
        checked_pairs=summary.checked_pairs,
        ready_pairs=summary.ready_pairs,
        routable_pairs=summary.routable_pairs,
        joinable_pairs=summary.joinable_pairs,
        planned_join_attempts=summary.planned_join_attempts,
        ready_now_pairs=summary.ready_now_pairs or 0,
        ready_now_targets=summary.ready_now_targets or 0,
        deferred_pairs=summary.deferred_pairs,
        earliest_resume_at=summary.earliest_resume_at,
        independent_mode=summary.independent_mode,
        requires_all_accounts=summary.requires_all_accounts,
        member_targets=summary.member_targets,
        joinable_targets=summary.joinable_targets,
        blocked_targets=summary.blocked_targets,
        unavailable_targets=summary.unavailable_targets,
        uncovered_targets=summary.uncovered_targets,
        oldest_checked_at=summary.oldest_checked_at,
        newest_checked_at=summary.newest_checked_at,
    )


@router.patch("/campaigns/{campaign_id}", response_model=CampaignRead)
async def update_campaign(
    campaign_id: int,
    payload: CampaignUpdate,
    session: SessionDep,
) -> CampaignRead:
    campaign = await session.get(Campaign, campaign_id)
    if campaign is None:
        raise HTTPException(status_code=404, detail="campaign not found")
    changes = payload.model_dump(exclude_unset=True)
    if (
        campaign.mode == CampaignMode.BROADCAST
        and "primary_text" in changes
        and "primary_html" not in changes
    ):
        changes["primary_html"] = None
    sender_account_ids_provided = "sender_account_ids" in changes
    sender_account_ids = changes.pop("sender_account_ids", None)
    dialogue_steps_provided = "dialogue_steps" in changes
    raw_dialogue_steps = changes.pop("dialogue_steps", None)
    normalized_dialogue_steps = None
    if dialogue_steps_provided:
        if campaign.mode != CampaignMode.DIALOGUE:
            raise HTTPException(
                status_code=422,
                detail="цепочку dialogue_steps можно использовать только в диалоге",
            )
        try:
            normalized_dialogue_steps = validate_dialogue_steps(list(raw_dialogue_steps or []))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        changes.update(
            primary_account_id=normalized_dialogue_steps[0].account_id,
            reply_account_id=normalized_dialogue_steps[1].account_id,
            primary_text=normalized_dialogue_steps[0].text,
            reply_text=normalized_dialogue_steps[1].text,
            reply_delay_seconds=normalized_dialogue_steps[1].delay_seconds,
        )
    if (
        campaign.mode == CampaignMode.DIALOGUE
        and campaign.dialogue_steps_json is not None
        and not dialogue_steps_provided
        and {"primary_text", "reply_text", "reply_delay_seconds"}.intersection(changes)
    ):
        raise HTTPException(
            status_code=422,
            detail="передайте всю цепочку диалога целиком в поле dialogue_steps",
        )
    if changes.get("sender_mode") == BroadcastSenderMode.INDEPENDENT:
        changes["auto_sender_failover"] = False
    _validate_campaign_update(campaign, changes)
    content_variants_json = _updated_primary_content_variants(campaign, changes)
    if sender_account_ids_provided and campaign.mode != CampaignMode.BROADCAST:
        raise HTTPException(
            status_code=422,
            detail="sender_account_ids are only supported for broadcast campaigns",
        )
    for field, value in changes.items():
        setattr(campaign, field, value)
    if content_variants_json is not None:
        campaign.content_variants_json = content_variants_json
    if normalized_dialogue_steps is not None:
        campaign.dialogue_steps_json = serialize_dialogue_steps(normalized_dialogue_steps)
    sender_ids: list[int] | None = None
    if sender_account_ids_provided:
        sender_ids = await _replace_broadcast_sender_pool(
            session, campaign, list(sender_account_ids or [])
        )
    details = dict(changes)
    if sender_ids is not None:
        details["sender_account_ids"] = sender_ids
    if normalized_dialogue_steps is not None:
        details["dialogue_step_count"] = len(normalized_dialogue_steps)
    add_audit_event(
        session,
        event_type="campaign.updated",
        entity_type="campaign",
        entity_id=campaign.id,
        details=details,
    )
    await _commit_or_conflict(session)
    await session.refresh(campaign)
    return await _campaign_read(session, campaign)


@router.post(
    "/campaigns/{campaign_id}/runs",
    response_model=list[RunRead],
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_campaign(
    campaign_id: int, payload: RunCreate, request: Request
) -> list[CampaignRun]:
    queue: RunQueue = request.app.state.run_queue
    try:
        return await queue.enqueue_campaign(
            campaign_id,
            payload.request_key,
            allow_deferred_start=payload.allow_deferred_start,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/runs", response_model=PageRead[RunRead])
async def list_runs(
    session: SessionDep,
    run_status: RunStatus | None = Query(default=None, alias="status"),
    page: int = Query(default=0, ge=0, le=1_000_000),
    page_size: int = Query(default=100, ge=1, le=500),
) -> PageRead[RunRead]:
    statement = select(CampaignRun).order_by(CampaignRun.id.desc())
    count_statement = select(func.count(CampaignRun.id))
    if run_status is not None:
        statement = statement.where(CampaignRun.status == run_status)
        count_statement = count_statement.where(CampaignRun.status == run_status)
    return PageRead(
        items=list(await session.scalars(statement.offset(page * page_size).limit(page_size))),
        total=int(await session.scalar(count_statement) or 0),
        page=page,
        page_size=page_size,
    )


@router.post("/runs/{run_id}/cancel", response_model=RunRead)
async def cancel_run(run_id: int, request: Request) -> CampaignRun:
    queue: RunQueue = request.app.state.run_queue
    try:
        return await queue.cancel(run_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.patch(
    "/launches/{anchor_run_id}",
    response_model=ActiveLaunchUpdateRead,
)
async def update_active_launch(
    anchor_run_id: int,
    payload: ActiveLaunchUpdate,
    request: Request,
) -> ActiveLaunchUpdateRead:
    queue: RunQueue = request.app.state.run_queue
    try:
        updated = await queue.update_active_launch(
            anchor_run_id,
            **payload.model_dump(exclude_unset=True),
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return ActiveLaunchUpdateRead(
        anchor_run_id=anchor_run_id,
        updated_tasks=updated,
    )


@router.post(
    "/launches/{anchor_run_id}/archive",
    response_model=LaunchArchiveRead,
)
async def archive_launch(anchor_run_id: int, request: Request) -> LaunchArchiveRead:
    repository = AdminRepository(request.app.state.database.session_factory)
    try:
        updated = await repository.set_launch_archived(anchor_run_id, True)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return LaunchArchiveRead(
        anchor_run_id=anchor_run_id,
        archived=True,
        updated_tasks=updated,
    )


@router.post(
    "/launches/{anchor_run_id}/restore",
    response_model=LaunchArchiveRead,
)
async def restore_launch(anchor_run_id: int, request: Request) -> LaunchArchiveRead:
    repository = AdminRepository(request.app.state.database.session_factory)
    try:
        updated = await repository.set_launch_archived(anchor_run_id, False)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return LaunchArchiveRead(
        anchor_run_id=anchor_run_id,
        archived=False,
        updated_tasks=updated,
    )


@router.get("/runs/{run_id}", response_model=RunDetails)
async def get_run(run_id: int, session: SessionDep) -> RunDetails:
    run = await session.get(CampaignRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    actions = list(
        await session.scalars(
            select(ActionLog).where(ActionLog.run_id == run_id).order_by(ActionLog.id)
        )
    )
    return RunDetails(**RunRead.model_validate(run).model_dump(), actions=actions)


@router.get("/audit", response_model=PageRead[AuditEventRead])
async def list_audit_events(
    session: SessionDep,
    page: int = Query(default=0, ge=0, le=1_000_000),
    page_size: int = Query(default=100, ge=1, le=500),
) -> PageRead[AuditEventRead]:
    return PageRead(
        items=list(
            await session.scalars(
                select(AuditEvent)
                .order_by(AuditEvent.id.desc())
                .offset(page * page_size)
                .limit(page_size)
            )
        ),
        total=int(await session.scalar(select(func.count(AuditEvent.id))) or 0),
        page=page,
        page_size=page_size,
    )


async def _set_target_approval(
    session: AsyncSession,
    target_id: int,
    *,
    approved: bool,
) -> Target:
    target = await session.get(Target, target_id)
    if target is None:
        raise HTTPException(status_code=404, detail="target not found")
    target.approved_by_operator = approved
    target.approved_at = utc_now() if approved else None
    add_audit_event(
        session,
        event_type="target.approved" if approved else "target.revoked",
        entity_type="target",
        entity_id=target.id,
        details={"chat_ref": target.chat_ref},
    )
    await session.commit()
    await session.refresh(target)
    return target


async def _set_target_list_approval(
    request: Request,
    target_list_id: int,
    *,
    approved: bool,
) -> TargetListRead:
    repository = AdminRepository(request.app.state.database.session_factory)
    try:
        await repository.set_target_list_approval(target_list_id, approved)
        view = await repository.get_target_list_view(target_list_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TargetListRead(
        id=view.id,
        name=view.name,
        item_count=view.item_count,
        approved_count=view.approved_count,
    )


async def _commit_or_conflict(session: AsyncSession) -> None:
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409, detail="resource conflicts with existing data"
        ) from exc


async def _flush_or_conflict(session: AsyncSession) -> None:
    try:
        await session.flush()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409, detail="resource conflicts with existing data"
        ) from exc


def _broadcast_sender_ids(primary_account_id: int, sender_account_ids: list[int]) -> list[int]:
    return list(dict.fromkeys([primary_account_id, *sender_account_ids]))


async def _close_rotated_account_client(view: ProxyView, gateway: object) -> None:
    for assignment_key in view.assignment_keys:
        await _close_assignment_account_client(assignment_key, gateway)


async def _close_assignment_account_client(
    assignment_key: str,
    gateway: object,
) -> None:
    if not assignment_key.startswith("account:"):
        return
    close_account = getattr(gateway, "close_account", None)
    if close_account is None:
        return
    try:
        account_id = int(assignment_key.split(":", 1)[1])
    except ValueError:
        return
    await close_account(account_id)


async def _replace_broadcast_sender_pool(
    session: AsyncSession,
    campaign: Campaign,
    sender_account_ids: list[int],
) -> list[int]:
    sender_ids = _broadcast_sender_ids(campaign.primary_account_id, sender_account_ids)
    await session.execute(delete(CampaignAccount).where(CampaignAccount.campaign_id == campaign.id))
    session.add_all(
        CampaignAccount(campaign_id=campaign.id, account_id=account_id) for account_id in sender_ids
    )
    await _flush_or_conflict(session)
    return sender_ids


def _updated_primary_content_variants(
    campaign: Campaign,
    changes: dict[str, object],
) -> str | None:
    """Keep the denormalized primary fields and the first variant atomic."""

    if campaign.mode != CampaignMode.BROADCAST or not campaign.content_variants_json:
        return None
    if not {"primary_text", "primary_html"}.intersection(changes):
        return None
    try:
        variants = parse_content_variants(
            campaign.content_variants_json,
            fallback_text=campaign.primary_text,
            fallback_html=campaign.primary_html,
            fallback_media_path=campaign.primary_media_path,
            fallback_media_kind=campaign.primary_media_kind,
            fallback_media_name=campaign.primary_media_name,
            fallback_media_size=campaign.primary_media_size,
        )
        primary = variants[0]
        if primary.contact is not None:
            raise ContentError("нельзя заменить текст основного варианта-контакта")
        text = str(changes.get("primary_text", campaign.primary_text))
        html_text = changes.get("primary_html", campaign.primary_html)
        if html_text is not None and not isinstance(html_text, str):
            raise ContentError("форматированный текст имеет неверный тип")
        updated = BroadcastContentVariant(
            variant_id=primary.variant_id,
            name=primary.name,
            text=text,
            html_text=html_text,
            media=primary.media,
            contact=None,
            enabled=primary.enabled,
        )
        variants[0] = updated
        return serialize_content_variants(variants)
    except ContentError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _validate_campaign_update(campaign: Campaign, changes: dict[str, object]) -> None:
    primary_text = changes.get("primary_text")
    if (
        campaign.mode != CampaignMode.DIALOGUE
        and isinstance(primary_text, str)
        and not primary_text.strip()
    ):
        raise HTTPException(status_code=422, detail="primary_text must not be blank")
    if campaign.mode == CampaignMode.DIALOGUE:
        if "sender_mode" in changes:
            raise HTTPException(
                status_code=422,
                detail="sender_mode is only supported for broadcast campaigns",
            )
        if "primary_html" in changes:
            raise HTTPException(
                status_code=422,
                detail="primary_html is only supported for broadcast campaigns",
            )
        if "hidden_mentions_enabled" in changes or "mention_cooldown_days" in changes:
            raise HTTPException(
                status_code=422,
                detail=(
                    "hidden_mentions_enabled and mention_cooldown_days are only supported "
                    "for broadcast campaigns"
                ),
            )
        if int(changes.get("spread_seconds") or 0) != 0:
            raise HTTPException(
                status_code=422,
                detail="spread_seconds is only supported for broadcast campaigns",
            )
        repeat_count = int(changes.get("repeat_count", campaign.repeat_count))
        repeat_interval = int(
            changes.get(
                "repeat_interval_seconds",
                campaign.repeat_interval_seconds,
            )
        )
        smart_timer_mode = bool(changes.get("smart_timer_mode", campaign.smart_timer_mode))
        repeat_forever = bool(changes.get("repeat_forever", campaign.repeat_forever))
        smart_duration_seconds = int(
            changes.get("smart_duration_seconds", campaign.smart_duration_seconds)
        )
        if (
            repeat_count != 1
            or repeat_interval != 0
            or bool(changes.get("auto_sender_failover", campaign.auto_sender_failover))
        ):
            raise HTTPException(
                status_code=422,
                detail="для диалога доступен один проход или умный режим",
            )
        if smart_timer_mode:
            if repeat_forever and smart_duration_seconds != 0:
                raise HTTPException(status_code=422, detail="repeat_forever requires zero duration")
            if not repeat_forever and not 60 <= smart_duration_seconds <= 7 * 24 * 3600:
                raise HTTPException(
                    status_code=422,
                    detail="smart duration must be between 1 minute and 7 days",
                )
            if int(changes.get("spread_seconds", campaign.spread_seconds)) != 0:
                raise HTTPException(
                    status_code=422,
                    detail="умный диалог нельзя распределять по временному окну",
                )
            activity_aware = bool(changes.get("activity_aware", campaign.activity_aware))
            minimum_messages = int(
                changes.get(
                    "min_messages_between_posts",
                    campaign.min_messages_between_posts,
                )
            )
            if activity_aware and minimum_messages < 1:
                raise HTTPException(
                    status_code=422,
                    detail="activity-aware mode requires min_messages_between_posts",
                )
        elif smart_duration_seconds != 0 or repeat_forever:
            raise HTTPException(
                status_code=422,
                detail="smart duration requires smart_timer_mode",
            )
        elif bool(changes.get("activity_aware", campaign.activity_aware)):
            raise HTTPException(
                status_code=422,
                detail="activity-aware mode requires smart_timer_mode",
            )
    elif campaign.mode == CampaignMode.BROADCAST:
        effective_primary_text = str(changes.get("primary_text", campaign.primary_text))
        if {"primary_text", "primary_html"}.intersection(changes):
            effective_primary_html = changes.get("primary_html", campaign.primary_html)
            if effective_primary_html is not None and not isinstance(effective_primary_html, str):
                raise HTTPException(status_code=422, detail="primary_html has an invalid type")
            try:
                validate_telegram_text(
                    effective_primary_text,
                    effective_primary_html,
                    has_media=campaign.primary_media_path is not None,
                )
            except ContentError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        if campaign.primary_media_path is not None and len(effective_primary_text) > 1024:
            raise HTTPException(
                status_code=422,
                detail="media caption must contain at most 1024 characters",
            )
        reply_text = changes.get("reply_text")
        if isinstance(reply_text, str) and reply_text.strip():
            raise HTTPException(
                status_code=422,
                detail="broadcast campaign must not set reply_text",
            )
        if int(changes.get("reply_delay_seconds") or 0) != 0:
            raise HTTPException(
                status_code=422,
                detail="задержку ответа можно использовать только в диалоге",
            )
        repeat_count = int(changes.get("repeat_count", campaign.repeat_count))
        repeat_interval = int(
            changes.get(
                "repeat_interval_seconds",
                campaign.repeat_interval_seconds,
            )
        )
        smart_timer_mode = bool(changes.get("smart_timer_mode", campaign.smart_timer_mode))
        repeat_forever = bool(changes.get("repeat_forever", campaign.repeat_forever))
        spread_seconds = int(changes.get("spread_seconds", campaign.spread_seconds))
        smart_duration_seconds = int(
            changes.get("smart_duration_seconds", campaign.smart_duration_seconds)
        )
        if smart_timer_mode:
            if repeat_count != 1:
                raise HTTPException(
                    status_code=422,
                    detail="timer-driven broadcasts must have repeat_count equal to 1",
                )
            if repeat_interval != 0 or spread_seconds != 0:
                raise HTTPException(
                    status_code=422,
                    detail="timer-driven broadcasts use dedicated smart timing fields",
                )
            if repeat_forever and smart_duration_seconds != 0:
                raise HTTPException(status_code=422, detail="repeat_forever requires zero duration")
            if not repeat_forever and not 60 <= smart_duration_seconds <= 7 * 24 * 3600:
                raise HTTPException(
                    status_code=422,
                    detail="smart duration must be between 1 minute and 7 days",
                )
            activity_aware = bool(changes.get("activity_aware", campaign.activity_aware))
            minimum_messages = int(
                changes.get(
                    "min_messages_between_posts",
                    campaign.min_messages_between_posts,
                )
            )
            if activity_aware and minimum_messages < 1:
                raise HTTPException(
                    status_code=422,
                    detail="activity-aware mode requires min_messages_between_posts",
                )
        elif repeat_count == 1 and repeat_interval != 0:
            raise HTTPException(
                status_code=422,
                detail="one-pass broadcast must have a zero repeat interval",
            )
        elif smart_duration_seconds != 0 or repeat_forever:
            raise HTTPException(
                status_code=422,
                detail="smart duration requires smart_timer_mode",
            )
        elif bool(changes.get("activity_aware", campaign.activity_aware)):
            raise HTTPException(
                status_code=422,
                detail="activity-aware mode requires smart_timer_mode",
            )
        if repeat_count > 1 and not 60 <= repeat_interval <= 7 * 24 * 3600:
            raise HTTPException(
                status_code=422,
                detail="repeat interval must be between 1 minute and 7 days",
            )


async def _campaign_read(
    session: AsyncSession,
    campaign: Campaign,
    *,
    sender_ids: list[int] | None = None,
) -> CampaignRead:
    if campaign.mode == CampaignMode.BROADCAST and sender_ids is None:
        sender_ids = list(
            await session.scalars(
                select(CampaignAccount.account_id)
                .where(CampaignAccount.campaign_id == campaign.id)
                .order_by(CampaignAccount.id)
            )
        )
    if campaign.mode == CampaignMode.BROADCAST and not sender_ids:
        sender_ids = [campaign.primary_account_id]
    if sender_ids is None:
        sender_ids = []
    dialogue_steps: list[DialogueStepPayload] = []
    content_variant_count = 1
    enabled_content_variant_count = 1
    if campaign.mode == CampaignMode.BROADCAST:
        try:
            variants = parse_content_variants(
                campaign.content_variants_json,
                fallback_text=campaign.primary_text,
                fallback_html=campaign.primary_html,
                fallback_media_path=campaign.primary_media_path,
                fallback_media_kind=campaign.primary_media_kind,
                fallback_media_name=campaign.primary_media_name,
                fallback_media_size=campaign.primary_media_size,
            )
        except ContentError:
            variants = []
        content_variant_count = len(variants)
        enabled_content_variant_count = sum(item.enabled for item in variants)
    if campaign.mode == CampaignMode.DIALOGUE:
        steps = parse_dialogue_steps(
            campaign.dialogue_steps_json,
            primary_account_id=campaign.primary_account_id,
            primary_text=campaign.primary_text,
            primary_html=campaign.primary_html,
            primary_media_path=campaign.primary_media_path,
            primary_media_kind=campaign.primary_media_kind,
            primary_media_name=campaign.primary_media_name,
            primary_media_size=campaign.primary_media_size,
            reply_account_id=campaign.reply_account_id,
            reply_text=campaign.reply_text,
            reply_delay_seconds=campaign.reply_delay_seconds,
        )
        dialogue_steps = [DialogueStepPayload(**step.as_dict()) for step in steps]
    return CampaignRead.model_validate(campaign).model_copy(
        update={
            "sender_account_ids": sender_ids,
            "dialogue_steps": dialogue_steps,
            "content_variant_count": content_variant_count,
            "enabled_content_variant_count": enabled_content_variant_count,
        }
    )


def _proxy_read(view: ProxyView) -> ProxyRead:
    proxy = view.proxy
    return ProxyRead(
        id=proxy.id,
        label=proxy.label,
        group_id=proxy.group_id,
        group_name=view.group_name,
        endpoint=view.masked_endpoint,
        enabled=proxy.enabled,
        status=proxy.status,
        success_count=proxy.success_count,
        failure_count=proxy.failure_count,
        consecutive_failures=proxy.consecutive_failures,
        latency_ms=proxy.latency_ms,
        observed_ip=proxy.observed_ip,
        last_error=proxy.last_error,
        last_checked_at=proxy.last_checked_at,
        has_rotation_url=view.has_rotation_url,
        last_rotated_at=proxy.last_rotated_at,
        assignment_key=view.assignment_key,
        assignment_purpose=view.assignment_purpose,
        assignment_keys=list(view.assignment_keys),
        assignment_count=view.assignment_count,
        created_at=proxy.created_at,
        updated_at=proxy.updated_at,
    )


def _proxy_check_read(result: ProxyCheckResult) -> ProxyCheckRead:
    return ProxyCheckRead(
        proxy_id=result.proxy_id,
        healthy=result.healthy,
        status=result.status,
        latency_ms=result.latency_ms,
        observed_ip=result.observed_ip,
        error=result.error,
    )
