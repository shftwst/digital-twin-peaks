from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.crm.models import Customer, CustomerNote
from enterprise_twins.services.crm.schemas import (
    CustomerPage,
    CustomerView,
    NotePage,
    NoteView,
    decode_cursor,
    decode_note_cursor,
    encode_cursor,
    encode_note_cursor,
)


def customer_view(item: Customer) -> CustomerView:
    return CustomerView(
        customerId=item.customer_id,
        displayName=item.display_name,
        primaryEmail=item.primary_email,
        externalReference=item.external_reference,
        accountStatus=item.account_status,
        contactMethods=item.contact_methods,
        externalIdentifiers=item.external_identifiers,
        version=item.version,
    )


def note_view(item: CustomerNote) -> NoteView:
    return NoteView(
        noteId=item.note_id,
        customerId=item.customer_id,
        body=item.body,
        association=item.association,
        createdBy=item.created_by,
        createdAt=item.created_at,
        archived=item.archived,
        version=item.version,
    )


class CustomerRepository:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        cursor_secret: str,
    ) -> None:
        self.factory = factory
        self.cursor_secret = cursor_secret

    async def active_epoch(self, session: AsyncSession) -> str:
        state = await session.get(ScenarioState, 1)
        if state is None or state.mode != "active":
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "CRM scenario is not active",
                status_code=503,
                retryable=True,
            )
        return state.active_epoch

    async def search(
        self,
        *,
        email: str | None,
        external_reference: str | None,
        identifier: str | None,
        limit: int,
        after: str | None,
    ) -> CustomerPage:
        async with self.factory() as session:
            epoch = await self.active_epoch(session)
            statement = select(Customer).where(Customer.scenario_epoch == epoch)
            if email is not None:
                statement = statement.where(func.lower(Customer.primary_email) == email.casefold())
            if external_reference is not None:
                statement = statement.where(Customer.external_reference == external_reference)
            if identifier is not None:
                statement = statement.where(
                    (Customer.customer_id == identifier)
                    | Customer.external_identifiers.contains({"loyalty": identifier})
                )
            if after is not None:
                try:
                    boundary = decode_cursor(after, self.cursor_secret)
                except (ValueError, KeyError, TypeError) as error:
                    raise ApiError(
                        ErrorCode.INVALID_REQUEST,
                        "pagination cursor is invalid",
                        status_code=422,
                    ) from error
                statement = statement.where(Customer.customer_id > boundary)
            rows = list(
                await session.scalars(statement.order_by(Customer.customer_id).limit(limit + 1))
            )
            has_more = len(rows) > limit
            rows = rows[:limit]
            return CustomerPage(
                items=[customer_view(item) for item in rows],
                nextCursor=(
                    encode_cursor(rows[-1].customer_id, self.cursor_secret)
                    if has_more and rows
                    else None
                ),
            )

    async def get(self, customer_id: str) -> CustomerView:
        async with self.factory() as session:
            epoch = await self.active_epoch(session)
            customer = await session.scalar(
                select(Customer).where(
                    Customer.scenario_epoch == epoch,
                    Customer.customer_id == customer_id,
                )
            )
            if customer is None:
                raise ApiError(
                    ErrorCode.NOT_FOUND,
                    "customer was not found",
                    status_code=404,
                )
            return customer_view(customer)

    async def list_notes(
        self,
        customer_id: str,
        include_archived: bool,
        limit: int,
        after: str | None,
    ) -> NotePage:
        async with self.factory() as session:
            epoch = await self.active_epoch(session)
            customer_exists = await session.scalar(
                select(Customer.row_id).where(
                    Customer.scenario_epoch == epoch,
                    Customer.customer_id == customer_id,
                )
            )
            if customer_exists is None:
                raise ApiError(
                    ErrorCode.NOT_FOUND,
                    "customer was not found",
                    status_code=404,
                )
            statement = select(CustomerNote).where(
                CustomerNote.scenario_epoch == epoch,
                CustomerNote.customer_id == customer_id,
            )
            if not include_archived:
                statement = statement.where(CustomerNote.archived.is_(False))
            if after is not None:
                try:
                    created_at, note_id = decode_note_cursor(
                        after,
                        customer_id,
                        include_archived,
                        epoch,
                        self.cursor_secret,
                    )
                except (ValueError, KeyError, TypeError) as error:
                    raise ApiError(
                        ErrorCode.INVALID_REQUEST,
                        "pagination cursor is invalid",
                        status_code=422,
                    ) from error
                statement = statement.where(
                    (CustomerNote.created_at > created_at)
                    | ((CustomerNote.created_at == created_at) & (CustomerNote.note_id > note_id))
                )
            rows = list(
                await session.scalars(
                    statement.order_by(CustomerNote.created_at, CustomerNote.note_id).limit(
                        limit + 1
                    )
                )
            )
            has_more = len(rows) > limit
            rows = rows[:limit]
            return NotePage(
                items=[note_view(item) for item in rows],
                nextCursor=(
                    encode_note_cursor(
                        customer_id,
                        include_archived,
                        epoch,
                        rows[-1].created_at,
                        rows[-1].note_id,
                        self.cursor_secret,
                    )
                    if has_more and rows
                    else None
                ),
            )
