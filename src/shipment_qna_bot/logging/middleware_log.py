# src/shipment_qna_bot/logging/middleware_log.py

import time
import uuid
from typing import Awaitable, Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from .logger import logger, set_log_context


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """_summary_:Logs each incoming HTTP request and response with custom format.

    Args:
        BaseHTTPMiddleware (_type_): _description_:Attach a fresh trace_id for correlation.
    """

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:

        # generate a new unique trace_id per request
        trace_id = str(uuid.uuid4())

        # conversation_id can be in header or later in the body, here we grab it from header if present
        conversation_id = request.headers.get("X-Conversation-Id", "-")

        # set minimal logging context, intent & consignee can be filled later inside the graph/route
        set_log_context(trace_id=trace_id, conversation_id=conversation_id)

        start = time.perf_counter()

        # Log the incoming request
        logger.info(
            f"Incoming request: {request.method} {request.url.path}",
            extra={"step": "API:REQUEST"},
        )

        try:
            response = await call_next(request)
        except Exception as e:
            duration_ms = (time.perf_counter() - start) * 1000
            logger.error(
                f"Unhandled exception processing request: {request.method} {request.url.path} - {str(e)}",
                f"after {duration_ms: .1f}ms",
                extra={"step": "API:ERROR"},
                exc_info=True,
            )
            raise

        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            f"Completed request: {request.method} {request.url.path} with status={response.status_code}"
            f"in {duration_ms: .1f}ms",
            extra={"step": "API:RESPONSE"},
        )

        return response
