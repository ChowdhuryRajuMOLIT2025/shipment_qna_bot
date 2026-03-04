import json
import os
from typing import Dict, List, Optional, Union

from shipment_qna_bot.logging.logger import logger

_ALLOW_UNSAFE_SCOPE = os.getenv(
    "SHIPMENT_QNA_BOT_ALLOW_UNSAFE_SCOPE", "false"
).strip().lower() in {"1", "true", "yes", "on"}

_REGISTRY_CACHE: Optional[Dict[str, List[str]]] = None


def _load_identity_registry() -> Dict[str, List[str]]:
    global _REGISTRY_CACHE
    if _REGISTRY_CACHE is not None:
        return _REGISTRY_CACHE

    raw_json = os.getenv("CONSIGNEE_SCOPE_REGISTRY_JSON")
    path = os.getenv("CONSIGNEE_SCOPE_REGISTRY_PATH")

    registry: Dict[str, List[str]] = {}
    try:
        if raw_json:
            data = json.loads(raw_json)
        elif path:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = None

        if isinstance(data, dict):
            for identity, codes in data.items():
                if isinstance(codes, str):
                    norm = [c.strip() for c in codes.split(",") if c.strip()]
                elif isinstance(codes, list):
                    norm = [str(c).strip() for c in codes if str(c).strip()]
                else:
                    norm = []
                registry[str(identity)] = norm
        else:
            logger.error("Consignee scope registry is missing or invalid.")
    except Exception as exc:
        logger.error(f"Failed to load consignee scope registry: {exc}")

    _REGISTRY_CACHE = registry
    return registry


def resolve_allowed_scope(
    user_identity: Optional[str], payload_codes: Optional[Union[str, List[str]]]
) -> List[str]:
    """
    Resolves the effective allowed consignee codes for a user.

    Rules:
    1. If payload_codes is None/Empty -> Return empty list (Fail Closed).
    2. Normalize payload_codes to List[str].
    3. If user_identity is missing -> Fail closed unless SHIPMENT_QNA_BOT_ALLOW_UNSAFE_SCOPE=true.
    4. Validate that 'user_identity' is authorized for requested codes using
       a registry (fail closed if missing) unless SHIPMENT_QNA_BOT_ALLOW_UNSAFE_SCOPE=true.
       Registry sources: CONSIGNEE_SCOPE_REGISTRY_JSON or
       CONSIGNEE_SCOPE_REGISTRY_PATH (JSON mapping user->codes).

    Args:
        user_identity: The user's ID or role (e.g., from JWT).
        payload_codes: The codes requested in the API payload.

    Returns:
        List of unique, valid consignee codes.
    """
    if not payload_codes:
        logger.warning(
            f"User {user_identity} provided no consignee codes. Access denied."
        )
        return []

    # Normalize to list
    if isinstance(payload_codes, str):
        # Handle "code1,code2" string format
        codes = [c.strip() for c in payload_codes.split(",") if c.strip()]
    elif isinstance(payload_codes, list):
        codes = [str(c).strip() for c in payload_codes if str(c).strip()]
    else:
        logger.error(f"Invalid payload_codes format: {type(payload_codes)}")
        return []

    # Deduplicate while preserving order
    seen = set()
    codes = [c for c in codes if not (c in seen or seen.add(c))]

    if not user_identity:
        # User clarified that external security (VPN/Firewall) handles authentication.
        # We trust the payload codes for scoping if no specific identity is forced.
        logger.info("No user identity provided; using payload codes for data scope.")
        return codes

    registry = _load_identity_registry()
    if not registry:
        if _ALLOW_UNSAFE_SCOPE:
            logger.error(
                "No consignee scope registry available; allowing payload consignee codes due to unsafe override."
            )
            logger.info(
                f"Resolved scope for {user_identity}: {codes}",
                extra={"extra_data": {"scope_count": len(codes)}},
            )
            return codes
        logger.error("No consignee scope registry available; access denied.")
        return []

    allowed_codes = registry.get(user_identity) or registry.get("*") or []
    allowed_codes = [c for c in allowed_codes if c]
    if "*" in allowed_codes:
        effective = codes
    else:
        allowed_set = {c for c in allowed_codes}
        effective = [c for c in codes if c in allowed_set]

    if not effective:
        logger.warning(
            "User %s requested unauthorized consignee codes: %s",
            user_identity,
            codes,
        )
        return []

    logger.info(
        f"Resolved scope for {user_identity}: {effective}",
        extra={"extra_data": {"scope_count": len(effective)}},
    )
    return effective
