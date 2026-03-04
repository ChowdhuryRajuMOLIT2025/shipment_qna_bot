from typing import List


def build_search_filter(
    allowed_codes: List[str], field_name: str = "consignee_code_ids"
) -> str:
    """
    Builds an OData filter string for Azure AI Search to enforce RLS.

    Args:
        allowed_codes: List of consignee codes the user is allowed to see.
        field_name: The name of the field in the Azure Search index (Collection(Edm.String)).

    Returns:
        A valid OData filter string. Returns "false" (no results) if allowed_codes is empty.
    """
    if not allowed_codes:
        return "false"

    # Azure Search OData syntax for Collection(Edm.String):
    # consignee_code_ids/any(c: c eq 'CODE1' or c eq 'CODE2' ...)
    # OR
    # search.in(consignee_code_ids, 'CODE1,CODE2', ',')
    # search.in is faster and cleaner for large lists.

    # Escape codes just in case (though they should be alphanumeric)
    safe_codes = [c.replace("'", "''") for c in allowed_codes]
    joined_codes = ",".join(safe_codes)

    # Using search.in for performance, the syntax is: consignee_code_ids/any(g: search.in(g, 'a,b,c'))
    # BUT for a collection field filter, the syntax is: consignee_code_ids/any(t: search.in(t, 'val1, val2'))

    return f"{field_name}/any(t: search.in(t, '{joined_codes}', ','))"
