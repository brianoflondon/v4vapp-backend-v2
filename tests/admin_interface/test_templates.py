import datetime

import pytest
from jinja2 import Environment, FileSystemLoader

pytestmark = pytest.mark.integration


def test_ledger_entries_template_compiles_and_renders():
    loader = FileSystemLoader("src/v4vapp_backend_v2/admin/templates")
    env = Environment(loader=loader)
    # Provide a minimal url_for stub used by base.html
    env.globals["url_for"] = lambda *args, **kwargs: "/static/fake"
    from v4vapp_backend_v2.accounting.sanity_checks import SanityCheckResults

    # Provide an empty sanity results object so base.html rendering doesn't fail
    env.globals["sanity_results"] = (SanityCheckResults(),)

    # Ensure the template parses and renders with a minimal context
    template = env.get_template("ledger_entries/entries.html")

    class DummyRequest:
        def __init__(self):
            self.query_params = {}

    # create a fake entry with reversed timestamp to exercise badge logic
    from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry

    entry = LedgerEntry(group_id="g1", short_id="s1")
    entry.reversed = datetime.datetime.utcnow()

    ctx = {
        "accounts_by_type": {},
        "account_string": "",
        "sub_filter": "",
        "as_of_date": datetime.datetime.utcnow(),
        "age_hours": 0,
        "line_items": True,
        "user_memos": True,
        "request": DummyRequest(),
        "nav_items": [],
        "pending_transactions": [],
        "title": "Test",
        "breadcrumbs": [],
        "entries": [entry],
    }

    rendered = template.render(**ctx)

    assert "Select Account" in rendered
    # New: ensure clear filters button is present
    assert 'id="clear-search"' in rendered
    # ensure new date range inputs and ledger type and general search fields are present
    assert 'name="from_date_str"' in rendered
    assert 'name="to_date_str"' in rendered
    assert 'name="ledger_type"' in rendered
    assert 'name="general_search"' in rendered
    # reversed badge should be visible
    assert "REVERSED" in rendered
