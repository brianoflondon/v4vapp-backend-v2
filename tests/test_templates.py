import datetime
from jinja2 import Environment, FileSystemLoader


def test_ledger_entries_template_compiles_and_renders():
    loader = FileSystemLoader("src/v4vapp_backend_v2/admin/templates")
    env = Environment(loader=loader)
    # Provide a minimal url_for stub used by base.html
    env.globals["url_for"] = lambda *args, **kwargs: "/static/fake"

    # Ensure the template parses and renders with a minimal context
    template = env.get_template("ledger_entries/entries.html")

    class DummyRequest:
        def __init__(self):
            self.query_params = {}

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
    }

    rendered = template.render(**ctx)

    assert "Select Account" in rendered
