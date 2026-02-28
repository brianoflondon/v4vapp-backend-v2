def test_dashboard_template_includes_force_threshold():
    """Ensure the admin HTML has a field for the new threshold."""
    path = "src/v4vapp_backend_v2/admin/templates/v4vconfig/dashboard.html"
    with open(path) as f:
        html = f.read()
    assert "force_custom_json_payment_sats" in html
    assert "Force custom_json threshold" in html
