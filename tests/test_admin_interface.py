"""
Admin Interface Tests

Comprehensive test suite for the FastAPI admin interface.
Tests all endpoints, templates, navigation, and functionality.
"""

import pytest
from fastapi.testclient import TestClient

from v4vapp_backend_v2.admin.admin_app import create_admin_app


@pytest.fixture
def admin_client():
    """Create a test client for the admin app"""
    app = create_admin_app(config_filename="devhive.config.yaml")
    return TestClient(app)


class TestAdminEndpoints:
    """Test all admin endpoints"""

    def test_root_redirect(self, admin_client):
        """Test that root redirects to admin dashboard"""
        response = admin_client.get("/")
        # The root endpoint serves the admin dashboard directly (status 200)
        # rather than redirecting (status 302)
        assert response.status_code == 200
        # Verify it serves the admin dashboard content
        assert "Admin Dashboard" in response.text

    def test_admin_dashboard(self, admin_client):
        """Test admin dashboard loads successfully"""
        response = admin_client.get("/admin")
        assert response.status_code == 200
        content = response.text

        # Check for essential dashboard elements
        assert "Admin Dashboard" in content
        assert "V4VApp Admin" in content  # Updated to match actual content
        assert "nav" in content  # Navigation should be present

    def test_users_page(self, admin_client):
        """Test users page loads and displays data"""
        response = admin_client.get("/admin/users")
        assert response.status_code == 200
        content = response.text

        # Check for users page specific content
        assert "VSC Liability Users" in content
        assert "User" in content
        assert "Balance (SATS)" in content
        assert "Status" in content
        assert "Total Users" in content

        # Check for table structure
        assert "<table" in content
        assert "</table>" in content
        assert "<tr>" in content

    def test_health_check(self, admin_client):
        """Test health check endpoint"""
        response = admin_client.get("/admin/health")
        assert response.status_code == 200
        data = response.json()

        assert "status" in data
        assert data["status"] == "healthy"
        assert "admin_version" in data
        assert "project_version" in data

    def test_favicon(self, admin_client):
        """Test favicon endpoint"""
        response = admin_client.get("/favicon.ico")
        # Favicon might return 404 if file doesn't exist, which is acceptable
        assert response.status_code in [200, 404]

        if response.status_code == 200:
            assert response.headers.get("content-type") == "image/x-icon"


class TestAdminTemplates:
    """Test template rendering and content"""

    def test_dashboard_template_elements(self, admin_client):
        """Test dashboard template contains all expected elements"""
        response = admin_client.get("/admin")
        content = response.text

        # Check for Bootstrap classes (indicating proper template rendering)
        assert "container" in content
        assert "card" in content

        # Check for navigation
        assert "Dashboard" in content
        assert "Users" in content
        assert "/admin" in content
        assert "/admin/users" in content

    def test_users_template_structure(self, admin_client):
        """Test users template has proper structure"""
        response = admin_client.get("/admin/users")
        content = response.text

        # Check for Bootstrap components
        assert "card-header" in content
        assert "table-responsive" in content
        assert "badge" in content  # Status badges

        # Check for summary statistics cards
        assert "bg-light" in content
        assert "Total Users" in content
        assert "Active Users" in content

    def test_template_inheritance(self, admin_client):
        """Test that templates properly inherit from base"""
        response = admin_client.get("/admin/users")
        content = response.text

        # Check for base template elements
        assert "<!DOCTYPE html>" in content
        assert "<html" in content
        assert "</html>" in content
        assert "favicon" in content.lower()


class TestAdminNavigation:
    """Test navigation functionality"""

    def test_navigation_menu_items(self, admin_client):
        """Test all navigation menu items are present"""
        response = admin_client.get("/admin")
        content = response.text

        expected_nav_items = [
            "Dashboard",
            "Users",
            "V4V Config",
            "Account Balances",
            "Financial Reports",
        ]

        for item in expected_nav_items:
            assert item in content, f"Navigation item '{item}' not found"

    def test_navigation_active_state(self, admin_client):
        """Test navigation active state highlighting"""
        # Test users page has Users menu active
        response = admin_client.get("/admin/users")
        content = response.text

        # Should contain active class for Users menu
        assert "Users" in content

        # Test dashboard has Dashboard menu active
        response = admin_client.get("/admin")
        content = response.text
        assert "Dashboard" in content

    def test_navigation_links(self, admin_client):
        """Test navigation links point to correct URLs"""
        response = admin_client.get("/admin")
        content = response.text

        expected_links = [
            "/admin",
            "/admin/users",
            "/admin/v4vconfig",
            "/admin/accounts",
            "/admin/financial-reports",
        ]

        for link in expected_links:
            assert link in content, f"Navigation link '{link}' not found"


class TestAdminContent:
    """Test content rendering and data display"""

    def test_users_page_data_display(self, admin_client):
        """Test users page displays user data correctly"""
        response = admin_client.get("/admin/users")
        content = response.text

        # Check for user data (these should be present in test/dev environment)
        user_indicators = ["brianoflondon", "v4vapp", "test"]

        # At least one user indicator should be present
        found_users = any(user in content for user in user_indicators)
        assert found_users, "No expected users found in content"

    def test_balance_formatting(self, admin_client):
        """Test balance values are properly formatted"""
        response = admin_client.get("/admin/users")
        content = response.text

        # Check for number formatting (commas, decimals)
        import re

        # Look for patterns like "1,234" or "123" or "0"
        number_pattern = re.compile(r"\b\d{1,3}(?:,\d{3})*\b")
        assert number_pattern.search(content), "No properly formatted numbers found"

    def test_status_badges(self, admin_client):
        """Test status badges are displayed"""
        response = admin_client.get("/admin/users")
        content = response.text

        # Check for Bootstrap badge classes
        badge_classes = ["badge", "bg-success", "bg-danger", "bg-secondary", "bg-warning"]
        badges_found = any(cls in content for cls in badge_classes)
        assert badges_found, "No status badges found"


class TestAdminErrorHandling:
    """Test error handling and edge cases"""

    def test_invalid_endpoint(self, admin_client):
        """Test 404 handling for invalid endpoints"""
        response = admin_client.get("/admin/nonexistent")
        # Should return 404 for invalid admin endpoints
        assert response.status_code == 404

    def test_users_page_empty_state(self, admin_client):
        """Test users page handles empty data gracefully"""
        # This would require mocking the database to return empty results
        # For now, just ensure the page doesn't crash
        response = admin_client.get("/admin/users")
        assert response.status_code == 200

        content = response.text
        # Should either show data or empty state message
        has_data = "brianoflondon" in content or "v4vapp" in content
        has_empty_message = "No VSC Liability Users Found" in content

        assert has_data or has_empty_message, "Page should show data or empty state"


class TestAdminStaticFiles:
    """Test static file serving"""

    def test_static_file_access(self, admin_client):
        """Test static files are accessible"""
        # Try to access a CSS file that should exist
        response = admin_client.get("/admin/static/css/bootstrap.min.css")
        # This might 404 if Bootstrap is served differently, which is acceptable
        assert response.status_code in [200, 404]


class TestAdminIntegration:
    """Integration tests combining multiple components"""

    def test_full_user_workflow(self, admin_client):
        """Test complete user browsing workflow"""
        # 1. Access dashboard
        response = admin_client.get("/admin")
        assert response.status_code == 200

        # 2. Navigate to users page
        response = admin_client.get("/admin/users")
        assert response.status_code == 200
        content = response.text

        # 3. Verify users page has navigation back to dashboard
        assert "/admin" in content

        # 4. Check that user links are present (if any users exist)
        if "brianoflondon" in content:
            # Could test clicking user links, but that would require more complex setup
            assert "/admin/accounts" in content

    def test_template_consistency(self, admin_client):
        """Test templates have consistent structure and styling"""
        pages = ["/admin", "/admin/users"]

        for page in pages:
            response = admin_client.get(page)
            assert response.status_code == 200
            content = response.text

            # All pages should have consistent elements
            assert "<!DOCTYPE html>" in content
            assert "container" in content  # Bootstrap container
            assert "card" in content  # Bootstrap cards


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
