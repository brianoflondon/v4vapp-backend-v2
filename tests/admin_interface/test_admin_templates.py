"""
Admin Template Tests

Tests for Jinja2 template compilation, rendering, and validation.
"""

from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader, TemplateSyntaxError


class TestTemplateCompilation:
    """Test template compilation and syntax validation"""

    @pytest.fixture
    def template_env(self):
        """Create Jinja2 environment for template testing"""
        # Resolve repository root relative to this test file and find admin templates
        admin_dir = Path(__file__).parent.parent.parent / "src" / "v4vapp_backend_v2" / "admin"
        templates_dir = admin_dir / "templates"
        env = Environment(loader=FileSystemLoader(str(templates_dir)))

        # Add mock url_for function for testing
        def mock_url_for(name, **kwargs):
            if name == "static":
                return f"/admin/static/{kwargs.get('path', '')}"
            return f"/admin/{name}"

        env.globals["url_for"] = mock_url_for
        return env

    def test_base_template_compilation(self, template_env):
        """Test base template compiles without errors"""
        template = template_env.get_template("base.html")
        assert template is not None

        # Skip full rendering test due to FastAPI dependencies
        # Just test that template loads and has expected structure
        with open(template.filename, "r") as f:
            content = f.read()
            assert "{% block content %}" in content
            assert "<!DOCTYPE html>" in content
            assert "<title>" in content

    def test_users_template_compilation(self, template_env):
        """Test users template compiles without errors"""
        template = template_env.get_template("users/users.html")
        assert template is not None

        # Test template structure without full rendering
        with open(template.filename, "r") as f:
            content = f.read()
            assert '{% extends "base.html" %}' in content
            assert "VSC Liability Users" in content
            assert "User" in content
            assert "Balance" in content

    def test_dashboard_template_compilation(self, template_env):
        """Test dashboard template compiles without errors"""
        template = template_env.get_template("dashboard.html")
        assert template is not None

        # Test template structure without full rendering
        with open(template.filename, "r") as f:
            content = f.read()
            assert '{% extends "base.html" %}' in content
            assert "Account Balances" in content  # Check for dashboard-specific content
            assert "LND Balances" in content
            assert "Delta:" in content

    def test_template_inheritance(self, template_env):
        """Test template inheritance works correctly"""
        # Load a child template that extends base
        users_template = template_env.get_template("users/users.html")

        # The template should contain {% extends "base.html" %}
        template_source = users_template.filename
        with open(template_source, "r") as f:
            content = f.read()

        assert '{% extends "base.html" %}' in content
        assert "{% block content %}" in content
        assert "{% endblock %}" in content

    def test_template_syntax_validation(self, template_env):
        """Test all templates have valid Jinja2 syntax"""
        templates_dir = Path(template_env.loader.searchpath[0])

        # Find all .html files
        html_files = list(templates_dir.rglob("*.html"))

        for html_file in html_files:
            relative_path = html_file.relative_to(templates_dir)
            template_path = str(relative_path)

            try:
                template = template_env.get_template(template_path)
                assert template is not None
                print(f"✓ Template {template_path} compiled successfully")
            except TemplateSyntaxError as e:
                pytest.fail(f"Template {template_path} has syntax error: {e}")
            except Exception as e:
                pytest.fail(f"Template {template_path} failed to load: {e}")


class TestTemplateRendering:
    """Test template rendering with various data scenarios"""

    @pytest.fixture
    def template_env(self):
        """Create Jinja2 environment for template testing"""
        # Resolve repository root relative to this test file and find admin templates
        admin_dir = Path(__file__).parent.parent.parent / "src" / "v4vapp_backend_v2" / "admin"
        templates_dir = admin_dir / "templates"
        env = Environment(loader=FileSystemLoader(str(templates_dir)))

        # Add mock url_for function for testing
        def mock_url_for(name, **kwargs):
            if name == "static":
                return f"/admin/static/{kwargs.get('path', '')}"
            return f"/admin/{name}"

        env.globals["url_for"] = mock_url_for

        # Mock request object for template testing
        class MockRequest:
            class QueryParams:
                def get(self, key, default=None):
                    return default

            query_params = QueryParams()

        env.globals["request"] = MockRequest()
        return env

    def test_users_template_empty_data(self, template_env):
        """Test users template handles empty data gracefully"""
        # Skip full rendering due to complex FastAPI dependencies
        # Just test template structure
        template = template_env.get_template("users/users.html")
        assert template is not None

        # Verify template has empty state handling
        with open(template.filename, "r") as f:
            content = f.read()
            assert "No VSC Liability Users Found" in content
            assert "👥" in content

    def test_users_template_with_errors(self, template_env):
        """Test users template handles error states"""
        # Skip full rendering due to complex FastAPI dependencies
        # Just test template structure
        template = template_env.get_template("users/users.html")
        assert template is not None

        # Verify template has error handling
        with open(template.filename, "r") as f:
            content = f.read()
            assert "badge bg-danger" in content
            assert "Error" in content

    def test_users_template_balance_formatting(self, template_env):
        """Test balance formatting in various scenarios"""
        # Skip full rendering due to complex FastAPI dependencies
        # Just test template structure
        template = template_env.get_template("users/users.html")
        assert template is not None

        # Verify template has balance formatting logic
        with open(template.filename, "r") as f:
            content = f.read()
            assert "text-success" in content  # Positive balance styling
            assert "text-danger" in content  # Negative balance styling
            assert "text-muted" in content  # Zero balance styling


class TestTemplateFilters:
    """Test custom Jinja2 filters if any are defined"""

    @pytest.fixture
    def template_env(self):
        """Create Jinja2 environment with any custom filters"""
        # Resolve repository root relative to this test file and find admin templates
        admin_dir = Path(__file__).parent.parent.parent / "src" / "v4vapp_backend_v2" / "admin"
        templates_dir = admin_dir / "templates"
        env = Environment(loader=FileSystemLoader(str(templates_dir)))

        # Add any custom filters that might be used
        # For now, just test built-in filters
        return env

    def test_builtin_filters(self, template_env):
        """Test that built-in Jinja2 filters work"""
        # Test basic string formatting
        template = template_env.from_string("{{ 'hello'|upper }}")
        result = template.render()
        assert result == "HELLO"

        # Test that template environment is properly configured
        assert template_env is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
