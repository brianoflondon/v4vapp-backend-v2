"""
Navigation Manager

Manages the admin interface navigation menu and breadcrumbs.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class NavigationItem:
    """Represents a navigation menu item"""

    name: str
    url: str
    icon: str
    description: str
    active: bool = False
    badge: Optional[str] = None
    badge_color: str = "primary"


class NavigationManager:
    """Manages admin interface navigation"""

    def __init__(self):
        self._navigation_items = [
            NavigationItem(
                name="Dashboard", url="/admin", icon="🏠", description="Admin dashboard overview"
            ),
            NavigationItem(
                name="V4V Configuration",
                url="/admin/v4vconfig",
                icon="⚙️",
                description="Manage V4VApp configuration settings",
                badge="Config",
                badge_color="info",
            ),
            NavigationItem(
                name="Account Balances",
                url="/admin/accounts",
                icon="🏦",
                description="View ledger account balances and transaction history",
                badge="Ledger",
                badge_color="success",
            ),
            NavigationItem(
                name="Financial Reports",
                url="/admin/financial-reports",
                icon="📊",
                description="Balance sheet, profit & loss, and comprehensive financial reports",
                badge="Reports",
                badge_color="info",
            ),
            # Add more items here as you expand the admin interface
            # NavigationItem(
            #     name="User Management",
            #     url="/admin/users",
            #     icon="👥",
            #     description="Manage users and permissions"
            # ),
            # NavigationItem(
            #     name="System Logs",
            #     url="/admin/logs",
            #     icon="📋",
            #     description="View system logs and monitoring"
            # ),
            # NavigationItem(
            #     name="Database",
            #     url="/admin/database",
            #     icon="🗄️",
            #     description="Database management and monitoring"
            # ),
        ]

    def get_navigation_items(self, current_path: str = "") -> List[NavigationItem]:
        """Get navigation items with active state"""
        items = []
        for item in self._navigation_items:
            # Create a copy to avoid modifying the original
            nav_item = NavigationItem(
                name=item.name,
                url=item.url,
                icon=item.icon,
                description=item.description,
                active=current_path.startswith(item.url),
                badge=item.badge,
                badge_color=item.badge_color,
            )
            items.append(nav_item)
        return items

    def add_navigation_item(self, item: NavigationItem) -> None:
        """Add a new navigation item"""
        self._navigation_items.append(item)

    def remove_navigation_item(self, name: str) -> bool:
        """Remove a navigation item by name"""
        for i, item in enumerate(self._navigation_items):
            if item.name == name:
                self._navigation_items.pop(i)
                return True
        return False

    def get_breadcrumbs(self, current_path: str) -> List[Dict[str, str]]:
        """Generate breadcrumbs based on current path"""
        breadcrumbs = [{"name": "Admin", "url": "/admin"}]

        # Find matching navigation item
        for item in self._navigation_items:
            if current_path.startswith(item.url) and item.url != "/admin":
                breadcrumbs.append({"name": item.name, "url": item.url})
                break

        return breadcrumbs
