# V4VApp Admin Interface

A modern, responsive FastAPI-based admin interface for managing V4VApp backend configurations and services.

## Features

- 🏠 **Dashboard Overview**: Quick access to all admin functions
- ⚙️ **V4V Configuration**: Manage fees, limits, and gateway settings
- 🚦 **Gateway Control**: Enable/disable Hive ↔ Lightning gateways
- ⏱️ **Rate Limiting**: Configure rate limits with dynamic management
- 📊 **API Integration**: REST API with automatic documentation
- 📱 **Responsive Design**: Works on desktop, tablet, and mobile
- 🔄 **Real-time Updates**: Refresh configuration from Hive blockchain

## Quick Start

### 1. Run the Admin Server

```bash
cd /Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2
python src/v4vapp_backend_v2/admin/run_admin.py
```

### 2. Access the Interface

- **Admin Dashboard**: http://127.0.0.1:8080/admin
- **API Documentation**: http://127.0.0.1:8080/admin/docs
- **Health Check**: http://127.0.0.1:8080/admin/health

### 3. Configure V4VApp

1. Navigate to "V4V Configuration" in the sidebar
2. Adjust settings as needed
3. Click "Update Configuration" to save to Hive blockchain

## Command Line Options

```bash
python run_admin.py --help

Options:
  --host TEXT         Host to bind to (default: 127.0.0.1)
  --port INTEGER      Port to bind to (default: 8080)
  --config TEXT       Configuration file to use (default: devhive.config.yaml)
  --reload            Enable auto-reload for development
  --log-level TEXT    Log level (default: info)
```

### Examples

```bash
# Run on all interfaces
python run_admin.py --host 0.0.0.0 --port 8080

# Use different config file
python run_admin.py --config production.yaml

# Development mode with auto-reload
python run_admin.py --reload --log-level debug
```

## MCP server

`v4vapp-admin-mcp` exposes the admin data to an MCP client. It uses the same config, MongoDB, and Redis connection as the web UI.

stdio, for a client on this machine:

```bash
uv run v4vapp-admin-mcp --config devdocker.config.yaml
```

HTTP, for a Docker container reached over Tailscale. Put a long random value in `.env` as `V4VAPP_ADMIN_MCP_TOKEN`. The process refuses to listen without it. The `admin-mcp` service in `docker-compose.yaml` publishes port 8090.

```bash
uv run v4vapp-admin-mcp --transport http --host 0.0.0.0 --port 8090
```

Point a client at `http://<tailscale-host>:8090/mcp` and send the token on every request:

```json
{
  "mcpServers": {
    "v4vapp-admin": {
      "url": "http://<tailscale-host>:8090/mcp",
      "headers": {
        "Authorization": "Bearer <V4VAPP_ADMIN_MCP_TOKEN>"
      }
    }
  }
}
```

`/health` answers without the token so the container probe can run. It returns only `{"status": "ok"}`.

Read tools cover the dashboard, ledger accounts, users, ledger search, financial reports, V4V configuration, quote sources, the two gateway switches, and Hive deposit inspection. `account_balance` takes an account label from `list_ledger_accounts`.

Three tools change live state, and each refuses to run unless `confirm` is true:

- `flush_ledger_cache` drops cached ledger balances.
- `set_quote_sources` saves quote-service switches to Hive.
- `set_gateway` opens or closes Hive → Lightning and Lightning → Hive. Pass `true` to open a direction and `false` to close it. Pass both arguments to set them together.

Ledger edits, reversals, deposit replay, Lightning retries, and fee or limit updates stay on the web UI.

## Configuration Management

### V4V Configuration Settings

The admin interface allows you to manage all V4VApp configuration settings:

#### Fee Settings

- **Hive Return Fee**: Fee for returning Hive transactions
- **Conversion Fee (%)**: Percentage fee for conversions
- **Conversion Fee (Sats)**: Fixed sat fee for conversions
- **Streaming Fee (%)**: Fee for streaming sats to Hive

#### Payment Limits

- **Minimum Invoice**: Smallest invoice amount in sats
- **Maximum Invoice**: Largest invoice amount in sats
- **Max LND Fee**: Maximum routing fee in millisats

#### Gateway Control

- **Hive → Lightning**: Enable/disable payments from Hive to Lightning
- **Lightning → Hive**: Enable/disable payments from Lightning to Hive

#### Service URLs

- **Frontend IRI**: Frontend application URL
- **API IRI**: API endpoint URL
- **Dynamic Fees**: Hive account and permlink for dynamic fees

#### Rate Limits

- Dynamic rate limiting configuration
- Set limits by time period (hours) and amount (sats)
- Add/remove limits as needed

## API Endpoints

### V4V Configuration

- `GET /admin/v4vconfig/api` - Get current configuration
- `POST /admin/v4vconfig/api` - Update configuration
- `POST /admin/v4vconfig/validate` - Validate configuration
- `GET /admin/v4vconfig/refresh` - Refresh from Hive

### Ledger Cache

- `POST /admin/ledger-cache/flush` - Increment cache generation (flush all ledger balance entries). Typically triggered by the sidebar button but available for scripting.

### Example API Usage

```bash
# Get current configuration
curl http://127.0.0.1:8080/admin/v4vconfig/api

# Update configuration via API
curl -X POST http://127.0.0.1:8080/admin/v4vconfig/api \
  -H "Content-Type: application/json" \
  -d '{
    "hive_return_fee": 0.01,
    "conv_fee_percent": 0.5,
    "conv_fee_sats": 100,
    ...
  }'
```

## Architecture

### Directory Structure

```
src/v4vapp_backend_v2/admin/
├── __init__.py                 # Package initialization
├── app.py                      # Main FastAPI application
├── navigation.py               # Navigation management
├── run_admin.py               # Standalone server runner
├── routers/                   # FastAPI routers
│   ├── __init__.py
│   └── v4vconfig.py          # V4V configuration router
├── templates/                 # Jinja2 templates
│   ├── base.html             # Base template
│   ├── dashboard.html        # Main dashboard
│   ├── error.html            # Error page
│   └── v4vconfig/
│       └── dashboard.html    # V4V config page
└── static/                   # Static assets
    └── admin.css            # Custom CSS
```

### Adding New Admin Sections

1. **Create a new router** in `routers/`:

```python
# routers/new_section.py
from fastapi import APIRouter
router = APIRouter()

@router.get("/")
async def new_section_dashboard():
    return {"message": "New section"}
```

2. **Add navigation item** in `navigation.py`:

```python
NavigationItem(
    name="New Section",
    url="/admin/new-section",
    icon="🔧",
    description="Manage new functionality"
)
```

3. **Include router** in `app.py`:

```python
from v4vapp_backend_v2.admin.routers import new_section
self.app.include_router(
    new_section.router,
    prefix="/admin/new-section",
    tags=["New Section"]
)
```

4. **Create templates** in `templates/new_section/`

## Security Considerations

⚠️ **Important**: This admin interface currently has no built-in authentication. It's designed for:

- **Local development environments**
- **Private networks with proper firewall rules**
- **VPN-protected environments**

For production use, consider adding:

- Basic HTTP authentication
- OAuth integration
- IP whitelisting
- SSL/TLS encryption

## Development

### Prerequisites

- Python 3.12+
- FastAPI
- Uvicorn
- Jinja2

### Installation

```bash
pip install fastapi uvicorn jinja2 python-multipart
```

### Running in Development Mode

```bash
python run_admin.py --reload --log-level debug
```

This enables:

- Auto-reload on file changes
- Detailed logging
- Better error messages

## Troubleshooting

### Common Issues

1. **Config file not found**
   - Ensure the config file exists in the project root
   - Use `--config` to specify a different file

2. **Port already in use**
   - Use `--port` to specify a different port
   - Check what's running on the port: `lsof -i :8080`

3. **Template not found**
   - Ensure templates directory exists
   - Check file paths in router functions

4. **Hive connection issues**
   - Verify Hive node configuration
   - Check network connectivity
   - Review server account settings

#### Jinja / HTML formatter issues

If you edit Jinja templates under `templates/` with an editor that formats HTML on save (e.g. Prettier or generic HTML formatters), it can collapse multi-line Jinja logic into a single line and corrupt Jinja syntax (for example turning `==` into a broken `=""="` sequence). To avoid this:

- Disable automatic HTML formatting for template files, or map template files to a Jinja language ID and disable formatting for that language.
- The repository includes a workspace `.vscode/settings.json` that maps `src/v4vapp_backend_v2/admin/templates/**/*.html` to the `jinja` language and disables `formatOnSave` for the `jinja` language.
- For extra safety, we have a unit test `tests/test_templates.py` which compiles the key templates and fails if they do not render; this runs in CI.

Developer notes:

- Install local pre-commit hooks to validate templates on commit:

```bash
pip install pre-commit  # if you don't have it already
pre-commit install
```

- If you use Prettier or other HTML formatters in VS Code, disable format-on-save for the Jinja language or add `src/v4vapp_backend_v2/admin/templates/` to your `.prettierignore`.

### Logs

The admin interface uses the same logging configuration as the main V4VApp backend. Check logs for detailed error information.

## License

This admin interface is part of the V4VApp project. See the main project LICENSE file for details.
