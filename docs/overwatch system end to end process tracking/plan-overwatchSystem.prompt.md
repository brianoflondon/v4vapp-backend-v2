## Plan: Implement Overwatch System for Transaction Flow Tracking

Map all transaction flows in the v4vapp-backend-v2 system and layer an overwatch architecture to track, visualize, and alert on end-to-end processes without altering core logic. This enables proactive monitoring of Hive-to-Lightning conversions, internal transfers, and error handling, reducing debugging time and improving reliability.

**Steps**
1. Define flow state machines: Create Pydantic models in a new `src/v4vapp_backend_v2/overwatch/` module for flow types (e.g., HiveToLightning, LightningToHive) with states like "initiated", "converted", "paid", "completed", and error states. Reference [process_tracked_events.py](process_tracked_events.py) for operation types.
2. Implement event collector: Add async hooks in [process_tracked_events.py](process_tracked_events.py#L126-144) to emit events (e.g., `await overwatch.emit("operation_started", data)`) for each processor call. Store events in new MongoDB collection `overwatch_events` with fields: group_id, event_type, timestamp, data.
3. Build state machine engine: In `overwatch/state_machine.py`, define FSM classes per flow type, transitioning states on events (e.g., from "converted" to "paid" on payment success). Persist current states in `flow_states` collection. Integrate with existing `LedgerEntry` saves for state updates.
4. Add alert engine: Create rules in `overwatch/alerts.py` for thresholds (e.g., stuck flows >5min, balance discrepancies). Trigger alerts via existing logger notifications. Use sanity checks from [accounting/sanity_checks.py](src/v4vapp_backend_v2/accounting/sanity_checks.py) as data source.
5. Develop dashboard UI: Extend the admin FastAPI app in [admin/](src/v4vapp_backend_v2/admin/) with endpoints for querying flows/states. Add a web interface using Jinja2 templates or integrate Plotly for flow diagrams (e.g., Sankey charts showing value movement from Hive to Lightning).
6. Integrate real-time updates: Add WebSocket support in admin app for live flow progress. Poll `flow_states` collection for updates, ensuring minimal DB load.
7. Update config and logging: Add overwatch section to [config/*.yaml](config/) for settings (e.g., alert thresholds). Enhance custom logger in [config/mylogger.py](src/v4vapp_backend_v2/config/mylogger.py) to include overwatch events in extras.

**Verification**
Run full-stack tests with `uv run pytest -k "test_full_stack"` to ensure no performance regression (<1% latency). Manually simulate flows (e.g., Hive transfer → payment) and verify dashboard shows complete timelines. Use admin endpoints to query stuck flows and confirm alerts trigger on errors. Validate with sanity checks post-transaction.

**Decisions**
- Chose layered architecture to avoid core changes, reusing MongoDB for storage to minimize new dependencies.
- Prioritized async hooks for low overhead, targeting event emission in key processors like [process_payment.py](process_payment.py#L50-60) and [process_invoice.py](process_invoice.py#L40-50).
- Focused on group_id linking for end-to-end tracking, as it's already used in ledger entries and tracked objects.
