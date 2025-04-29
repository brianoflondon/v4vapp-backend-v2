
## 2025-04-29

## v0.2.11
### V4VAPP Backend V2

- Continued extensive re-write and testing of Hive scanner with Nectar (formerly Beem) in Async Python
- Matched with a Lightning node watcher which captures every event on the Lightning side
- Working with VSC guys on capture of their custom-json format
- MongoDB change streams working for back end sync. Previously I had an internal FastAPI based back end (which is still working and has proved to be pretty solid) but is an architectural horror show and a maintenance nightmare.
- New internal architecture means the ingestion side Hive and Lightning watching parts run independently feeding events into a MongoDB database.
- This is completely isolated from the action sides which can make Lightning or Hive payments (the same as my current architecture) but allows much more compartmentalization and separation of functions.

### Nectar (Advancing Beem)

- I've pushed a number of changes over to @thecrazygm fixing a few issues that crop up mostly and especially with error reporting
- My underlying Hive block watcher is an async stream and I'm working on making that code generalized and putting it back into Nectar. Beem was never async, this is a big deal.
