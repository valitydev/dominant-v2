# dominant_v2

## Migrations

Database migrations live in the `migrations/` directory and are applied
automatically on service startup by
[epg_migrator](https://hex.pm/packages/epg_migrator). Pending migrations are
applied in lexicographical order, each run is guarded by a Postgres advisory
lock, and applied migrations are recorded in the `schema_migrations` table.

To add a migration, create a new file named
`<unix timestamp>-<short_description>.sql` containing plain SQL:

```shell
touch "migrations/$(date +%s)-add_some_table.sql"
```

Migrations are forward-only: to undo a change that has already been applied
somewhere, add a new compensating migration instead of editing or deleting an
existing one.

Databases migrated by the legacy psql-migration tool (the `__migrations`
table) are picked up automatically: the recorded history is carried over into
`schema_migrations` on the first startup after the switchover, so already
applied migrations are not rerun.
