# dominant_v2

## Migration

First compile migration script with

```shell
make wc-make_psql_migration
```

Then you can use script with

```shell
bin/psql_migration -e .env
```

```shell
Usage: psql_migration [-h] [-d [<dir>]] [-e [<env>]] <command>

  -h, --help  Print this help text
  -d, --dir   Migration folder [default: migrations]
  -e, --env   Environment file to search for DATABASE_URL [default: .env]
  new <name>  Create a new migration
  list        List migrations indicating which have been applied
  run         Run all migrations
  revert      Revert the last migration
  reset       Resets your database by dropping the database in your
              DATABASE_URL and then runs `setup`
  setup       Creates the database specified in your DATABASE_URL, and
              runs any existing migrations.
```
