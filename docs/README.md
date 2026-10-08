# @seneca/postgres-store documentation

A Seneca entity store for PostgreSQL. These pages follow the
Diátaxis layout: tutorials teach, how-to guides solve a task,
reference describes, explanation discusses.

## Tutorials

| Page | What you learn |
| ---- | -------------- |
| [Getting started](tutorials/getting-started.md) | Install, connect, save, load, list and remove an entity |

## How-to guides

| Page | Task |
| ---- | ---- |
| [Run the tests locally](how-to/run-the-tests-locally.md) | Start PostgreSQL with Docker and run the test suite |
| [Map column names](how-to/map-column-names.md) | Convert between entity field names and column names |
| [Run native SQL](how-to/run-native-sql.md) | Use `native$` queries and the `native` handle |
| [Migrate from Seneca 3](how-to/migrate-from-seneca-3.md) | Move an application to Seneca 4 |

## Reference

| Page | Contents |
| ---- | -------- |
| [Options](reference/options.md) | Plugin options and connection settings |
| [Messages](reference/messages.md) | Action patterns the plugin adds |
| [Queries](reference/queries.md) | Query operators and directives |

## Explanation

| Page | Topic |
| ---- | ----- |
| [How the store works](explanation/how-it-works.md) | Pool, lifecycle, ids, transactions, Seneca 3 versus 4 |

## Feature index

| Feature | Kind | Documented in |
| ------- | ---- | ------------- |
| `name` | option | [Options](reference/options.md) |
| `host` / `server` | option | [Options](reference/options.md) |
| `port` | option | [Options](reference/options.md) |
| `username` | option | [Options](reference/options.md) |
| `password` / `pass` | option | [Options](reference/options.md) |
| `fromColumnName` | option | [Options](reference/options.md), [Map column names](how-to/map-column-names.md) |
| `toColumnName` | option | [Options](reference/options.md), [Map column names](how-to/map-column-names.md) |
| `map` (seneca-entity store option) | option | [Options](reference/options.md) |
| `SENECA_TEST_PG_*` | test env variables | [Run the tests locally](how-to/run-the-tests-locally.md) |
| `role:entity,cmd:save` | action | [Messages](reference/messages.md) |
| `role:entity,cmd:load` | action | [Messages](reference/messages.md) |
| `role:entity,cmd:list` | action | [Messages](reference/messages.md) |
| `role:entity,cmd:remove` | action | [Messages](reference/messages.md) |
| `role:entity,cmd:close` | action | [Messages](reference/messages.md) |
| `role:entity,cmd:native` | action | [Messages](reference/messages.md), [Run native SQL](how-to/run-native-sql.md) |
| `init:postgresql-store` | action | [Messages](reference/messages.md) |
| `role:sql,hook:generate_id,target:postgresql-store` | action | [Messages](reference/messages.md) |
| `sys:entity,transaction:begin` | action | [Messages](reference/messages.md) |
| `sys:entity,transaction:end` | action | [Messages](reference/messages.md) |
| `sys:entity,transaction:rollback` | action | [Messages](reference/messages.md) |
| `eq$`, `ne$`, `gt$`, `gte$`, `lt$`, `lte$`, `in$`, `nin$` | query operator | [Queries](reference/queries.md) |
| `or$`, `and$` | query operator | [Queries](reference/queries.md) |
| `sort$`, `limit$`, `skip$`, `fields$`, `all$`, `load$` | query directive | [Queries](reference/queries.md) |
| `native$` | query directive | [Queries](reference/queries.md), [Run native SQL](how-to/run-native-sql.md) |
| `upsert$`, `auto_increment$` | save directive | [Queries](reference/queries.md) |
| `transaction$` | message directive | [Queries](reference/queries.md) |

The plugin defines no error codes of its own; database errors from
`pg` are passed to the action callback unchanged.
