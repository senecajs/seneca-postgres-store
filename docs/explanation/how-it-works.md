# How the store works

## Lifecycle

The plugin registers its store functions with seneca-entity and adds
an `init:postgresql-store` action. When Seneca initialises the plugin
it creates one `pg.Pool`. Every entity action checks out a client,
runs its SQL and releases the client. `seneca.close()` reaches the
store's `close` operation, which ends the pool so the process can exit.

The store does not create or migrate tables. Each entity name is a
table name (`base/name` becomes `base_name`; the zone is ignored), and each field
is a column, renamed by `toColumnName` / `fromColumnName` if given.

## Ids

When a saved entity has no id, the store asks Seneca for one with
`role:sql,hook:generate_id,target:postgresql-store`. The default
action replies with a UUID v4. Making it an action means an
application can override id generation without changing the store.
`auto_increment$` skips this and lets the database assign the id.

## Transactions

seneca-entity 21.x and 22.x had a transaction API
(`entity.begin()`, `end()`, `rollback()`, `state()`). With those
versions the store runs `BEGIN` on the first action of a transaction
and reuses the same client until `COMMIT` or `ROLLBACK`. Later
seneca-entity releases removed the API; the store detects that
`entity.state` is missing and runs every action on its own pooled
client.

## Seneca 3 versus Seneca 4

* Seneca 4 does not decorate `seneca.store`. The store uses the
  `entity/init` export of seneca-entity, falling back to
  `seneca.store.init` on older setups.
* Seneca 4 does not wrap action errors, so PostgreSQL errors reach
  the callback with their own message.
* Options come only from `use()` or `options.plugin`.

## Limits

* Only the five connection options in [Options](../reference/options.md)
  reach `pg.Pool`.
* `native$` SQL is not passed through the column name mapping.
