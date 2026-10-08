# Messages

The store is registered with seneca-entity under the name
`postgresql-store`. seneca-entity routes entity operations to it.

## Entity operations

| Pattern | Called by | Reply |
| ------- | --------- | ----- |
| `role:entity,cmd:save` | `ent.save$()` | The saved entity. Without an id, an id is generated (see below) unless `auto_increment$` is set. With `upsert$`, an existing row matching those fields is updated. |
| `role:entity,cmd:load` | `ent.load$(q)` | The first matching entity, or `null`. |
| `role:entity,cmd:list` | `ent.list$(q)` | An array of entities. Supports `native$`. |
| `role:entity,cmd:remove` | `ent.remove$(q)` | Nothing, or the removed entity when `load$: true`. `all$: true` removes every match. |
| `role:entity,cmd:close` | `seneca.close()` | Ends the `pg` pool. |
| `role:entity,cmd:native` | `ent.native$()` | A client checked out from the pool. The caller must `release()` it. |

On Seneca 4 seneca-entity uses `sys:entity` in place of `role:entity`;
the behaviour is the same. Errors from PostgreSQL are replied
unchanged.

## Other actions

| Pattern | Parameters | Reply |
| ------- | ---------- | ----- |
| `init:postgresql-store` | none | Creates the connection pool from the options. |
| `role:sql,hook:generate_id,target:postgresql-store` | none | `{ id }` with a random UUID v4. Override it to generate your own ids. |
| `sys:entity,transaction:begin` | none | `{ get_handle }`; the SQL `BEGIN` runs on first use. |
| `sys:entity,transaction:end` | transaction details | Runs `COMMIT`; replies `{ done: true }`. |
| `sys:entity,transaction:rollback` | transaction details | Runs `ROLLBACK`; replies `{ done: false, rollback: true }`. |

The transaction actions are only called by seneca-entity 21.x and
22.x.

Example of a custom id generator:

```js
seneca.add('role:sql,hook:generate_id,target:postgresql-store', function (msg, reply) {
  reply({ id: 'foo-' + Date.now() })
})
```
