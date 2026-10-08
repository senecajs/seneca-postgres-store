# Queries

A query is an object passed to `load$`, `list$` or `remove$`. Plain
fields match by equality; an array value matches any element.

## Operators

| Operator | Example | Matches |
| -------- | ------- | --- |
| `eq$` | `{ price: { eq$: 200 } }` | price equal to 200 |
| `ne$` | `{ price: { ne$: 200 } }` | price not equal to 200 |
| `gt$`, `gte$` | `{ price: { gte$: 200 } }` | greater than (or equal) |
| `lt$`, `lte$` | `{ price: { lt$: 200 } }` | less than (or equal) |
| `in$` | `{ label: { in$: ['a', 'b'] } }` | any listed value |
| `nin$` | `{ label: { nin$: ['a'] } }` | none of the listed values |
| `or$` | `{ or$: [{ label: 'a' }, { price: 200 }] }` | any sub-query |
| `and$` | `{ and$: [{ label: 'a' }, { price: 200 }] }` | all sub-queries |

## Directives

| Directive | Applies to | Effect |
| --------- | ---------- | ------ |
| `sort$` | list | `{ field: 1 }` ascending, `-1` descending. |
| `limit$` | list | Maximum rows. |
| `skip$` | list | Rows to skip. |
| `fields$` | list | Array of fields to select. |
| `all$` | remove | Remove every match, not only the first. |
| `load$` | remove | Reply with the removed entity. |
| `native$` | list | SQL string, or `[sql, ...values]` with `?` placeholders. |
| `upsert$` | save | Array of field names; update the row matching them or insert. |
| `auto_increment$` | save | Let the database generate the id (for `SERIAL` columns). |
| `transaction$` | any | `false` runs the action outside the current transaction. |

`upsert$` and `auto_increment$` are passed to `save$`:

```js
await seneca.entity('players').data$({ username: 'ann', points: 3 })
  .save$({ upsert$: ['username'] })
```
