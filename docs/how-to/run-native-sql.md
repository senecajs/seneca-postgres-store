# Run native SQL

Goal: run SQL that the query language cannot express.

## With `list$`

1. Pass `native$` as a string, or as an array of SQL followed by
   values:

```js
const all = await seneca.entity('foo').list$({ native$: 'SELECT * FROM foo' })
const some = await seneca.entity('foo').list$({
  native$: ['SELECT * FROM foo WHERE x > ? AND p1 = ?', 1, 'a'],
})
```

2. Each `?` becomes `$1`, `$2`, ... and the values are sent as bound
   parameters. Each row becomes an entity of the type you listed.

## With the pool client

`role:entity,cmd:native` (`entity.native$()`) replies with a client
checked out from the `pg` pool. Release it when done:

```js
const ent = seneca.entity('foo')
ent.native$(async function (err, client) {
  if (err) throw err
  try {
    const res = await client.query('SELECT count(*) FROM foo')
    console.log(res.rows)
  } finally {
    client.release()
  }
})
```
