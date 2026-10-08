# Map column names

Goal: store entity field `fooBar` in column `foo_bar` (or any other
mapping).

By default field names and column names are the same.

1. Write two functions: `toColumnName(field)` returns the column for a
   field, `fromColumnName(column)` returns the field for a column.
2. Pass them as plugin options:

```js
function toColumnName(field) {
  return field.replace(/[A-Z]/g, (c, i) => (i ? '_' : '') + c.toLowerCase())
}

function fromColumnName(column) {
  return column.replace(/_([a-z])/g, (_, c) => c.toUpperCase())
}

seneca.use('@seneca/postgres-store', {
  name: 'mydb', host: '127.0.0.1', port: 5432,
  username: 'me', password: 'secret',
  toColumnName,
  fromColumnName,
})
```

The functions are applied to saved data, query fields and `native$`
result rows. They are not applied to the SQL text of a `native$` query,
so write real column names there.
