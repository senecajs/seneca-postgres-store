# Options

Pass options with `seneca.use('@seneca/postgres-store', options)`.
Connection options are passed to `pg.Pool`.

| Option | Type | Default | Effect |
| ------ | ---- | ------- | ------ |
| `name` | string | none (pg default) | Database name (`database` in pg). |
| `host` | string | none (pg default, `localhost`) | Server host. `server` is accepted when `host` is not set. |
| `port` | number | none (pg default, 5432) | Server port. |
| `username` | string | none (pg default) | User name (`user` in pg). |
| `password` | string | none | Password. `pass` is accepted when `password` is not set. |
| `toColumnName` | function | identity | Maps an entity field name to a column name. |
| `fromColumnName` | function | identity | Maps a column name to an entity field name. |
| `map` | object | none | seneca-entity store option: which entity canons this store handles, for example `{ '-/-/foo': '*' }`. |

Notes, verified in `lib/intern.js` (`getConfig`):

* Use `username` and `name`. The pg names `user` and `database` are
  ignored when passed as options.
* Other `pg.Pool` settings (pool size, SSL, timeouts) are not passed
  through.

When no option is given, `pg` also reads the standard `PGHOST`,
`PGPORT`, `PGUSER`, `PGPASSWORD` and `PGDATABASE` environment
variables.
