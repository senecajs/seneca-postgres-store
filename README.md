# @seneca/postgres-store

A [Seneca](http://senecajs.org) entity store plugin for PostgreSQL,
using the [pg](https://node-postgres.com/) driver. It needs Seneca 4
(`seneca` `^4.0.0`), the entity plugin `@seneca/entity` 29 and
Node.js 24 or later. Seneca 3 users stay on `seneca-postgres-store@2`.

[![npm version](https://img.shields.io/npm/v/@seneca/postgres-store.svg)](https://www.npmjs.com/package/@seneca/postgres-store)
[![build](https://github.com/senecajs/seneca-postgres-store/actions/workflows/build.yml/badge.svg)](https://github.com/senecajs/seneca-postgres-store/actions/workflows/build.yml)

| ![Voxgig](https://www.voxgig.com/res/img/vgt01r.png) | This open source module is sponsored and supported by [Voxgig](https://www.voxgig.com). |
|---|---|

## Install

```sh
npm install seneca@^4 @seneca/entity @seneca/postgres-store
```

The store does not create tables; create them before use.

## Quick Example

```js
const Seneca = require('seneca')

const seneca = Seneca()
  .use('@seneca/entity', { mem_store: false })
  .use('@seneca/postgres-store', {
    name: 'mydb',
    host: '127.0.0.1',
    port: 5432,
    username: 'me',
    password: 'secret',
  })

seneca.ready(async () => {
  const foo = await seneca.entity('foo').data$({ p1: 'a' }).save$()
  console.log(await seneca.entity('foo').load$(foo.id))
  seneca.close()
})
```

## More Examples

* [Getting started](docs/tutorials/getting-started.md), with a runnable
  [example](docs/examples/getting-started.js).
* [Map column names](docs/how-to/map-column-names.md)
* [Run native SQL](docs/how-to/run-native-sql.md)
* [Migrate from Seneca 3](docs/how-to/migrate-from-seneca-3.md)

## Motivation

Seneca entities give an application one data API for any store. This
plugin keeps that API and puts the data in PostgreSQL, with SQL
available when needed. See [How the store works](docs/explanation/how-it-works.md).

## Support

* Open an issue on [GitHub](https://github.com/senecajs/seneca-postgres-store/issues).
* Seneca documentation: [senecajs.org](http://senecajs.org).
* Sponsored by [Voxgig](https://www.voxgig.com).

## API

| Topic | Reference |
| ----- | --------- |
| Options and connection settings | [Options](docs/reference/options.md) |
| Action patterns | [Messages](docs/reference/messages.md) |
| Query operators and directives | [Queries](docs/reference/queries.md) |

The full list of features is the [documentation index](docs/README.md).

## Contributing

The tests need PostgreSQL. With Docker and Node.js 24 or later:

```sh
npm install
npm run services:up     # postgres:18 on host port 55432
npm test
npm run services:down
```

See [Run the tests locally](docs/how-to/run-the-tests-locally.md) for
the `SENECA_TEST_PG_*` variables and testing with an unreleased
Seneca build. CI workflow changes are kept in `.patches/` (apply with
`git am .patches/*.patch`).

## Background

Originally written by Marian Radulescu, maintained by the Seneca
community. See [CHANGES.md](CHANGES.md).

| Version | Package | Seneca | Entity plugin | Node |
| ------- | ------- | ------ | ------------- | ---- |
| 3.x | `@seneca/postgres-store` | 4 | `@seneca/entity` 29 | 24 or later |
| 2.4.x | `seneca-postgres-store` | 3 | `seneca-entity` 20 to 22 | older releases |

License: MIT. See [LICENSE](LICENSE).
