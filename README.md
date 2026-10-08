# @seneca/postgres-store

A [Seneca](http://senecajs.org) entity store plugin for PostgreSQL,
using the [pg](https://node-postgres.com/) driver. It works with
Seneca 4 (tested with 4.0.0-rc5 and 4.0.0) and seneca-entity 28, on
Node 24 and 22.

[![npm version](https://img.shields.io/npm/v/@seneca/postgres-store.svg)](https://www.npmjs.com/package/@seneca/postgres-store)
[![build](https://github.com/senecajs/seneca-postgres-store/actions/workflows/build.yml/badge.svg)](https://github.com/senecajs/seneca-postgres-store/actions/workflows/build.yml)

| ![Voxgig](https://www.voxgig.com/res/img/vgt01r.png) | This open source module is sponsored and supported by [Voxgig](https://www.voxgig.com). |
|---|---|

## Install

```sh
npm install seneca seneca-entity @seneca/postgres-store
```

The store does not create tables; create them before use.

## Quick Example

```js
const Seneca = require('seneca')

const seneca = Seneca()
  .use('entity', { mem_store: false })
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

The tests need PostgreSQL. With Docker and Node 24 or 22:

```sh
npm install
npm run services:up     # postgres:18 on host port 55432
npm test
npm run services:down
```

See [Run the tests locally](docs/how-to/run-the-tests-locally.md) for
the `SENECA_TEST_PG_*` variables and testing with the unreleased
Seneca 4.0.0. CI workflow changes are kept in `.patches/` (apply with
`git am .patches/*.patch`).

## Background

Originally written by Marian Radulescu, maintained by the Seneca
community. See [CHANGES.md](CHANGES.md).

| Version | Seneca | seneca-entity | Node |
| ------- | ------ | ------------- | ---- |
| 2.5.x | 4 (prerelease and 4.0.0) | 28 | 22, 24 |
| 2.4.x | 3 | 20 to 22 | older releases |

License: MIT. See [LICENSE](LICENSE).
