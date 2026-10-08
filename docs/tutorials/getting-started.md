# Getting started

You will connect a Seneca 4 application to PostgreSQL and save, load,
list and remove an entity.

## 1. Install

```sh
npm install seneca seneca-entity @seneca/postgres-store
```

Until Seneca 4.0.0 is published, install the prerelease with
`npm install seneca@4.0.0-rc5`.

## 2. Start a database

The store does not create tables. This tutorial uses the database and
schema from this repository's `docker-compose.yml`, which has a table
`foo` with columns `id`, `p1`, `x` and others:

```sh
npm run services:up
```

## 3. Write the program

This is [`docs/examples/getting-started.js`](../examples/getting-started.js):

```js
const Seneca = require('seneca')

const env = process.env

const seneca = Seneca({ legacy: false })
  .test()
  .use('entity', { mem_store: false })
  .use(require('../../postgresql-store.js'), {
    name: env.SENECA_TEST_PG_DATABASE || 'senecatest_71v94h',
    host: env.SENECA_TEST_PG_HOST || '127.0.0.1',
    port: parseInt(env.SENECA_TEST_PG_PORT || '55432', 10),
    username: env.SENECA_TEST_PG_USER || 'senecatest',
    password: env.SENECA_TEST_PG_PASSWORD || 'senecatest_2086hab80y',
  })

seneca.ready(async function () {
  const foo = await seneca.entity('foo').data$({ p1: 'a', x: 1 }).save$()
  console.log('saved:', foo.id, foo.p1)

  const loaded = await seneca.entity('foo').load$(foo.id)
  console.log('loaded:', loaded.p1, loaded.x)

  const list = await seneca.entity('foo').list$({ p1: 'a' })
  console.log('listed:', list.length)

  const rows = await seneca.entity('foo').list$({ native$: ['SELECT * FROM foo WHERE x = ?', 1] })
  console.log('native:', rows.length)

  await seneca.entity('foo').remove$({ all$: true })
  console.log('removed all')

  seneca.close(() => console.log('closed'))
})
```

In your own project use `.use('@seneca/postgres-store', {...})`.

## 4. Run it

```sh
node docs/examples/getting-started.js
```

Output with `seneca@4.0.0-rc5` (the id is a random UUID):

```
saved: 5b106ee0-8195-41b5-bd30-851b56b45f13 a
loaded: a 1
listed: 1
native: 1
removed all
closed
```

## What happened

* `seneca-entity` provides the entity API (`entity`, `save$`, ...).
  `mem_store: false` stops it loading the in-memory store.
* The plugin opened a `pg` connection pool when Seneca initialised it.
* `save$` without an id generated a UUID and ran an `INSERT`.
* `list$` with `native$` ran your SQL; `?` became `$1`.
* `seneca.close()` ended the pool, so the process exited.

## Next steps

* [Options](../reference/options.md) for every connection setting.
* [Queries](../reference/queries.md) for operators such as `in$` and `sort$`.
* [How the store works](../explanation/how-it-works.md).
