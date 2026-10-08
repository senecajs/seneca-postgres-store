// Save, load, list and remove an entity in PostgreSQL.
// Needs the database from docker-compose.yml: npm run services:up
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
