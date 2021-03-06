const Assert = require('assert')
const Pg = require('pg')

const Q = require('./lib/qbuilder')
const { intern } = require('./lib/intern')
const { asyncMethod } = intern

const STORE_NAME = 'postgresql-store'
const ACTION_ROLE = 'sql'


function postgres_store(options) {
  const seneca = this

  const {
    fromColumnName = intern.identity,
    toColumnName = intern.identity
  } = options


  let dbPool

  function configure(spec, done) {
    const conf = intern.getConfig(spec)

    dbPool = new Pg.Pool({
      user: conf.user,
      host: conf.host,
      database: conf.database,
      password: conf.password,
      port: conf.port
    })

    return done()
  }

  const store = {
    name: STORE_NAME,

    close: function (_msg, done) {
      dbPool.end().then(done).catch(done)
    },

    save: asyncMethod(async function (msg) {
      const seneca = this

      return intern.withDbClient(dbPool, async (client) => {
        const ctx = { seneca, client, fromColumnName, toColumnName }

        const { ent, q } = msg
        const { auto_increment$: autoIncrement = false } = q

        if (intern.isUpdate(msg)) {
          return intern.updateEnt(ent, ctx)
        }


        const newEnt = ent.clone$()

        if (!autoIncrement) {
          const generatedId = await intern
            .askSenecaToGenerateId({ role: ACTION_ROLE, target: STORE_NAME }, ctx)

          const newId = null == ent.id$
            ? generatedId
            : ent.id$

          newEnt.id = newId
        }


        const upsertFields = intern.maybeUpsert(msg)

        if (null != upsertFields) {
          return intern.upsertEnt(upsertFields, newEnt, q, ctx)
        }

        return intern.insertEnt(newEnt, ctx)
      })
    }),

    load: asyncMethod(async function (msg) {
      const seneca = this

      return intern.withDbClient(dbPool, async (client) => {
        const ctx = { seneca, client }
        const { qent, q } = msg

        return intern.findEnt(qent, q, ctx)
      })
    }),

    list: asyncMethod(async function (msg) {
      const seneca = this

      return intern.withDbClient(dbPool, async (client) => {
        const ctx = { seneca, client, fromColumnName, toColumnName }
        const { qent, q } = msg

        const nativeQuery = isNativeQuery(q)

        if (null == nativeQuery) {
          return intern.listEnts(qent, q, ctx)
        }

        const { rows } = await intern.execQuery(nativeQuery, ctx)

        return rows
          .map((row) => intern.deepXformKeys(fromColumnName, row))
          .map((row) => intern.makeent(qent, row))
      })

      function isNativeQuery(q) {
        if ('string' === typeof q.native$) {
          return toPgSql(q.native$)
        }

        if (Array.isArray(q.native$)) {
          Assert(0 < q.native$.length, 'q.native$.length')
          const [sql, ...bindings] = q.native$

          return { text: toPgSql(sql), values: bindings }
        }

        return null
      }

      function toPgSql(sql) {
        let param_no = 1
        return sql.replace(/\?/g, _ => Q.valuePlaceholder(param_no++))
      }
    }),

    remove: asyncMethod(async function (msg) {
      const seneca = this

      return intern.withDbClient(dbPool, async (client) => {
        const ctx = { seneca, client }
        const { qent, q } = msg

        return intern.removeEnt(qent, q, ctx)
      })
    }),

    native: function (_msg, done) {
      dbPool.connect().then(done).catch(done)
    }
  }


  const meta = seneca.store.init(seneca, options, store)


  seneca.add({ init: store.name, tag: meta.tag }, function (_msg, done) {
    return configure(options, done)
  })


  seneca.add(intern.msgForGenerateId({ role: ACTION_ROLE, target: STORE_NAME }),
    function (_msg, done) {
      const id = intern.generateId()
      return done(null, { id })
    })


  return { name: store.name, tag: meta.tag }
}


module.exports = postgres_store
