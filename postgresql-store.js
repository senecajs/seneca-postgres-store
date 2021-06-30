'use strict'

const _ = require('lodash')
const Assert = require('assert')
const Pg = require('pg')
const Uuid = require('uuid')
const RelationalStore = require('./lib/relational-util')

const Util = require('util')
const { intern } = require('./lib/intern')
const { asyncMethod } = intern

const STORE_NAME = 'postgresql-store'
const ACTION_ROLE = 'sql'

const MIN_WAIT = 16
const MAX_WAIT = 5000

module.exports = function (opts) {
  const seneca = this

  opts.minwait = opts.minwait || MIN_WAIT
  opts.maxwait = opts.maxwait || MAX_WAIT

  const ColumnNameParsing = {
    fromColumnName: opts.fromColumnName || _.identity,
    toColumnName: opts.toColumnName || _.identity
  }
  const QueryBuilder = require('./lib/query-builder')(ColumnNameParsing)
  const Q = require('./lib/qbuilder')


  let minwait
  const internals = {}

  function error (query, args, err/*, next*/) {
    if (err) {
      const errorDetails = {
        message: err.message,
        err: err,
        stack: err.stack,
        query: query
      }
      seneca.log.error('Query Failed', JSON.stringify(errorDetails, null, 1))
      // next ({code: 'entity/error', store: STORE_NAME})

      if ('ECONNREFUSED' === err.code || 'notConnected' === err.message || 'Error: no open connections' === err) {
        minwait = opts.minwait
        if (minwait) {
          reconnect(args)
        }
      }

      return true
    }

    return false
  }

  function reconnect (args) {
    seneca.log.debug('attempting db reconnect')

    configure(opts, function (err) {
      if (err) {
        seneca.log.debug('db reconnect (wait ' + opts.minwait + 'ms) failed: ' + err)
        minwait = Math.min(2 * minwait, opts.maxwait)
        setTimeout(function () {
          reconnect(args)
        }, minwait)
      }
      else {
        minwait = opts.minwait
        seneca.log.debug('reconnect ok')
      }
    })
  }

  let pgConf
  let dbPool

  function configure (spec, done) {
    pgConf = 'string' === typeof (spec) ? null : spec

    if (!pgConf) {
      pgConf = {}

      const urlM = /^postgres:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec)
      pgConf.name = urlM[7]
      pgConf.port = urlM[6]
      pgConf.host = urlM[4]
      pgConf.username = urlM[2]
      pgConf.password = urlM[3]

      pgConf.port = pgConf.port ? parseInt(pgConf.port, 10) : null
    }

    // pg conf properties
    pgConf.user = pgConf.username
    pgConf.database = pgConf.name

    pgConf.host = pgConf.host || pgConf.server
    pgConf.username = pgConf.username || pgConf.user
    pgConf.password = pgConf.password || pgConf.pass

    // TODO: CLEAN UP. spec MUST MAKE IT THROUGH TO THE PG DRIVER !!!
    // The reason it is like so now, is because the driver breaks if
    // you try to pass any Object value.
    //
    dbPool = new Pg.Pool({
      name: pgConf.name,
      host: pgConf.host,
      port: pgConf.port,
      username: pgConf.username,
      password: pgConf.password,
      minwait: pgConf.minwait,
      maxwait: pgConf.maxwait,
      user: pgConf.user,
      database: pgConf.database
    })

    return done()
  }

  async function withDbClient(dbPool, f) {
    const client = await dbPool.connect()

    let result

    try {
      result = await f(client)
    } finally {
      client.release()
    }

    return result
  }

  async function execQuery_2(query, ctx) {
    if (!query) {
      throw new Error('An empty query is not a valid query')
    }

    const { client } = ctx

    return client.query(query)
  }

  async function execQuery(query) {
    if (!query) {
      throw new Error('An empty query is not a valid query')
    }

    const client = await dbPool.connect()
    let result

    try {
      result = await client.query(query)
    } finally {
      client.release()
    }

    return result
  }


  const store = {
    name: STORE_NAME,

    close: function (args, done) {
      dbPool.end().then(done).catch(done)
    },

    save: asyncMethod(async function (msg) {
      const seneca = this

      return withDbClient(dbPool, async (client) => {
        const ctx = { seneca, client }

        const { ent, q } = msg
        const { auto_increment$: autoIncrement = false } = q

        if (isUpdate(ent)) {
          return await updateEnt(ent, ctx)
        }


        const newEnt = ent.clone$()

        if (!autoIncrement) {
          const generatedId = await generateId(seneca)

          const newId = null == ent.id$
            ? generatedId
            : ent.id$

          newEnt.id = newId
        }


        const upsertFields = maybeUpsert(ent, q)

        if (null != upsertFields) {
          return upsertEnt(upsertFields, newEnt, q)
        }

        return insertEnt(newEnt)


        function maybeUpsert(ent, q) {
          if (isUpdate(ent)) {
            return null
          }

          if (!Array.isArray(q.upsert$)) {
            return null
          }

          const upsertFields = q.upsert$.filter((p) => !p.includes('$'))

          if (0 === upsertFields.length) {
            return null
          }

          return upsertFields
        }

        function isUpdate(ent) {
          return null != ent.id
        }
      })
    }),

    load: asyncMethod(async function (msg) {
      const seneca = this

      return withDbClient(dbPool, async (client) => {
        const ctx = { seneca, client }
        const { qent, q } = msg

        return findEnt(qent, q, ctx)
      })
    }),

    list: asyncMethod(async function (msg) {
      const seneca = this

      return withDbClient(dbPool, async (client) => {
        const ctx = { seneca, client }
        const { qent, q } = msg

        const nativeQuery = isNativeQuery(q)

        if (null == nativeQuery) {
          return listEnts(qent, q, ctx)
        }

        const { rows } = await execQuery_2(nativeQuery, ctx)

        return rows.map(row => makeEntOfRow(row, qent))
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

      return withDbClient(dbPool, async (client) => {
        const ctx = { seneca, client }
        const { qent, q } = msg

        return removeEnt(qent, q, ctx)
      })
    }),

    native: function (msg, done) {
      dbPool.connect().then(done).catch(done)
    }
  }

  internals.transformDBRowToJSObject = function (row) {
    const obj = {}

    for (const attr in row) {
      if (row.hasOwnProperty(attr)) {
        obj[ColumnNameParsing.fromColumnName(attr)] = row[attr]
      }
    }

    return obj
  }


  const meta = seneca.store.init(seneca, opts, store)

  seneca.add({ init: store.name, tag: meta.tag }, function (args, done) {
    return configure(opts, done)
  })

  seneca.add({ role: ACTION_ROLE, hook: 'generate_id', target: STORE_NAME }, function (args, done) {
    return done(null, { id: Uuid() })
  })

  return {name: store.name, tag: meta.tag}


  function stripInvalidLimitInPlace(q) {
    if (Array.isArray(q)) {
      return
    }

    if (!(typeof q.limit$ === 'number' && q.limit$ >= 0)) {
      delete q.limit$
    }
  }

  function stripInvalidSkipInPlace(q) {
    if (Array.isArray(q)) {
      return
    }

    if (!(typeof q.skip$ === 'number' && q.skip$ >= 0)) {
      delete q.skip$
    }
  }

  async function generateId(seneca) {
    const act = Util.promisify(seneca.act).bind(seneca)
    const result = await act({ role: ACTION_ROLE, hook: 'generate_id', target: STORE_NAME })

    const { id: newId } = result

    return newId
  }

  async function insertEnt(ent) {
    const query = QueryBuilder.savestm(ent)
    const res = await execQuery(query)

    if (res.rows && res.rows.length > 0) {
      // NOTE: res.rows should always be an array of length === 1,
      // however we want to play it safe here, and not crash the client
      // if something goes awry.
      //
      return makeEntOfRow(res.rows[0], ent)
    }

    return null
  }

  async function findEnt(ent, q, ctx) {
    const { client } = ctx
    const ent_table = RelationalStore.tablename(ent)

    const query = Q.selectstm({
      columns: '*',
      from: ent_table,
      where: whereOfQ(q, ctx),
      limit: 1,
      offset: 0 <= q.skip$ ? q.skip$ : null,
      order_by: q.sort$ || null,
      escapeIdentifier: client.escapeIdentifier.bind(client)
    })

    const { rows } = await execQuery(query)

    if (rows.length > 0) {
      return makeEntOfRow(rows[0], ent)
    }

    return null
  }

  async function listEnts(ent, q, ctx) {
    const { client } = ctx
    const ent_table = RelationalStore.tablename(ent)

    const query = Q.selectstm({
      columns: '*',
      from: ent_table,
      where: whereOfQ(q, ctx),
      limit: 0 <= q.limit$ ? q.limit$ : null,
      offset: 0 <= q.skip$ ? q.skip$ : null,
      order_by: q.sort$ || null,
      escapeIdentifier: client.escapeIdentifier.bind(client)
    })

    const { rows } = await execQuery(query)

    return rows.map((row) => makeEntOfRow(row, ent))
  }

  function whereOfQ(q, ctx) {
    if ('string' === typeof q || Array.isArray(q)) {
      return { id: q }
    }

    const { seneca } = ctx

    return seneca.util.clean(q)
  }

  async function upsertEnt(upsertFields, ent, q) {
    const query = QueryBuilder.upsertstm(ent, upsertFields)
    const res = await execQuery(query)

    if (res.rows && res.rows.length > 0) {
      // NOTE: res.rows should always be an array of length === 1,
      // however we want to play it safe here, and not crash the client
      // if something goes awry.
      //
      return makeEntOfRow(res.rows[0], ent)
    }

    return null
  }

  async function updateEnt(ent, ctx) {
    const { client } = ctx
    const escapeIdentifier = client.escapeIdentifier.bind(client)

    const ent_table = RelationalStore.tablename(ent)
    const entp = RelationalStore.makeentp(ent)

    const { id: ent_id } = ent

    const update_query = Q.updatestm({
      table: ent_table,
      set: intern.compact(entp),
      where: { id: ent_id },
      escapeIdentifier
    })

    const update = await execQuery_2(update_query, ctx)
    const updated_anything = update.rows.length > 0

    if (updated_anything) {
      return makeEntOfRow(update.rows[0], ent)
    }


    // TODO: Re-write using upserts on the id column:
    //
    const ins_query = Q.insertstm({
      into: ent_table,
      values: intern.compact(entp),
      escapeIdentifier
    })

    const insert = await execQuery_2(ins_query, ctx)

    return makeEntOfRow(insert.rows[0], ent)
  }

  async function removeEnt(ent, q, ctx) {
    if (q.all$) {
      return removeManyEnts(ent, q, ctx)
    }

    return removeOneEnt(ent, q, ctx)


    async function removeOneEnt(ent, q, ctx) {
      const { seneca, client } = ctx

      const ent_table = RelationalStore.tablename(ent)
      const escapeIdentifier = client.escapeIdentifier.bind(client)


      const sel_query = Q.selectstm({
        columns: ['id'],
        from: ent_table,
        where: seneca.util.clean(q),
        limit: 1,
        offset: 0 <= q.skip$ ? q.skip$ : null,
        order_by: q.sort$ || null,
        escapeIdentifier
      })

      const { rows: sel_rows } = await execQuery_2(sel_query, ctx)


      const del_query = Q.deletestm({
        from: ent_table,
        where: {
          id: sel_rows.map(x => x.id)
        },
        escapeIdentifier
      })

      const { rows: del_rows } = await execQuery_2(del_query, ctx)

      if (q.load$) {
        return 0 < del_rows.length
          ? makeEntOfRow(del_rows[0], ent)
          : null
      }

      return null
    }


    async function removeManyEnts(ent, q, ctx) {
      const { seneca, client } = ctx

      const ent_table = RelationalStore.tablename(ent)
      const escapeIdentifier = client.escapeIdentifier.bind(client)


      const sel_query = Q.selectstm({
        columns: ['id'],
        from: ent_table,
        where: seneca.util.clean(q),
        limit: 0 <= q.limit$ ? q.limit$ : null,
        offset: 0 <= q.skip$ ? q.skip$ : null,
        order_by: q.sort$ || null,
        escapeIdentifier
      })

      const { rows } = await execQuery_2(sel_query, ctx)


      const del_query = Q.deletestm({
        from: ent_table,
        where: {
          id: rows.map(x => x.id)
        },
        escapeIdentifier
      })

      await execQuery_2(del_query, ctx)

      return
    }
  }

  function makeEntOfRow(row, baseEnt) {
    const attrs = internals.transformDBRowToJSObject(row)
    return RelationalStore.makeent(baseEnt, attrs)
  }
}

