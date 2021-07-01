const Assert = require('assert')
const Util = require('util')
const _ = require('lodash')
const Q = require('./qbuilder')
const Uuid = require('uuid')


const intern = {
  sTypes: {
    escape: '"',
    prepared: '$'
  },


  asyncMethod(f) {
    return function (msg, done) {
      const seneca = this
      const p = f.call(seneca, msg)

      Assert('function' === typeof p.then &&
      'function' === typeof p.catch,
      'The function must be async, i.e. return a promise.')

      return p
        .then(result => done(null, result))
        .catch(done)
    }
  },


  compact(obj) {
    return Object.keys(obj)
      .map(k => [k, obj[k]])
      .filter(([, v]) => undefined !== v)
      .reduce((acc, [k, v]) => {
        acc[k] = v
        return acc
      }, {})
  },


  async withDbClient(dbPool, f) {
    const client = await dbPool.connect()

    let result

    try {
      result = await f(client)
    } finally {
      client.release()
    }

    return result
  },


  async execQuery(query, ctx) {
    if (!query) {
      throw new Error('An empty query is not a valid query')
    }

    const { client } = ctx

    return client.query(query)
  },


  async insertEnt(ent, ctx) {
    const {
      client,
      fromColumnName: fromColumn = _.identity,
      toColumnName: toColumn = _.identity
    } = ctx

    const ent_table = intern.tablename(ent)
    const entp = intern.makeentp(ent)

    const values = intern.deepXformKeys(toColumn, intern.compact(entp))
    const escapeIdentifier = client.escapeIdentifier.bind(client)

    const ins_query = Q.insertstm({
      into: ent_table,
      values,
      escapeIdentifier
    })

    const insert = await intern.execQuery(ins_query, ctx)
    const ent_fields = intern.deepXformKeys(fromColumn, insert.rows[0])

    return intern.makeent(ent, ent_fields)
  },


  async findEnt(ent, q, ctx) {
    const { client } = ctx
    const ent_table = intern.tablename(ent)

    const query = Q.selectstm({
      columns: '*',
      from: ent_table,
      where: intern.whereOfQ(q, ctx),
      limit: 1,
      offset: 0 <= q.skip$ ? q.skip$ : null,
      order_by: q.sort$ || null,
      escapeIdentifier: client.escapeIdentifier.bind(client)
    })

    const { rows } = await intern.execQuery(query, ctx)

    if (rows.length > 0) {
      return intern.makeent(ent, rows[0])
    }

    return null
  },


  async listEnts(ent, q, ctx) {
    const ent_table = intern.tablename(ent)

    const {
      client,
      fromColumnName: fromColumn = _.identity,
      toColumnName: toColumn = _.identity
    } = ctx

    const columns = q.fields$
      ? intern.deepXformKeys(toColumn, q.fields$)
      : '*'

    const where = intern.deepXformKeys(toColumn, intern.whereOfQ(q, ctx))

    const query = Q.selectstm({
      columns,
      from: ent_table,
      where,
      limit: 0 <= q.limit$ ? q.limit$ : null,
      offset: 0 <= q.skip$ ? q.skip$ : null,
      order_by: q.sort$ || null,
      escapeIdentifier: client.escapeIdentifier.bind(client)
    })

    const { rows } = await intern.execQuery(query, ctx)

    return rows
      .map((row) => intern.deepXformKeys(fromColumn, row))
      .map((row) => intern.makeent(ent, row))
  },


  filterObj(f, obj) {
    const out = {}

    for (const k in obj) {
      const v = obj[k]

      if (f(k, v)) {
        out[k] = v
      }
    }

    return out
  },


  whereOfQ(q, ctx) {
    if ('string' === typeof q || Array.isArray(q)) {
      return { id: q }
    }


    const { seneca } = ctx
    const cq = seneca.util.clean(q)


    const ops = intern.filterObj(Q.isOp, q)

    if ('ids' in cq) {
      Assert(Array.isArray(cq.ids), 'ids must be an array of ids')

      ops.in$ = ops.in$ || {}
      ops.in$.id = (ops.in$.id || []).concat(q.ids)

      delete cq.ids
    }


    return { ...cq, ...ops }
  },


  async upsertEnt(upsert_fields, ent, q, ctx) {
    const { client } = ctx
    const escapeIdentifier = client.escapeIdentifier.bind(client)

    const ent_table = intern.tablename(ent)
    const entp = intern.makeentp(ent)

    const insert_values = intern.compact(entp)
    const set_values = intern.compact(entp); delete set_values.id

    const query = Q.insertstm({
      into: ent_table,
      values: insert_values,
      on_conflict: {
        columns: upsert_fields,
        do_update: {
          set: set_values
        }
      },
      escapeIdentifier
    })

    const { rows } = await intern.execQuery(query, ctx)

    return intern.makeent(ent, rows[0])
  },


  async updateEnt(ent, ctx) {
    const { client } = ctx
    const escapeIdentifier = client.escapeIdentifier.bind(client)

    const ent_table = intern.tablename(ent)
    const entp = intern.makeentp(ent)

    const { id: ent_id } = ent

    const update_query = Q.updatestm({
      table: ent_table,
      set: intern.compact(entp),
      where: { id: ent_id },
      escapeIdentifier
    })

    const update = await intern.execQuery(update_query, ctx)
    const updated_anything = update.rows.length > 0

    if (updated_anything) {
      return intern.makeent(ent, update.rows[0])
    }


    // TODO: Re-write using upserts on the id column:
    //
    const ins_query = Q.insertstm({
      into: ent_table,
      values: intern.compact(entp),
      escapeIdentifier
    })

    const insert = await intern.execQuery(ins_query, ctx)

    return intern.makeent(ent, insert.rows[0])
  },


  async removeEnt(ent, q, ctx) {
    if (q.all$) {
      return removeManyEnts(ent, q, ctx)
    }

    return removeOneEnt(ent, q, ctx)


    async function removeOneEnt(ent, q, ctx) {
      const { seneca, client } = ctx

      const ent_table = intern.tablename(ent)
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

      const { rows: sel_rows } = await intern.execQuery(sel_query, ctx)


      const del_query = Q.deletestm({
        from: ent_table,
        where: {
          id: sel_rows.map(x => x.id)
        },
        escapeIdentifier
      })

      const { rows: del_rows } = await intern.execQuery(del_query, ctx)

      if (q.load$) {
        return 0 < del_rows.length
          ? intern.makeent(ent, del_rows[0])
          : null
      }

      return null
    }


    async function removeManyEnts(ent, q, ctx) {
      const { seneca, client } = ctx

      const ent_table = intern.tablename(ent)
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

      const { rows } = await intern.execQuery(sel_query, ctx)


      const del_query = Q.deletestm({
        from: ent_table,
        where: {
          id: rows.map(x => x.id)
        },
        escapeIdentifier
      })

      await intern.execQuery(del_query, ctx)

      return
    }
  },


  msgForGenerateId(args) {
    const { role, target } = args
    return { role, target, hook: 'generate_id' }
  },


  async askSenecaToGenerateId(args, ctx) {
    const { seneca } = ctx

    const act = Util.promisify(seneca.act).bind(seneca)
    const result = await act(intern.msgForGenerateId(args))

    const { id: newId } = result

    return newId
  },


  generateId() {
    return Uuid()
  },


  getConfig(spec) {
    let conf

    if ('string' === typeof spec) {
      const urlM = /^postgres:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec)

      conf = {}
      conf.name = urlM[7]
      conf.port = urlM[6]
      conf.host = urlM[4]
      conf.username = urlM[2]
      conf.password = urlM[3]
      conf.port = conf.port ? parseInt(conf.port, 10) : null
    } else {
      conf = spec
    }

    // pg conf properties
    conf.user = conf.username
    conf.database = conf.name

    conf.host = conf.host || conf.server
    conf.username = conf.username || conf.user
    conf.password = conf.password || conf.pass

    return conf
  },


  maybeUpsert(msg) {
    const { ent, q } = msg

    if (!Array.isArray(q.upsert$)) {
      return null
    }

    const upsertFields = q.upsert$.filter((p) => !p.includes('$'))

    if (0 === upsertFields.length) {
      return null
    }

    return upsertFields
  },


  isUpdate(msg) {
    const { ent } = msg
    return null != ent.id
  },


  isObject(x) {
    return null != x && '[object Object]' === toString.call(x)
  },


  isDate(x) {
    return '[object Date]' === toString.call(x)
  },


  deepXformKeys(f, x) {
    if (Array.isArray(x)) {
      return x.map(y => intern.deepXformKeys(f, y))
    }

    if (intern.isObject(x)) {
      const out = {}

      for (const k in x) {
        out[f(k)] = intern.deepXformKeys(f, x[k])
      }

      return out
    }

    return x
  },


  /**
   * NOTE: makeentp is used to create a new persistable entity from the entity
   * object.
   */
  makeentp(ent) {
    const fields = ent.fields$()
    const entp = {}

    for (const field of fields) {
      if (!intern.isDate(ent[field]) && intern.isObject(ent[field])) {
        entp[field] = JSON.stringify(ent[field])
      } else {
        entp[field] = ent[field]
      }
    }

    return entp
  },


  /**
   * NOTE: makeent is used to create a new entity using a row from a database.
   *
   */
  makeent(ent, row) {
    if (!row) {
      return null
    }

    const fields = Object.keys(row)
    const entp = {}

    for (const field of fields) {
      let value = row[field]

      try {
        const parsed = JSON.parse(row[field])

        if (intern.isObject(parsed)) {
          value = parsed
        }
      } catch (err) {
        if (!(err instanceof SyntaxError)) {
          throw err
        }
      }

      entp[field] = value
    }

    return ent.make$(entp)
  },


  tablename(ent) {
    const canon = ent.canon$({ object: true })

    return (canon.base ? canon.base + '_' : '') + canon.name
  }
}

module.exports = { intern }
