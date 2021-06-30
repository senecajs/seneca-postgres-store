const Assert = require('assert')
const Util = require('util')
const RelationalStore = require('./relational-util')
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
    const { client } = ctx
    const escapeIdentifier = client.escapeIdentifier.bind(client)

    const ent_table = RelationalStore.tablename(ent)
    const entp = RelationalStore.makeentp(ent)

    const ins_query = Q.insertstm({
      into: ent_table,
      values: intern.compact(entp),
      escapeIdentifier
    })

    const insert = await intern.execQuery(ins_query, ctx)

    return intern.makeEntOfRow(insert.rows[0], ent)
  },


  async findEnt(ent, q, ctx) {
    const { client } = ctx
    const ent_table = RelationalStore.tablename(ent)

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
      return intern.makeEntOfRow(rows[0], ent)
    }

    return null
  },


  async listEnts(ent, q, ctx) {
    const { client } = ctx
    const ent_table = RelationalStore.tablename(ent)

    const query = Q.selectstm({
      columns: '*',
      from: ent_table,
      where: intern.whereOfQ(q, ctx),
      limit: 0 <= q.limit$ ? q.limit$ : null,
      offset: 0 <= q.skip$ ? q.skip$ : null,
      order_by: q.sort$ || null,
      escapeIdentifier: client.escapeIdentifier.bind(client)
    })

    const { rows } = await intern.execQuery(query, ctx)

    return rows.map((row) => intern.makeEntOfRow(row, ent))
  },


  whereOfQ(q, ctx) {
    if ('string' === typeof q || Array.isArray(q)) {
      return { id: q }
    }

    const { seneca } = ctx

    return seneca.util.clean(q)
  },


  async upsertEnt(upsert_fields, ent, q, ctx) {
    const { client } = ctx
    const escapeIdentifier = client.escapeIdentifier.bind(client)

    const ent_table = RelationalStore.tablename(ent)
    const entp = RelationalStore.makeentp(ent)

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

    return intern.makeEntOfRow(rows[0], ent)
  },


  async updateEnt(ent, ctx) {
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

    const update = await intern.execQuery(update_query, ctx)
    const updated_anything = update.rows.length > 0

    if (updated_anything) {
      return intern.makeEntOfRow(update.rows[0], ent)
    }


    // TODO: Re-write using upserts on the id column:
    //
    const ins_query = Q.insertstm({
      into: ent_table,
      values: intern.compact(entp),
      escapeIdentifier
    })

    const insert = await intern.execQuery(ins_query, ctx)

    return intern.makeEntOfRow(insert.rows[0], ent)
  },


  async removeEnt(ent, q, ctx) {
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


  makeEntOfRow(row, ent) {
    return RelationalStore.makeent(ent, row)
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
  }
}

module.exports = { intern }
