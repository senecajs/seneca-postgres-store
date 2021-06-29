'use strict'

var _ = require('lodash')
var Pg = require('pg')
var Uuid = require('uuid')
var RelationalStore = require('./lib/relational-util')

const Util = require('util')
const { intern } = require('./lib/intern')
const { asyncMethod } = intern

var STORE_NAME = 'postgresql-store'
var ACTION_ROLE = 'sql'

var MIN_WAIT = 16
var MAX_WAIT = 5000

module.exports = function (opts) {
  var seneca = this

  opts.minwait = opts.minwait || MIN_WAIT
  opts.maxwait = opts.maxwait || MAX_WAIT

  var ColumnNameParsing = {
    fromColumnName: opts.fromColumnName || _.identity,
    toColumnName: opts.toColumnName || _.identity
  }
  var QueryBuilder = require('./lib/query-builder')(ColumnNameParsing)


  var minwait
  var internals = {}

  function error (query, args, err/*, next*/) {
    if (err) {
      var errorDetails = {
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

  var pgConf

  function configure (spec, done) {
    pgConf = 'string' === typeof (spec) ? null : spec

    if (!pgConf) {
      pgConf = {}

      var urlM = /^postgres:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec)
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

    setImmediate(function () {
      return done(undefined)
    })
  }

  function execQuery (query) {
    return new Promise((resolve, reject) => {
      return Pg.connect(pgConf, (err, client, releaseConnection) => {
        if (err) {
          return reject(err)
        }

        if (!query) {
          seneca.log.error('An empty query is not a valid query')
          releaseConnection()

          return reject(err)
        }

        return client.query(query, (err, res) => {
          releaseConnection()

          if (err) {
            return reject(err)
          }

          return resolve(res)
        })
      })
    })
  }


  var store = {
    name: STORE_NAME,

    close: function (args, done) {
      Pg.end()
      return done()
    },

    save: asyncMethod(async function (msg) {
      const seneca = this

      const { ent, q } = msg
      const { auto_increment$: autoIncrement = false } = q

      if (isUpdate(ent)) {
        const update = await updateEnt(ent)
        const updatedAnything = update.rowCount > 0

        if (!updatedAnything) {
          return insertEnt(ent)
        }

        return findEnt(ent, { id: ent.id })
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
    }),

    load: asyncMethod(async function (msg) {
      var qent = msg.qent
      var q = msg.q

      return findEnt(qent, q)
    }),

    list: asyncMethod(async function (msg) {
      var qent = msg.qent
      var q = msg.q

      return listEnts(qent, q)
    }),

    remove: asyncMethod(async function (msg) {
      var qent = msg.qent
      var q = msg.q

      return removeEnt(qent, q)
    }),

    native: function (msg, done) {
      Pg.connect(pgConf, done)
    }
  }

  internals.transformDBRowToJSObject = function (row) {
    var obj = {}
    for (var attr in row) {
      if (row.hasOwnProperty(attr)) {
        obj[ColumnNameParsing.fromColumnName(attr)] = row[attr]
      }
    }
    return obj
  }


  var meta = seneca.store.init(seneca, opts, store)

  seneca.add({ init: store.name, tag: meta.tag }, function (args, cb) {
    configure(opts, function (err) {
      cb(err)
    })
  })

  seneca.add({ role: ACTION_ROLE, hook: 'generate_id', target: STORE_NAME }, function (args, done) {
    return done(null, { id: Uuid() })
  })

  return {name: store.name, tag: meta.tag}


  function buildLoadStm(ent, q) {
    var loadQ = _.clone(q)
    loadQ.limit$ = 1

    return QueryBuilder.selectstm(ent, loadQ)
  }

  function buildListStm(ent, q) {
    var cleanQ = _.clone(q)
    stripInvalidLimitInPlace(cleanQ)
    stripInvalidLimitInPlace(cleanQ)

    return QueryBuilder.buildSelectStatement(ent, cleanQ)
  }

  function buildRemoveStm(ent, q) {
    var cleanQ = _.clone(q)
    stripInvalidLimitInPlace(cleanQ)
    stripInvalidSkipInPlace(cleanQ)

    return QueryBuilder.deletestm(ent, cleanQ)
  }

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

  async function findEnt(ent, q) {
    const query = buildLoadStm(ent, q)
    const res = await execQuery(query)

    if (res.rows && res.rows.length > 0) {
      return makeEntOfRow(res.rows[0], ent)
    }

    return null
  }

  async function listEnts(ent, q) {
    const query = buildListStm(ent, q)
    const res = await execQuery(query)

    const list = res.rows.map((row) => {
      return makeEntOfRow(row, ent)
    })

    return list
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

  async function updateEnt(ent) {
    const query = QueryBuilder.updatestm(ent)
    return execQuery(query)
  }

  async function removeEnt(ent, q) {
    const delQuery = buildRemoveStm(ent, q)
    const res = await execQuery(delQuery)
    const shouldLoad = !q.all$ && q.load$

    if (shouldLoad && res.rows.length > 0) {
      return makeEntOfRow(res.rows[0], ent)
    }

    return null
  }

  function makeEntOfRow(row, baseEnt) {
    const attrs = internals.transformDBRowToJSObject(row)
    return RelationalStore.makeent(baseEnt, attrs)
  }
}

