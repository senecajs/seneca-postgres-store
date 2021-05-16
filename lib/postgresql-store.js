'use strict'

var _ = require('lodash')
var Pg = require('pg')
var Uuid = require('node-uuid')
var name = 'postgresql-store'
var actionRole = 'sql'

var StandardQueryPlugin = require('seneca-standard-query')
var RelationalStore = require('./relational-util')

var MIN_WAIT = 16
var MAX_WAIT = 5000

module.exports = function (opts) {
  var seneca = this

  opts.minwait = opts.minwait || MIN_WAIT
  opts.maxwait = opts.maxwait || MAX_WAIT

  var ColumnNameParsing = {
    fromColumnName: opts.fromColumnName,
    toColumnName: opts.toColumnName
  }
  var QueryBuilder = require('./query-builder')(ColumnNameParsing)

  seneca.use(StandardQueryPlugin, ColumnNameParsing)
  var StandardQuery
  seneca.ready(function () {
    StandardQuery = seneca.export('standard-query/utils')
  })


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
      // next ({code: 'entity/error', store: name})

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

  function execQuery (query, done) {
    Pg.connect(pgConf, function (err, client, releaseConnection) {
      if (err) {
        seneca.log.error('Connection error', err)
        return done(err, undefined)
      }
      else {
        if (!query) {
          err = new Error('Query cannot be empty')
          seneca.log.error('An empty query is not a valid query', err)
          releaseConnection()
          return done(err, undefined)
        }

        client.query(query, function (err, res) {
          releaseConnection()
          return done(err, res)
        })
      }
    })
  }


  var store = {

    name: name,

    close: function (args, done) {
      Pg.end()
      setImmediate(done)
    },

    save: function (args, done) {
      var seneca = this

      return execGetSchema(Object.assign({}, args, { target: name }), function (err, schema) {
        if (err) {
          seneca.log.error('Error while trying to build a schema:', err)
          return done(err)
        }

        return execSave(Object.assign({}, args, { target: name }), schema, function (err, newEnt) {
          if (err) {
            seneca.log.error('Error while trying to save an entity:', err)
            return done(err)
          }

          return done(null, newEnt)
        })
      })
    },

    load: function (args, done) {
      var seneca = this

      return buildLoadStm(Object.assign({}, args, { target: name }), function (err, queryObj) {
        var qent = args.qent
        var query = queryObj.query

        if (err) {
          seneca.log.error('Postgres load error', err)
          return done(err, {code: 'load', tag: args.tag$, store: store.name, query: query, error: err})
        }

        return execQuery(query, function (err, res) {
          if (error(query, args, err)) {
            var trace = new Error()
            seneca.log.error(query.text, query.values, trace.stack)
            return done({code: 'load', tag: args.tag$, store: store.name, query: query, error: err})
          }

          var ent = null
          if (res.rows && res.rows.length > 0) {
            var attrs = internals.transformDBRowToJSObject(res.rows[0])
            ent = StandardQuery.makeent(qent, attrs)
          }

          seneca.log.debug({ tag$: args.tag$ }, 'load', ent)

          return done(null, ent)
        })
      })
    },

    list: function (args, done) {
      var seneca = this

      return buildListStm(Object.assign({}, args, { target: name }), function (err, queryObj) {
        var qent = args.qent
        var q = args.q
        var query = queryObj.query

        var list = []

        if (err) {
          seneca.log.error('Postgres list error', err)
          return done(err, {code: 'list', tag: args.tag$, store: store.name, query: q, error: err})
        }

        return execQuery(query, function (err, res) {
          if (error(query, args, err, done)) {
            return done(null, {code: 'list', tag: args.tag$, store: store.name, query: query, error: err})
          }

          res.rows.forEach(function (row) {
            var attrs = internals.transformDBRowToJSObject(row)
            var ent = StandardQuery.makeent(qent, attrs)
            list.push(ent)
          })

          seneca.log.debug({ tag$: args.tag$ }, 'list', list.length, list[0])

          return done(null, list)
        })
      })
    },

    // TODO: Please consider re-writing this method.
    //
    remove: function (args, done) {
      var seneca = this
      var q = args.q

      return listEntitiesToBeRemovedStm(Object.assign({}, args, { target: name }), function (err, queryObj) {
        var query = queryObj.query

        if (err) {
          seneca.log.error('Error loading an entity/entities:', err)
          return done(err)
        }

        return execQuery(query, function (err, res) {
          if (error(query, args, err, done)) {
            seneca.log.error('Error removing an entity/entities:', err)
            return done(err)
          }

          var entps = res.rows
          var ids = entps.map(function (row) { return row.id })

          return buildRemoveByIdsStm(Object.assign({}, args, { target: name }), ids, function (err, queryObj) {
            if (err) {
              seneca.log.error('Postgres list error', err)
              return done(err, {code: 'remove', tag: args.tag$, store: store.name, query: q, error: err})
            }

            var query = queryObj.query

            return execQuery(query, function (err, res) {
              if (!error(query, args, err, done)) {
                seneca.log.debug({ tag$: args.tag$ }, 'remove', res.rowCount)

                var should_load = !args.q.all$ && args.q.load$

                if (should_load && entps.length > 0) {
                  var attrs = internals.transformDBRowToJSObject(entps[0])
                  var newEnt = StandardQuery.makeent(args.ent, attrs)

                  return done(null, newEnt)
                }

                return done()
              }
              else if (err) {
                return done(err)
              }
              else {
                err = new Error('no candidate for deletion')
                err.critical = false
                return done(err)
              }
            })
          })
        })
      })


      function listEntitiesToBeRemovedStm(args, done) {
        if (args.q.all$) {
          return buildListStm(args, done)
        }

        return buildLoadStm(args, done)
      }


      function executeRemove(ids, done) {
      }

    },

    native: function (args, done) {
      Pg.connect(pgConf, done)
    }
  }

  internals.transformDBRowToJSObject = function (row) {
    var obj = {}
    for (var attr in row) {
      if (row.hasOwnProperty(attr)) {
        obj[StandardQuery.fromColumnName(attr)] = row[attr]
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

  seneca.add({role: actionRole, hook: 'generate_id', target: store.name}, function (args, done) {
    return done(null, {id: Uuid()})
  })

  return {name: store.name, tag: meta.tag}


  function buildRemoveByIdsStm(args, ids, done) {
    var qent = args.qent
    var sTypes = specificTypes(args.target)

    var query = QueryBuilder.deletebyidsstm(qent, ids, sTypes)

    return done(null, {query: query})
  }

  function buildLoadStm(args, done) {
    var qent = args.qent
    var sTypes = specificTypes(args.target)
    var q = _.clone(args.q)
    q.limit$ = 1

    return QueryBuilder.selectstm(qent, q, sTypes, function (err, query) {
      return done(err, {query: query})
    })
  }

  function buildListStm(args, done) {
    if (null != args.q.native$) {
      return buildNativeStm(args, done)
    }

    var qent = args.qent
    var sTypes = specificTypes(args.target)


    var q = _.clone(args.q)
    var has_query_limit = !Array.isArray(q) && typeof q.limit$ === 'number'

    if (!(has_query_limit && q.limit$ >= 0)) {
      delete q.limit$
    }


    return QueryBuilder.buildSelectStatement(qent, q, sTypes, function (err, query) {
      return done(err, {query: query})
    })
  }

  function buildNativeStm(args, done) {
    var qent = args.qent
    var sTypes = specificTypes(args.target)
    var q = args.q

    if (typeof q.native$ === 'string') {
      return done(null, { query: { text: q.native$, values: [] } })
    }

    if (Array.isArray(q.native$)) {
      if (q.native$.length === 0 || typeof q.native$[0] !== 'string') {
        return done(new Error('Expected the first item in the array to be a SQL string.'))
      }

      var text = (function () {
        var placeholderNum = 1

        return q.native$[0].replace(/\?/g, function () {
          return RelationalStore.preparedStatements(sTypes.name, placeholderNum++)
        })
      })()

      var values = q.native$.slice(1)
      var query = { text: text, values: values }

      return done(null, { query: query })
    }

    return done(new Error('native$ can only be a SQL string,' +
      ' or an array in format: [sql_query, ...values]'))
  }

  function buildSchemaStm(args, done) {
    var sTypes = specificTypes(args.target)
    var query = QueryBuilder.schemastm(args.ent, sTypes)

    return done(null, { query: query })
  }

  function execGetSchema(args, done) {
    return buildSchemaStm(args, function (err, queryObj) {
      if (err) {
        return done(err)
      }

      var query = queryObj.query

      return execQuery(query, function (err, res) {
        if (error(query, args, err)) {
          return done(err)
        }

        var schema = res.rows

        return done(null, schema)
      })
    })
  }

  function execSave(args, schema, done) {
    return buildSaveStm(args, schema, function (err, queryObj) {
      if (err) {
        return done(err)
      }

      var query = queryObj.query
      var operation = queryObj.operation

      return execQuery(query, function (err, res) {
        if (error(query, args, err)) {
          return done(err)
        }


        var newEnt = null

        if (res.rows && res.rows.length > 0) {
          // NOTE: res.rows should always be an array of length === 1,
          // however we want to play it safe here, and not crash the client
          // if something goes awry.
          //
          var attrs = internals.transformDBRowToJSObject(res.rows[0])
          newEnt = StandardQuery.makeent(args.ent, attrs)
        }


        seneca.log.debug({ tag$: args.tag$ }, operation, newEnt)


        return done(null, newEnt)
      })
    })
  }

  function buildSaveStm(args, schema, done) {
    var ent = args.ent
    var query
    var autoIncrement = args.auto_increment || false
    var sTypes = specificTypes(args.target)


    var update = !!ent.id

    if (update) {
      var merge = shouldMerge(ent, opts)
      query = QueryBuilder.upsertbyidstm(ent, sTypes, schema, { merge })

      return done(null, {query: query, operation: 'update'})
    }


    if (ent.id$) {
      ent.id = ent.id$
      query = QueryBuilder.savestm(ent, sTypes)
      return done(null, {query: query, operation: 'save'})
    }

    if (autoIncrement) {
      query = QueryBuilder.savestm(ent, sTypes)
      return done(null, {query: query, operation: 'save'})
    }

    return seneca.act({role: actionRole, hook: 'generate_id', target: args.target}, function (err, result) {
      if (err) {
        seneca.log.error('hook generate_id failed')
        return done(err)
      }

      var newId = result.id
      ent.id = newId

      query = QueryBuilder.savestm(ent, sTypes)

      return done(null, { query: query, operation: 'save' })
    })
  }

  function specificTypes(storeName) {
    var sTypes = {
      escape: '"',
      prepared: '$'
    }
    sTypes.name = storeName

    if (storeName === 'mysql-store') {
      sTypes.escape = '`'
      sTypes.prepared = '?'
    }

    return sTypes
  }

  function shouldMerge(ent, plugin_opts) {
    if ('merge$' in ent) {
      return Boolean(ent.merge$)
    }

    if (plugin_opts && ('merge' in plugin_opts)) {
      return Boolean(plugin_opts.merge)
    }

    return true
  }
}
