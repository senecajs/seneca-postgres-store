/* Copyright (c) 2012-2013 Marian Radulescu */
'use strict'

var _ = require('lodash')
var Pg = require('pg')
var Uuid = require('node-uuid')
var RelationalStore = require('./relational-util')
var QueryBuilder = require('./query-builder')

var name = 'postgresql-store'

var MIN_WAIT = 16
var MAX_WAIT = 5000

module.exports = function (opts) {
  var seneca = this

  opts.minwait = opts.minwait || MIN_WAIT
  opts.maxwait = opts.maxwait || MAX_WAIT

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

      seneca.act({role: name, hook: 'save'}, args, function (err, queryObj) {
        var query = queryObj.query
        var operation = queryObj.operation

        if (err) {
          seneca.log.error('Postgres save error', err)
          return done(err, {code: operation, tag: args.tag$, store: store.name, query: query, error: err})
        }

        execQuery(query, function (err, res) {
          if (error(query, args, err)) {
            seneca.log.error(query.text, query.values, err)
            return done({code: operation, tag: args.tag$, store: store.name, query: query, error: err})
          }

          seneca.log(args.tag$, operation, args.ent)
          return done(null, args.ent)
        })
      })
    },

    load: function (args, done) {
      var seneca = this

      seneca.act({role: name, hook: 'load'}, args, function (err, queryObj) {
        var qent = args.qent
        var query = queryObj.query

        if (err) {
          seneca.log.error('Postgres load error', err)
          return done(err, {code: 'load', tag: args.tag$, store: store.name, query: query, error: err})
        }

        execQuery(query, function (err, res) {
          if (error(query, args, err)) {
            var trace = new Error()
            seneca.log.error(query.text, query.values, trace.stack)
            return done({code: 'load', tag: args.tag$, store: store.name, query: query, error: err})
          }

          var ent = null
          if (res.rows && res.rows.length > 0) {
            var attrs = internals.transformDBRowToJSObject(res.rows[0])
            ent = RelationalStore.makeent(qent, attrs)
          }
          seneca.log(args.tag$, 'load', ent)
          return done(null, ent)
        })
      })
    },

    list: function (args, done) {
      var seneca = this

      seneca.act({role: name, hook: 'list'}, args, function (err, queryObj) {
        var qent = args.qent
        var q = args.q
        var query = queryObj.query

        var list = []

        if (err) {
          seneca.log.error('Postgres list error', err)
          return done(err, {code: 'list', tag: args.tag$, store: store.name, query: q, error: err})
        }

        execQuery(query, function (err, res) {
          if (error(query, args, err, done)) {
            return done(null, {code: 'list', tag: args.tag$, store: store.name, query: query, error: err})
          }

          res.rows.forEach(function (row) {
            var attrs = internals.transformDBRowToJSObject(row)
            var ent = RelationalStore.makeent(qent, attrs)
            list.push(ent)
          })
          seneca.log(args.tag$, 'list', list.length, list[0])
          return done(null, list)
        })
      })
    },

    remove: function (args, done) {
      var seneca = this
      var q = args.q

      function executeRemove (args, done) {
        seneca.act({role: name, hook: 'remove'}, args, function (err, queryObj) {
          var query = queryObj.query
          if (err) {
            seneca.log.error('Postgres list error', err)
            return done(err, {code: 'remove', tag: args.tag$, store: store.name, query: q, error: err})
          }
          execQuery(query, function (err, res) {
            if (!error(query, args, err, done)) {
              seneca.log(args.tag$, 'remove', res.rowCount)
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
      }

      if (q.all$) {
        executeRemove(args, done)
      }
      else {
        seneca.act({role: name, hook: 'list'}, args, function (err, queryObj) {
          var query = queryObj.query

          if (err) {
            seneca.log.error('Postgres list error', err)
            return done(err, {code: 'list', tag: args.tag$, store: store.name, query: q, error: err})
          }

          execQuery(query, function (err, res) {
            if (error(query, args, err, done)) {
              var errorDetails = {
                message: err.message,
                err: err,
                stack: err.stack,
                query: query
              }
              seneca.log.error('Query Failed', JSON.stringify(errorDetails, null, 1))
              return done(err)
            }

            var entp = res.rows[0]

            if (entp) {
              executeRemove(args, done)
            }
            else {
              return done(null)
            }
          })
        })
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
        obj[RelationalStore.snakeToCamelCase(attr)] = row[attr]
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

  seneca.add({role: store.name, hook: 'save'}, function (args, done) {
    var ent = args.ent
    var query
    var update = !!ent.id

    if (update) {
      query = QueryBuilder.updatestm(ent)
    }
    else {
      ent.id = ent.id$ || Uuid()
      query = QueryBuilder.savestm(ent)
    }

    return done(null, {query: query, operation: update ? 'update' : 'save'})
  })

  seneca.add({role: store.name, hook: 'load'}, function (args, done) {
    var qent = args.qent
    var q = args.q

    QueryBuilder.selectstm(qent, q, function (err, query) {
      return done(err, {query: query})
    })
  })

  seneca.add({role: store.name, hook: 'list'}, function (args, done) {
    var qent = args.qent
    var q = args.q

    buildSelectStatement(q, function (err, query) {
      return done(err, {query: query})
    })

    function buildSelectStatement (q, done) {
      var query

      if (_.isString(q)) {
        return done(null, q)
      }
      else if (_.isArray(q)) {
        // first element in array should be query, the other being values
        if (q.length === 0) {
          var errorDetails = {
            message: 'Invalid query',
            query: q
          }
          seneca.log.error('Invalid query')
          return done(errorDetails)
        }
        query = {}
        query.text = QueryBuilder.fixPrepStatement(q[0])
        query.values = _.clone(q)
        query.values.splice(0, 1)
        return done(null, query)
      }
      else {
        if (q.ids) {
          return done(null, QueryBuilder.selectstmOr(qent, q))
        }
        else {
          QueryBuilder.selectstm(qent, q, done)
        }
      }
    }
  })

  seneca.add({role: store.name, hook: 'remove'}, function (args, done) {
    var qent = args.qent
    var q = args.q

    var query = QueryBuilder.deletestm(qent, q)
    return done(null, {query: query})
  })

  return {name: store.name, tag: meta.tag}
}
