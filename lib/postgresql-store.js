/* Copyright (c) 2012-2013 Marian Radulescu */
'use strict'

var _ = require('lodash')
var pg = require('pg')
var uuid = require('node-uuid')
var relationalstore = require('./relational-util')
var query_builder = require('./query_builder')

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
    pg.connect(pgConf, function (err, client, releaseConnection) {
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
      pg.end()
      setImmediate(done)
    },

    save: function (args, done) {
      var ent = args.ent
      var query
      var update = !!ent.id

      if (update) {
        query = query_builder.updatestm(ent)
        execQuery(query, function (err, res) {
          if (error(query, args, err)) {
            seneca.log.error(query.text, query.values, err)
            return done({code: 'update', tag: args.tag$, store: store.name, query: query, error: err})
          }

          seneca.log(args.tag$, 'update', ent)
          return done(null, ent)
        })
      }
      else {
        ent.id = ent.id$ || uuid()

        query = query_builder.savestm(ent)

        execQuery(query, function (err, res) {
          if (error(query, args, err)) {
            seneca.log.error(query.text, query.values, err)
            return done({code: 'save', tag: args.tag$, store: store.name, query: query, error: err})
          }

          seneca.log(args.tag$, 'save', ent)
          return done(null, ent)
        })
      }
    },

    load: function (args, done) {
      var qent = args.qent
      var q = args.q

      query_builder.selectstm(qent, q, function (err, query) {
        if (err) {
          return done({code: 'load', tag: args.tag$, store: store.name, query: query, error: err})
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
            ent = relationalstore.makeent(qent, attrs)
          }
          seneca.log(args.tag$, 'load', ent)
          return done(null, ent)
        })
      })
    },

    list: function (args, done) {
      var qent = args.qent
      var q = args.q

      var list = []

      buildSelectStatement(q, function (err, query) {
        if (err) {
          seneca.log.error('Postgres list error', err)
          return done({code: 'list', tag: args.tag$, store: store.name, query: q, error: err})
        }

        execQuery(query, function (err, res) {
          if (error(query, args, err, done)) {
            return done({code: 'list', tag: args.tag$, store: store.name, query: query, error: err})
          }

          res.rows.forEach(function (row) {
            var attrs = internals.transformDBRowToJSObject(row)
            var ent = relationalstore.makeent(qent, attrs)
            list.push(ent)
          })
          seneca.log(args.tag$, 'list', list.length, list[0])
          return done(null, list)
        })
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
          query.text = query_builder.fixPrepStatement(q[0])
          query.values = _.clone(q)
          query.values.splice(0, 1)
          return done(null, query)
        }
        else {
          if (q.ids) {
            return done(null, query_builder.selectstmOr(qent, q))
          }
          else {
            query_builder.selectstm(qent, q, done)
          }
        }
      }
    },

    remove: function (args, done) {
      var qent = args.qent
      var q = args.q

      if (q.all$) {
        var query = query_builder.deletestm(qent, q)

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
      }
      else {
        query_builder.selectstm(qent, q, function (err, selectQuery) {
          if (err) {
            var errorDetails = {
              message: err.message,
              err: err,
              stack: err.stack,
              query: query
            }
            seneca.log.error('Query Failed', JSON.stringify(errorDetails, null, 1))
            return done(err)
          }

          execQuery(selectQuery, function (err, res) {
            if (error(selectQuery, args, err, done)) {
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
              var query = query_builder.deletestm(qent, {id: entp.id})

              execQuery(query, function (err, res) {
                if (err) {
                  return done(err)
                }

                seneca.log(args.tag$, 'remove', res.rowCount)
                return done(null)
              })
            }
            else {
              return done(null)
            }
          })
        })
      }
    },

    native: function (args, done) {
      pg.connect(pgConf, done)
    }
  }

  internals.transformDBRowToJSObject = function (row) {
    var obj = {}
    for (var attr in row) {
      if (row.hasOwnProperty(attr)) {
        obj[relationalstore.snakeToCamelCase(attr)] = row[attr]
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

  return {name: store.name, tag: meta.tag}
}
