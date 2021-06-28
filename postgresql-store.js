'use strict'

var _ = require('lodash')
var Pg = require('pg')
var Uuid = require('uuid')

var STORE_NAME = 'postgresql-store'
var ACTION_ROLE = 'sql'

var RelationalStore = require('./lib/relational-util')

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
    name: STORE_NAME,

    close: function (args, done) {
      Pg.end()
      setImmediate(done)
    },

    save: function (args, done) {
      var seneca = this

      var ent = args.ent

      var q = args.q
      var autoIncrement = q.auto_increment$ || false

      if (isUpdate(ent)) {
        return updateEnt(ent, opts, function (err, res) {
          if (err) {
            seneca.log.error('save/update', 'Error while updating the entity:', err)
            return done(err)
          }

          var updatedAnything = res.rowCount > 0

          if (!updatedAnything) {
            return insertEnt(ent, function (err, res) {
              if (err) {
                seneca.log.error('save/insert', 'Error while inserting the entity:', err)
                return done(err)
              }

              seneca.log.debug('save/insert', res)

              return done(null, res)
            })
          }

          return findEnt(ent, { id: ent.id }, function (err, res) {
            if (err) {
              seneca.log.error('save/update', 'Error while fetching the updated entity:', err)
              return done(err)
            }

            seneca.log.debug('save/update', res)

            return done(null, res)
          })
        })
      }


      return generateId(seneca, STORE_NAME, function (err, generatedId) {
        if (err) {
          seneca.log.error('save/insert', 'Error while generating an id for the entity:', err)
          return done(err)
        }


        var newId = null == ent.id$
          ? generatedId
          : ent.id$


        var newEnt = ent.clone$()

        if (!autoIncrement) {
          newEnt.id = newId
        }


        if (isUpsert(ent, q)) {
          return upsertEnt(newEnt, q, function (err, res) {
            if (err) {
              seneca.log.error('save/upsert', 'Error while inserting the entity:', err)
              return done(err)
            }

            seneca.log.debug('save/upsert', res)

            return done(null, res)
          })
        }


        return insertEnt(newEnt, function (err, res) {
          if (err) {
            seneca.log.error('save/insert', 'Error while inserting the entity:', err)
            return done(err)
          }

          seneca.log.debug('save/insert', res)

          return done(null, res)
        })
      })

      function isUpsert(ent, q) {
        return !isUpdate(ent) &&
          Array.isArray(q.upsert$) &&
          internals.cleanArray(q.upsert$).length > 0
      }


      function isUpdate(ent) {
        return null != ent.id
      }
    },

    load: function (args, done) {
      var seneca = this

      var qent = args.qent
      var q = args.q

      return findEnt(qent, q, function (err, res) {
        if (err) {
          seneca.log.error('load', 'Error while fetching the entity:', err)
          return done(err)
        }

        seneca.log.debug('load', res)

        return done(null, res)
      })
    },

    list: function (args, done) {
      var seneca = this

      var qent = args.qent
      var q = args.q

      return listEnts(qent, q, function (err, res) {
        if (err) {
          seneca.log.error('list', 'Error while listing the entities:', err)
          return done(err)
        }

        seneca.log.debug('list', q, res.length)

        return done(null, res)
      })
    },

    remove: function (args, done) {
      var seneca = this

      var qent = args.qent
      var q = args.q

      return removeEnt(qent, q, function (err, res) {
        if (err) {
          seneca.log.error('remove', 'Error while removing the entity/entities:', err)
          return done(err)
        }

        seneca.log.debug('remove', q)

        return done(null, res)
      })
    },

    native: function (args, done) {
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

  internals.cleanArray = function (ary) {
    var isPublicProp = (p) => !p.includes('$')
    return ary.filter(isPublicProp)
  }


  var meta = seneca.store.init(seneca, opts, store)

  seneca.add({ init: store.name, tag: meta.tag }, function (args, cb) {
    configure(opts, function (err) {
      cb(err)
    })
  })

  seneca.add({role: ACTION_ROLE, hook: 'generate_id', target: store.name}, function (args, done) {
    return done(null, {id: Uuid()})
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

  function generateId(seneca, target, done) {
    return seneca.act({ role: ACTION_ROLE, hook: 'generate_id', target: target }, function (err, res) {
      if (err) {
        return done(err)
      }

      var newId = res.id

      return done(null, newId)
    })
  }

  function insertEnt(ent, done) {
    var query = QueryBuilder.savestm(ent)

    return execQuery(query, function (err, res) {
      if (err) {
        return done(err)
      }

      if (res.rows && res.rows.length > 0) {
        // NOTE: res.rows should always be an array of length === 1,
        // however we want to play it safe here, and not crash the client
        // if something goes awry.
        //
        return done(null, makeEntOfRow(res.rows[0], ent))
      }

      return done(null, null)
    })
  }

  function findEnt(ent, q, done) {
    try {
      var query = buildLoadStm(ent, q)

      return execQuery(query, function (err, res) {
        if (err) {
          return done(err)
        }

        if (res.rows && res.rows.length > 0) {
          return done(null, makeEntOfRow(res.rows[0], ent))
        }

        return done(null, null)
      })
    } catch (err) {
      return done(err)
    }
  }

  function listEnts(ent, q, done) {
    try {
      var query = buildListStm(ent, q)

      return execQuery(query, function (err, res) {
        if (err) {
          return done(err)
        }

        var list = res.rows.map(function (row) {
          return makeEntOfRow(row, ent)
        })

        return done(null, list)
      })
    } catch (err) {
      return done(err)
    }
  }

  function upsertEnt(ent, q, done) {
    try {
      var upsertFields = internals.cleanArray(q.upsert$)
      var query = QueryBuilder.upsertstm(ent, upsertFields)

      return execQuery(query, function (err, res) {
        if (err) {
          return done(err)
        }

        if (res.rows && res.rows.length > 0) {
          // NOTE: res.rows should always be an array of length === 1,
          // however we want to play it safe here, and not crash the client
          // if something goes awry.
          //
          return done(null, makeEntOfRow(res.rows[0], ent))
        }

        return done(null, null)
      })
    } catch (err) {
      return done(err)
    }
  }

  function updateEnt(ent, opts, done) {
    try {
      var query = QueryBuilder.updatestm(ent)

      return execQuery(query, done)
    } catch (err) {
      return done(err)
    }
  }

  function removeEnt(ent, q, done) {
    try {
      var delQuery = buildRemoveStm(ent, q)

      return execQuery(delQuery, function (err, res) {
        if (err) {
          return done(err)
        }

        var shouldLoad = !q.all$ && q.load$

        if (shouldLoad && res.rows.length > 0) {
          return done(null, makeEntOfRow(res.rows[0], ent))
        }

        return done(null, null)
      })
    } catch (err) {
      return done(err)
    }
  }

  function makeEntOfRow(row, baseEnt) {
    var attrs = internals.transformDBRowToJSObject(row)
    var newEnt = RelationalStore.makeent(baseEnt, attrs)

    return newEnt
  }
}

