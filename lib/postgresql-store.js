/* Copyright (c) 2012-2013 Marian Radulescu */
'use strict'

var _ = require('lodash')
var pg = require('pg')
var uuid = require('node-uuid')
var relationalstore = require('./relational-util')

var name = 'postgresql-store'

var MIN_WAIT = 16
var MAX_WAIT = 5000

module.exports = function (opts) {
  var seneca = this

  opts.minwait = opts.minwait || MIN_WAIT
  opts.maxwait = opts.maxwait || MAX_WAIT
  var nolimit = opts.nolimit || false

  var minwait

  var upperCaseRegExp = /[A-Z]/g

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
        query = internals.updatestm(ent)
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

        query = internals.savestm(ent)

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

      internals.selectstm(qent, q, function (err, query) {
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
          query.text = internals.fixPrepStatement(q[0])
          query.values = _.clone(q)
          query.values.splice(0, 1)
          return done(null, query)
        }
        else {
          if (q.distinct$ || q.fields$) {
            return done(null, internals.filterStatement(qent, q))
          }
          else if (q.ids) {
            return done(null, internals.selectstmOr(qent, q))
          }
          else {
            internals.selectstm(qent, q, done)
          }
        }
      }
    },


    remove: function (args, done) {
      var qent = args.qent
      var q = args.q

      if (q.all$) {
        var query = internals.deletestm(qent, q)

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
        internals.selectstm(qent, q, function (err, selectQuery) {
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

            if (!entp) {
              err = new Error('no candidate for deletion')
              err.critical = false
              return done(err)
            }
            else {
              var query = internals.deletestm(qent, {id: entp.id})

              execQuery(query, function (err, res) {
                if (err) {
                  return done(err)
                }

                seneca.log(args.tag$, 'remove', res.rowCount)
                return done(null)
              })
            }
          })
        })
      }
    },

    native: function (args, done) {
      pg.connect(pgConf, done)
    }
  }

  internals.fixPrepStatement = function (stm) {
    var index = 1
    while (stm.indexOf('?') !== -1) {
      stm = stm.replace('?', '$' + index)
      index++
    }
    return stm
  }

  internals.savestm = function (ent) {
    var stm = {}

    var table = relationalstore.tablename(ent)
    var entp = relationalstore.makeentp(ent)
    var fields = _.keys(entp)

    var values = []
    var params = []

    var cnt = 0

    var escapedFields = []
    fields.forEach(function (field) {
      escapedFields.push('"' + internals.escapeStr(internals.camelToSnakeCase(field)) + '"')
      values.push(entp[field])
      params.push('$' + (++cnt))
    })

    stm.text = 'INSERT INTO ' + internals.escapeStr(table) + ' (' + escapedFields + ') values (' + internals.escapeStr(params) + ')'
    stm.values = values

    return stm
  }


  internals.updatestm = function (ent) {
    var stm = {}

    var table = relationalstore.tablename(ent)
    var entp = relationalstore.makeentp(ent)
    var fields = _.keys(entp)

    var values = []
    var params = []
    var cnt = 0

    fields.forEach(function (field) {
      if (!_.isUndefined(entp[field])) {
        values.push(entp[field])
        params.push('"' + internals.escapeStr(internals.camelToSnakeCase(field)) + '"=$' + (++cnt))
      }
    })

    stm.text = 'UPDATE ' + internals.escapeStr(table) + ' SET ' + params + " WHERE id='" + internals.escapeStr(ent.id) + "'"
    stm.values = values

    return stm
  }


  internals.deletestm = function (qent, q) {
    var stm = {}

    var table = relationalstore.tablename(qent)
    var entp = relationalstore.makeentp(qent)

    var values = []
    var params = []

    var cnt = 0

    var w = internals.whereargs(entp, q)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      w.params.forEach(function (param) {
        params.push('"' + internals.escapeStr(internals.camelToSnakeCase(param)) + '"=$' + (++cnt))
      })

      if (!_.isEmpty(w.values)) {
        w.values.forEach(function (val) {
          values.push(internals.escapeStr(val))
        })
      }

      wherestr = ' WHERE ' + params.join(' AND ')
    }

    stm.text = 'DELETE FROM ' + internals.escapeStr(table) + wherestr
    stm.values = values

    return stm
  }


  internals.filterStatement = function (qent, q) {
    var stm = {}

    var table = relationalstore.tablename(qent)
    var entp = relationalstore.makeentp(qent)

    var values = []
    var params = []

    var cnt = 0

    var w = internals.whereargs(entp, q)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      w.params.forEach(function (param) {
        params.push('"' + internals.escapeStr(internals.camelToSnakeCase(param)) + '"=$' + (++cnt))
      })

      w.values.forEach(function (value) {
        values.push(value)
      })

      wherestr = ' WHERE ' + params.join(' AND ')
    }

    var mq = internals.metaquery(qent, q)

    var metastr = ' ' + mq.params.join(' ')

    var filterParams = q.distinct$ || q.fields$

    var selectColumns = []
    if (filterParams && !_.isString(filterParams) && _.isArray(filterParams)) {
      selectColumns = filterParams
    }
    if (selectColumns.length === 0) {
      selectColumns.push('*')
    }

    var select = 'SELECT ' + (q.distinct$ ? 'DISTINCT ' : '')
    stm.text = select + internals.escapeStr(selectColumns.join(',')) + ' FROM ' + internals.escapeStr(table) + wherestr + internals.escapeStr(metastr)
    stm.values = values

    return stm
  }

  internals.selectstm = function (qent, q, done) {
    var stm = {}

    var table = relationalstore.tablename(qent)
    var entp = relationalstore.makeentp(qent)

    var w = internals.whereargs(entp, q)

    internals.buildWhereStr(w, function (err, wherestr) {
      if (err) {
        return done(err)
      }

      var values = []
      w.values.forEach(function (value) {
        values.push(value)
      })

      var mq = internals.metaquery(qent, q)

      var metastr = ' ' + mq.params.join(' ')

      stm.text = 'SELECT * FROM ' + internals.escapeStr(table) + wherestr + internals.escapeStr(metastr)
      stm.values = values

      done(null, stm)
    })
  }

  internals.selectstmOr = function (qent, q) {
    var stm = {}

    var table = relationalstore.tablename(qent)
    var entp = relationalstore.makeentp(qent)

    var values = []
    var params = []

    var cnt = 0

    var w = internals.whereargs(entp, q.ids)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      w.params.forEach(function (param) {
        params.push('"' + internals.escapeStr(internals.camelToSnakeCase('id')) + '"=$' + (++cnt))
      })

      w.values.forEach(function (value) {
        values.push(value)
      })

      wherestr = ' WHERE ' + params.join(' OR ')
    }

    // This is required to set the limit$ to be the length of the 'ids' array, so that in situations
    // when it's not set in the query(q) it won't be applied the default limit$ of 20 records
    if (!q.limit$) {
      q.limit$ = q.ids.length
    }

    var mq = internals.metaquery(qent, q)

    var metastr = ' ' + mq.params.join(' ')

    stm.text = 'SELECT * FROM ' + internals.escapeStr(table) + wherestr + internals.escapeStr(metastr)
    stm.values = values

    return stm
  }

  internals.whereargs = function (entp, q) {
    var w = {}

    w.params = []
    w.values = []

    var qok = relationalstore.fixquery(entp, q)

    for (var p in qok) {
      if (qok[p] !== undefined) {
        w.params.push(internals.camelToSnakeCase(p))
        w.values.push(qok[p])
      }
    }

    return w
  }

  internals.metaquery = function (qent, q) {
    var mq = {}

    mq.params = []
    mq.values = []

    if (q.sort$) {
      for (var sf in q.sort$) break
      var sd = q.sort$[sf] > 0 ? 'ASC' : 'DESC'
      mq.params.push('ORDER BY ' + internals.camelToSnakeCase(sf) + ' ' + sd)
    }

    if (q.limit$) {
      mq.params.push('LIMIT ' + q.limit$)
    }
    else {
      if (nolimit === false) {
        mq.params.push('LIMIT 20')
      }
    }

    if (q.skip$) {
      mq.params.push('OFFSET ' + q.skip$)
    }

    return mq
  }

  internals.escapeStr = function (input) {
    if (input instanceof Date) {
      return input
    }
    var str = '' + input
    return str.replace(/[\0\b\t\x08\x09\x1a\n\r"'\\\%]/g, function (char) {
      switch (char) {
        case '\0':
          return '\\0'
        case '\x08':
          return '\\b'
        case '\b':
          return '\\b'
        case '\x09':
          return '\\t'
        case '\t':
          return '\\t'
        case '\x1a':
          return '\\z'
        case '\n':
          return '\\n'
        case '\r':
          return '\\r'
        case '\"':
        case '\'':
        case '\\':
        case '%':
          return '\\' + char

      }
    })
  }

  internals.buildWhereStr = function (w, done) {
    var cnt = 0
    var params = []
    if (!_.isEmpty(w) && w.params.length > 0) {
      var i = 0
      for (var i in w.params) {
        var current_name = w.params[i]
        var current_value = w.values[i]

        if (current_value === null) {
          // we can't use the equality on null because NULL != NULL
          w.values.splice(i, 1)
          params.push('"' + internals.escapeStr(internals.camelToSnakeCase(current_name)) + '" IS NULL')
        }
        else if (current_value instanceof RegExp) {
          var op = (current_value.ignoreCase) ? '~*' : '~'
          params.push('"' + internals.escapeStr(internals.camelToSnakeCase(current_name)) + '"' + op + '$' + (++cnt))
          w.values[i] = w.values[i].source
        }
        else if (_.isObject(current_value)) {
          var err = parseComplexSelectOperator(current_name, current_value, w, i, params)
          if (err) {
            return done(err)
          }
        }
        else {
          op = '='
          params.push('"' + internals.escapeStr(internals.camelToSnakeCase(current_name)) + '"' + op + '$' + (++cnt))
        }
      }

      return done(null, ' WHERE ' + params.join(' AND '))
    }
    else {
      return done(null, ' ')
    }


    function parseComplexSelectOperator (current_name, current_value, w, i, params) {
      var complex_operators = {
        'ne$': function (operator, value, params) {
          params.push('"' + internals.escapeStr(internals.camelToSnakeCase(current_name)) + '"' + '<>' + '$' + (++cnt))
        },
        'eq$': function (operator, value, params) {
          params.push('"' + internals.escapeStr(internals.camelToSnakeCase(current_name)) + '"' + '=' + '$' + (++cnt))
        },
        'gte$': function (operator, value, params) {
          params.push('"' + internals.escapeStr(internals.camelToSnakeCase(current_name)) + '"' + '>=' + '$' + (++cnt))
        },
        'lte$': function (operator, value, params) {
          params.push('"' + internals.escapeStr(internals.camelToSnakeCase(current_name)) + '"' + '<=' + '$' + (++cnt))
        },
        'gt$': function (operator, value, params) {
          params.push('"' + internals.escapeStr(internals.camelToSnakeCase(current_name)) + '"' + '>' + '$' + (++cnt))
        },
        'lt$': function (operator, value, params) {
          params.push('"' + internals.escapeStr(internals.camelToSnakeCase(current_name)) + '"' + '<' + '$' + (++cnt))
        }

      }

      for (var op in current_value) {
        var op_val = current_value[op]
        w.values[i] = op_val
        if (!complex_operators[op]) {
          // report error - unknown operator
          return 'This operator is not yet implemented: ' + op
        }
        var err = complex_operators[op](op, op_val, params)
        if (err) {
          return err
        }
      }
    }
  }

  internals.camelToSnakeCase = function (field) {
    // replace "camelCase" with "camel_case"
    upperCaseRegExp.lastIndex = 0 // just to be sure. does not seem necessary. String.replace seems to reset the regexp each time.
    return field.replace(upperCaseRegExp, function (str, offset) {
      return ('_' + str.toLowerCase())
    })
  }

  internals.snakeToCamelCase = function (column) {
    // replace "snake_case" with "snakeCase"
    var arr = column.split('_')
    var field = arr[0]
    for (var i = 1; i < arr.length; i++) {
      field += arr[i][0].toUpperCase() + arr[i].slice(1, arr[i].length)
    }

    return field
  }

  internals.transformDBRowToJSObject = function (row) {
    var obj = {}
    for (var attr in row) {
      if (row.hasOwnProperty(attr)) {
        obj[internals.snakeToCamelCase(attr)] = row[attr]
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
