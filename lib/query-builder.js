'use strict'

var RelationalStore = require('./relational-util')
var _ = require('lodash')
var mySQLStore = 'mysql-store'
var postgresStore = 'postgresql-store'
var operatorMap = {
  'or$': 'OR',
  'and$': 'AND'
}

module.exports = function (opts) {
  var fromColumn = opts.fromColumnName || _.identity
  var toColumn = opts.toColumnName || _.identity

  function parseExpression (sTypes, currentName, currentValue, params, values) {
    var result = {
      processed: true
    }

    if (currentName.indexOf('$') !== -1) {
      return result
    }

    if (currentValue === null) {
      // we can't use the equality on null because NULL != NULL
      params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(currentName)) + sTypes.escape + ' IS NULL')
      return result
    }

    if (currentValue instanceof RegExp) {
      var op = (currentValue.ignoreCase) ? '~*' : '~'
      values.push(currentValue.source)
      params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(currentName)) + sTypes.escape + op + RelationalStore.preparedStatements(sTypes.name, values.length))
      return result
    }

    if (_.isObject(currentValue)) {
      result.processed = false
      return result
    }
    else {
      values.push(currentValue)
      params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(currentName)) + sTypes.escape + '=' + RelationalStore.preparedStatements(sTypes.name, values.length))
      return result
    }

    return result
  }

  function parseOperation (entp, sTypes, currentName, currentValue, params, values, operator, parseExpression) {
    if (!_.isArray(currentValue)) {
      return {err: operator + ' operator requires an array value'}
    }

    var results = []
    for (var i in currentValue) {
      var w = whereargs(entp, currentValue[i])
      var subParams = []
      for (var j in w.params) {
        if (operatorMap[w.params[j]]) {
          parseOperation(entp, sTypes, w.params[j], w.values[j], subParams, values, operatorMap[w.params[j]], parseExpression)
          results.push(subParams.join(' AND '))
          continue
        }
        var current_result = parseExpression(sTypes, w.params[j], w.values[j], subParams, values)
        if (current_result.err) {
          return current_result
        }
        results.push(subParams.join(' AND '))
      }
    }

    var resultStr = ''
    for (i in results) {
      if (resultStr.length > 0) {
        resultStr += ' ' + operator + ' '
      }
      resultStr += results[i]
    }
    params.push('(' + resultStr + ')')
  }

  function buildQueryFromExpressionCustom (entp, query_parameters, sTypes, values, parseExpression) {
    var params = []
    values = values || []

    if (!_.isEmpty(query_parameters) && query_parameters.params.length > 0) {
      for (var i in query_parameters.params) {
        var currentName = query_parameters.params[i]
        var currentValue = query_parameters.values[i]

        if (operatorMap[currentName]) {
          parseOperation(entp, sTypes, currentName, currentValue, params, values, operatorMap[currentName], parseExpression)
          continue
        }

        var result = parseExpression(sTypes, currentName, currentValue, params, values)
        if (result.err) {
          return result
        }
      }

      return {err: null, data: params.join(' AND '), values: values}
    }
    else {
      return {values: values}
    }
  }

  function buildQueryFromExpression (entp, query_parameters, sTypes, values) {
    return buildQueryFromExpressionCustom(entp, query_parameters, sTypes, values, parseExpression)
  }

  function whereargs (entp, q) {
    var w = {}

    w.params = []
    w.values = []

    var qok = RelationalStore.fixquery(entp, q)

    for (var p in qok) {
      if (qok[p] !== undefined) {
        w.params.push(toColumn(p))
        w.values.push(qok[p])
      }
    }

    return w
  }

  function fixPrepStatement (stm, sTypes) {
    if (sTypes.name === mySQLStore) {
      return stm
    }

    var index = 1
    while (stm.indexOf('?') !== -1) {
      stm = stm.replace('?', '$' + index)
      index++
    }
    return stm
  }

  function jsonSupport (sTypes) {
    return (sTypes.name === postgresStore)
  }

  function savestm (ent, sTypes) {
    var stm = {}

    var table = RelationalStore.tablename(ent)
    var entp = RelationalStore.makeentp(ent, jsonSupport(sTypes))
    var fields = _.keys(entp)

    var values = []
    var params = []

    var cnt = 0

    var escapedFields = []
    fields.forEach(function (field) {
      escapedFields.push(sTypes.escape + RelationalStore.escapeStr(toColumn(field)) + sTypes.escape)
      values.push(entp[field])
      params.push(RelationalStore.preparedStatements(sTypes.name, ++cnt))
    })

    stm.text = 'INSERT INTO ' + RelationalStore.escapeStr(table) + ' (' + escapedFields + ') values (' + RelationalStore.escapeStr(params) + ')'
    stm.values = values

    return stm
  }

  function updatestm (ent, sTypes) {
    var stm = {}

    var table = RelationalStore.tablename(ent)
    var entp = RelationalStore.makeentp(ent, jsonSupport(sTypes))
    var fields = _.keys(entp)

    var values = []
    var params = []
    var cnt = 0

    fields.forEach(function (field) {
      if (field.indexOf('$') !== -1) {
        return
      }

      if (!_.isUndefined(entp[field])) {
        values.push(entp[field])
        params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(field)) + sTypes.escape + '=' + RelationalStore.preparedStatements(sTypes.name, ++cnt))
      }
    })

    stm.text = 'UPDATE ' + RelationalStore.escapeStr(table) + ' SET ' + params + " WHERE id='" + RelationalStore.escapeStr(ent.id) + "'"
    stm.values = values

    return stm
  }

  function upsertbyidstm (ent, sTypes) {
    var table = RelationalStore.tablename(ent)
    var entp = RelationalStore.makeentp(ent, jsonSupport(sTypes))


    var fields = _.keys(entp)

    var publicFields = fields.filter(function (field) {
      return field.indexOf('$') === -1
    })

    var publicDefinedFields = publicFields.filter(function (field) {
      return !_.isUndefined(entp[field])
    })


    var values = []
    var params = []
    var columns = []

    var cnt = 0

    publicDefinedFields.forEach(function (field) {
      values.push(entp[field])
      params.push(RelationalStore.preparedStatements(sTypes.name, ++cnt))
    })

    var paramsForUpdate = fields.map(function (_, i) {
      var key = fields[i]
      var value = params[i]

      return [key, value]
    })

    var columns = publicDefinedFields.map(function (field) {
      return toColumn(field)
    })


    var escapedColumns = columns.map(function (col) {
      return sTypes.escape + RelationalStore.escapeStr(col) + sTypes.escape
    })

    var escapedParams = RelationalStore.escapeStr(params)

    var escapedParamsForUpdate = paramsForUpdate.map(function (kv) {
      var col = kv[0]
      var escapedColumn = sTypes.escape + RelationalStore.escapeStr(col) + sTypes.escape

      var valuePlaceholder = kv[1]

      return [escapedColumn, valuePlaceholder].join('=')
    })


    var stm = {
      text: 'INSERT INTO ' + RelationalStore.escapeStr(table) +
        ' (' + escapedColumns + ') VALUES (' + escapedParams + ')' +
        ' ON CONFLICT (id) DO UPDATE SET ' + escapedParamsForUpdate +
        ' RETURNING *',

      values: values
    }


    return stm
  }

  function deletestm (qent, q, sTypes) {
    var stm = {}

    var table = RelationalStore.tablename(qent)
    var entp = RelationalStore.makeentp(qent, jsonSupport(sTypes))

    var values = []
    var params = []

    var cnt = 0

    var w = whereargs(entp, q)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      for (var i in w.params) {
        var param = w.params[i]
        var val = w.values[i]

        if (param.indexOf('$') !== -1) {
          continue
        }

        params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(param)) + sTypes.escape + '=' + RelationalStore.preparedStatements(sTypes.name, ++cnt))
        values.push(RelationalStore.escapeStr(val))
      }

      if (params.length > 0) {
        wherestr = ' WHERE ' + params.join(' AND ')
      }
      else {
        wherestr = ' '
      }
    }

    stm.text = 'DELETE FROM ' + RelationalStore.escapeStr(table) + wherestr
    stm.values = values

    return stm
  }

  function selectstmCustom (qent, q, sTypes, queryFromExpressionBuilder, done) {
    var specialOps = ['fields$']
    var specialOpsVal = {}

    var stm = {}

    for (var i in specialOps) {
      if (q[specialOps[i]]) {
        specialOpsVal[specialOps[i]] = q[specialOps[i]]
        delete q[specialOps[i]]
      }
    }

    var table = RelationalStore.tablename(qent)
    var entp = RelationalStore.makeentp(qent, jsonSupport(sTypes))


    var w = whereargs(entp, q)

    var response = queryFromExpressionBuilder(entp, w, sTypes)
    if (response.err) {
      return done(response.err)
    }

    var wherestr = response.data

    var values = response.values

    var mq = metaquery(qent, q)

    var metastr = ' ' + mq.params.join(' ')

    var what = '*'
    if (specialOpsVal['fields$'] && _.isArray(specialOpsVal['fields$']) && specialOpsVal['fields$'].length > 0) {
      what = ' ' + specialOpsVal['fields$'].join(', ')
      what += ', id '
    }

    stm.text = 'SELECT ' + what + ' FROM ' + RelationalStore.escapeStr(table) + (wherestr ? ' WHERE ' + wherestr : '') + RelationalStore.escapeStr(metastr)
    stm.values = values

    done(null, stm)
  }

  function selectstm (qent, q, sTypes, done) {
    selectstmCustom(qent, q, sTypes, buildQueryFromExpression, done)
  }

  function selectstmOr (qent, q, sTypes) {
    var specialOps = ['fields$']
    var specialOpsVal = {}

    var stm = {}

    for (var i in specialOps) {
      if (q[specialOps[i]]) {
        specialOpsVal[specialOps[i]] = q[specialOps[i]]
        delete q[specialOps[i]]
      }
    }

    var table = RelationalStore.tablename(qent)
    var entp = RelationalStore.makeentp(qent, jsonSupport(sTypes))

    var values = []
    var params = []

    var cnt = 0

    var w = whereargs(entp, q.ids)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      w.params.forEach(function (param) {
        params.push(sTypes.escape + RelationalStore.escapeStr(toColumn('id')) + sTypes.escape + '=' + RelationalStore.preparedStatements(sTypes.name, ++cnt))
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

    var mq = metaquery(qent, q)

    var metastr = ' ' + mq.params.join(' ')

    var what = '*'
    if (specialOpsVal['fields$'] && _.isArray(specialOpsVal['fields$']) && specialOpsVal['fields$'].length > 0) {
      what = ' ' + specialOpsVal['fields$'].join(', ')
      what += ', id '
    }

    stm.text = 'SELECT ' + what + ' FROM ' + RelationalStore.escapeStr(table) + wherestr + RelationalStore.escapeStr(metastr)
    stm.values = values

    return stm
  }

  function metaquery (qent, q) {
    var mq = {}

    mq.params = []
    mq.values = []

    if (q.sort$) {
      for (var sf in q.sort$) break
      var sd = q.sort$[sf] > 0 ? 'ASC' : 'DESC'
      mq.params.push('ORDER BY ' + toColumn(sf) + ' ' + sd)
    }

    if (q.limit$) {
      mq.params.push('LIMIT ' + q.limit$)
    }

    if (typeof q.skip$ === 'number' && q.skip$ >= 0) {
      mq.params.push('OFFSET ' + q.skip$)
    }

    return mq
  }

  function buildSelectStatementCustom (qent, q, sTypes, selectstm, selectstmOr, done) {
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
        return done(errorDetails)
      }
      query = {}
      query.text = fixPrepStatement(q[0], sTypes)
      query.values = _.clone(q)
      query.values.splice(0, 1)
      return done(null, query)
    }
    else {
      if (q.ids) {
        return done(null, selectstmOr(qent, q, sTypes))
      }
      else {
        selectstm(qent, q, sTypes, done)
      }
    }
  }

  function buildSelectStatement (qent, q, sTypes, done) {
    buildSelectStatementCustom(qent, q, sTypes, selectstm, selectstmOr, done)
  }

  return {
    buildQueryFromExpression: buildQueryFromExpression,
    updatestm: updatestm,
    upsertbyidstm: upsertbyidstm,
    deletestm: deletestm,
    selectstm: selectstm,
    selectstmCustom: selectstmCustom,
    savestm: savestm,
    fixPrepStatement: fixPrepStatement,
    selectstmOr: selectstmOr,
    jsonSupport: jsonSupport,
    whereargs: whereargs,
    metaquery: metaquery,
    buildQueryFromExpressionCustom: buildQueryFromExpressionCustom,
    parseExpression: parseExpression,
    makeent: RelationalStore.makeent,
    fromColumnName: fromColumn,
    toColumnName: toColumn,
    buildSelectStatement: buildSelectStatement,
    buildSelectStatementCustom: buildSelectStatementCustom
  }
}
