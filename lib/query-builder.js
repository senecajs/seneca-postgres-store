'use strict'

var RelationalStore = require('./relational-util')
var _ = require('lodash')
var OpParser = require('./operator_parser')
var mySQLStore = 'mysql-store'
var postgresStore = 'postgresql-store'

const { intern } = require('./intern')
const { sTypes } = intern

var operatorMap = {
  'or$': 'OR',
  'and$': 'AND'
}

module.exports = function (opts) {
  var fromColumn = opts.fromColumnName || _.identity
  var toColumn = opts.toColumnName || _.identity

  function parseExpression (currentName, currentValue, params, values) {
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
      params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(currentName)) + sTypes.escape + op + RelationalStore.preparedStatements(values.length))
      return result
    }

    if (_.isObject(currentValue)) {
      result.processed = false
      return result
    }
    else {
      values.push(currentValue)
      params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(currentName)) + sTypes.escape + '=' + RelationalStore.preparedStatements(values.length))
      return result
    }

    return result
  }

  function parseExtendedExpression (currentName, currentValue, params, values) {
    function parseComplexSelectOperator (currentName, currentValue, params, values) {
      var result = {}

      result.processed = _.every(currentValue, function (opVal, op) {
        if (!OpParser[op]) {
          result.err = 'This operator is not yet implemented: ' + op
          return false
        }
        var err = OpParser[op](currentName, opVal, params, values)
        if (err) {
          result.err = err
          return false
        }

        return true
      })

      return result
    }

    var result = parseExpression(currentName, currentValue, params, values)

    if (!result.processed && _.isObject(currentValue)) {
      result = parseComplexSelectOperator(currentName, currentValue, params, values)
      if (result.err) {
        return result
      }
    }

    return result
  }


  function parseOperation (entp, currentName, currentValue, params, values, operator, parseExpression) {
    if (!_.isArray(currentValue)) {
      return {err: operator + ' operator requires an array value'}
    }

    var results = []
    for (var i in currentValue) {
      var w = whereargs(entp, currentValue[i])
      var subParams = []
      for (var j in w.params) {
        if (operatorMap[w.params[j]]) {
          parseOperation(entp, w.params[j], w.values[j], subParams, values, operatorMap[w.params[j]], parseExpression)
          results.push(subParams.join(' AND '))
          continue
        }
        var current_result = parseExpression(w.params[j], w.values[j], subParams, values)
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

  function buildQueryFromExpressionCustom (entp, query_parameters, values, parseExpression) {
    var params = []
    values = values || []

    if (!_.isEmpty(query_parameters) && query_parameters.params.length > 0) {
      for (var i in query_parameters.params) {
        var currentName = query_parameters.params[i]
        var currentValue = query_parameters.values[i]

        if (operatorMap[currentName]) {
          parseOperation(entp, currentName, currentValue, params, values, operatorMap[currentName], parseExpression)
          continue
        }

        var result = parseExpression(currentName, currentValue, params, values)
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

  function buildQueryFromExpression (entp, queryParameters, values) {
    return buildQueryFromExpressionCustom(entp, queryParameters, values, parseExtendedExpression)
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

  function fixPrepStatement (stm) {
    var index = 1
    while (stm.indexOf('?') !== -1) {
      stm = stm.replace('?', '$' + index)
      index++
    }
    return stm
  }

  function jsonSupport () {
    return true
  }

  function savestm (ent) {
    var stm = {}

    var table = RelationalStore.tablename(ent)
    var entp = RelationalStore.makeentp(ent, jsonSupport())
    var fields = _.keys(entp)

    var values = []
    var params = []

    var cnt = 0

    var escapedFields = []
    fields.forEach(function (field) {
      escapedFields.push(sTypes.escape + RelationalStore.escapeStr(toColumn(field)) + sTypes.escape)
      values.push(entp[field])
      params.push(RelationalStore.preparedStatements(++cnt))
    })

    stm.text = 'INSERT INTO ' + RelationalStore.escapeStr(table) +
      ' (' + escapedFields + ') VALUES (' + RelationalStore.escapeStr(params) + ')' +
      ' RETURNING *'

    stm.values = values

    return stm
  }

  function updatestm(ent, opts) {
    var table = RelationalStore.tablename(ent)
    var entp = RelationalStore.makeentp(ent, jsonSupport())

    var fields = fieldsRequestedForUpdate(entp)
    var values = valuesForMerge(fields, entp)

    return buildStatement(fields, values)


    function fieldsRequestedForUpdate(entp) {
      var fields = _.keys(entp)

      var publicFields = fields.filter(function (field) {
        return field.indexOf('$') === -1
      })

      var publicDefinedFields = publicFields.filter(function (field) {
        return !_.isUndefined(entp[field])
      })

      return publicDefinedFields
    }

    function valuesForMerge(fields, entp) {
      return fields.map(function (field) {
        return entp[field]
      })
    }

    function buildStatement(fields, values) {
      var params = fields.map(function (field, i) {
        var placeholderNum = i + 1
        return RelationalStore.preparedStatements(placeholderNum)
      })

      var escapedParams = params


      var paramsForUpdate = fields.map(function (_, i) {
        var key = fields[i]
        var value = params[i]

        return [key, value]
      })

      var escapedParamsForUpdate = paramsForUpdate.map(function (kv) {
        var col = kv[0]
        var escapedColumn = sTypes.escape + RelationalStore.escapeStr(col) + sTypes.escape

        var valuePlaceholder = kv[1]

        return [escapedColumn, valuePlaceholder].join('=')
      })


      var stm = {
        text: 'UPDATE ' + RelationalStore.escapeStr(table) + ' SET ' +
          escapedParamsForUpdate.join(', ') +
          " WHERE id='" + RelationalStore.escapeStr(ent.id) + "'",

        values: values
      }


      return stm
    }
  }

  function upsertstm(ent, upsertFields) {
    var table = RelationalStore.tablename(ent)
    var entp = RelationalStore.makeentp(ent, jsonSupport())

    var fields = fieldsRequestedForUpdate(entp)
    var values = valuesForMerge(fields, entp)

    return buildStatement(fields, values, upsertFields)


    function fieldsRequestedForUpdate(entp) {
      var fields = _.keys(entp)

      var publicFields = fields.filter(function (field) {
        return field.indexOf('$') === -1
      })

      var publicDefinedFields = publicFields.filter(function (field) {
        return !_.isUndefined(entp[field])
      })

      return publicDefinedFields
    }

    function valuesForMerge(fields, entp) {
      return fields.map(function (field) {
        return entp[field]
      })
    }

    function buildStatement(fields, values, upsertFields) {
      var params = fields.map(function (field, i) {
        var placeholderNum = i + 1
        return RelationalStore.preparedStatements(placeholderNum)
      })

      var escapedParams = params


      var paramsForUpdate = fields
        .map(function (_, i) {
          var key = fields[i]
          var value = params[i]

          return [key, value]
        })
        .filter(function (kv) {
          var key = kv[0]
          return key !== 'id'
        })

      var escapedParamsForUpdate = paramsForUpdate.map(function (kv) {
        var col = kv[0]
        var escapedColumn = escapeColumn(col)

        var valuePlaceholder = kv[1]

        return [escapedColumn, valuePlaceholder].join('=')
      })


      var columns = fields.map(toColumn)

      var escapedColumns = columns.map(function (col) {
        return escapeColumn(col)
      })


      var upsertColumns = upsertFields.map(toColumn)

      var escapedUpsertColumns = upsertColumns.map(function (col) {
        return escapeColumn(col)
      })


      var stm = {
        text: 'INSERT INTO ' + RelationalStore.escapeStr(table) +
          ' (' + escapedColumns.join(', ') + ') VALUES (' +
          escapedParams.join(', ') + ')' + ' ON CONFLICT (' +
          escapedUpsertColumns + ') DO UPDATE SET ' +
          escapedParamsForUpdate.join(', ') + ' RETURNING *',

        values: values
      }


      return stm
    }
  }

  function deletestm (qent, q) {
    var selQ = _.clone(q)

    if (!selQ.all$) {
      selQ.limit$ = 1
    }

    var select = selectstm(qent, selQ, ['id'])
    var table = RelationalStore.tablename(qent)


    var stm = {
      text: 'DELETE FROM ' + RelationalStore.escapeStr(table) +
        ' WHERE ' + escapeColumn(toColumn('id')) +
        ' IN (' + select.text + ') RETURNING *',

      values: select.values
    }


    return stm
  }

  function selectstmCustom (qent, q, queryFromExpressionBuilder, selectFields = null) {
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
    var entp = RelationalStore.makeentp(qent, jsonSupport())


    var w = whereargs(entp, q)

    var response = queryFromExpressionBuilder(entp, w)
    if (response.err) {
      throw response.err
    }

    var wherestr = response.data

    var values = response.values

    var mq = metaquery(qent, q)

    var metastr = ' ' + mq.params.join(' ')

    var what = '*'
    if (specialOpsVal['fields$'] && _.isArray(specialOpsVal['fields$']) && specialOpsVal['fields$'].length > 0) {
      what = ' ' + specialOpsVal['fields$'].join(', ')
      what += ', id '
    } else if (null != selectFields) {
      if (Array.isArray(selectFields)) {
        var columns = selectFields.map(toColumn)

        var escapedColumns = columns.map(function (col) {
          return escapeColumn(col)
        })

        what = escapedColumns.join(', ')
      } else if (selectFields === '*') {
        what = selectFields
      } else {
        throw new Error('The optional `selectFields` arg, if given, must be either an array or a "*"')
      }
    }

    stm.text = 'SELECT ' + what + ' FROM ' + RelationalStore.escapeStr(table) + (wherestr ? ' WHERE ' + wherestr : '') + RelationalStore.escapeStr(metastr)
    stm.values = values

    return stm
  }

  function selectstm (qent, q, selectFields = null) {
    return selectstmCustom(qent, q, buildQueryFromExpression, selectFields)
  }

  function selectstmOr (qent, q) {
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
    var entp = RelationalStore.makeentp(qent, jsonSupport())

    var values = []
    var params = []

    var cnt = 0

    var w = whereargs(entp, q.ids)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      w.params.forEach(function (param) {
        params.push(sTypes.escape + RelationalStore.escapeStr(toColumn('id')) + sTypes.escape + '=' + RelationalStore.preparedStatements(++cnt))
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

  function buildSelectStatementCustom (qent, q, selectstm, selectstmOr) {
    if (_.isString(q.native$)) {
      return q.native$
    }

    if (_.isArray(q.native$)) {
      // first element in array should be query, the other being values
      //
      if (q.native$.length === 0) {
        throw new Error('Invalid query')
      }
      var query = {}
      query.text = fixPrepStatement(q.native$[0])
      query.values = _.clone(q.native$)
      query.values.splice(0, 1)

      return query
    }

    if (q.ids) {
      return selectstmOr(qent, q)
    }

    if (_.isArray(q)) {
      return selectstmOr(qent, { ids: q })
    }

    return selectstm(qent, q)
  }

  function buildSelectStatement (qent, q) {
    return buildSelectStatementCustom(qent, q, selectstm, selectstmOr)
  }

  function escapeColumn(col) {
    return sTypes.escape + RelationalStore.escapeStr(col) + sTypes.escape
  }


  return {
    buildQueryFromExpression: buildQueryFromExpression,
    updatestm: updatestm,
    upsertstm: upsertstm,
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
