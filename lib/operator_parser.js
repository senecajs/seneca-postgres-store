'use strict'

var RelationalStore = require('./relational-util')
var _ = require('lodash')

var ne$ = function (current_name, value, params, values, sTypes) {
  values.push(value)
  params.push(sTypes.escape + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + sTypes.escape + '<>' + RelationalStore.preparedStatements(sTypes.name, values.length))
}

var eq$ = function (current_name, value, params, values, sTypes) {
  values.push(value)
  params.push(sTypes.escape + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + sTypes.escape + '=' + RelationalStore.preparedStatements(sTypes.name, values.length))
}

var gte$ = function (current_name, value, params, values, sTypes) {
  values.push(value)
  params.push(sTypes.escape + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + sTypes.escape + '>=' + RelationalStore.preparedStatements(sTypes.name, values.length))
}

var lte$ = function (current_name, value, params, values, sTypes) {
  values.push(value)
  params.push(sTypes.escape + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + sTypes.escape + '<=' + RelationalStore.preparedStatements(sTypes.name, values.length))
}

var gt$ = function (current_name, value, params, values, sTypes) {
  values.push(value)
  params.push(sTypes.escape + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + sTypes.escape + '>' + RelationalStore.preparedStatements(sTypes.name, values.length))
}

var lt$ = function (current_name, value, params, values, sTypes) {
  values.push(value)
  params.push(sTypes.escape + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + sTypes.escape + '<' + RelationalStore.preparedStatements(sTypes.name, values.length))
}

var in$ = function (current_name, value, params, values, sTypes) {
  if (!_.isArray(value)) {
    return {err: 'Operator in$ accepts only Array as value'}
  }
  value = _.clone(value)
  for (var index in value) {
    values.push(value[index])
    value[index] = RelationalStore.preparedStatements(sTypes.name, values.length)
  }

  params.push(sTypes.escape + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + sTypes.escape + ' IN (' + value + ')')
}

var nin$ = function (current_name, value, params, values, sTypes) {
  if (!_.isArray(value)) {
    return {err: 'Operator nin$ accepts only Array as value'}
  }

  value = _.clone(value)
  for (var index in value) {
    values.push(value[index])
    value[index] = RelationalStore.preparedStatements(sTypes.name, values.length)
  }
  params.push(sTypes.escape + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + sTypes.escape + ' NOT IN (' + value + ')')
}


module.exports.ne$ = ne$
module.exports.gte$ = gte$
module.exports.gt$ = gt$
module.exports.lte$ = lte$
module.exports.lt$ = lt$
module.exports.eq$ = eq$
module.exports.in$ = in$
module.exports.nin$ = nin$
