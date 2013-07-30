/*jslint node: true*/
/*jslint asi: true */
/* Base class for relational databases */
"use strict";

var _ = require('underscore')

var SENECA_TYPE_COLUMN = 'seneca'

var OBJECT_TYPE = 'o'
var ARRAY_TYPE = 'a'
var DATE_TYPE = 'd'


module.exports.fixquery = function (entp, q) {
  var qq = {};

  for (var qp in q) {
    if (!qp.match(/\$$/)) {
      qq[qp] = q[qp]
    }
  }

  if (_.isFunction(qq.id)) {
    delete qq.id
  }

  return qq
}


/**
 * Create a new persistable entity from the entity object. The function adds
 * the value for SENECA_TYPE_COLUMN with hints for type of the serialized objects.
 *
 * @param ent entity
 * @return {Object}
 */
module.exports.makeentp = function (ent) {
  var entp = {}
  var type = {}
  var fields = ent.fields$()

  fields.forEach(function (field) {
    if (_.isDate(ent[field])) {
      type[field] = DATE_TYPE;
      entp[field] = JSON.stringify(ent[field])
    }
    else if (_.isArray(ent[field])) {
      type[field] = ARRAY_TYPE;
      entp[field] = JSON.stringify(ent[field])
    }
    else if (_.isObject(ent[field])) {
      type[field] = OBJECT_TYPE;
      entp[field] = JSON.stringify(ent[field])
    }
    else {
      entp[field] = ent[field]
    }
  })

  if (!_.isEmpty(type)) {
    entp[SENECA_TYPE_COLUMN] = JSON.stringify(type)
  }

  return entp
}


/**
 * Create a new entity using a row from database. This function is using type
 * hints from database column SENECA_TYPE_COLUMN to deserialize stored values
 * into proper objects.
 *
 * @param ent entity
 * @param row database row data
 * @return {Entity}
 */
module.exports.makeent = function (ent, row) {
  var entp = {}
  var senecatype = {}
  var fields = _.keys(row)

  if (!_.isUndefined(row[SENECA_TYPE_COLUMN]) && !_.isNull(row[SENECA_TYPE_COLUMN])) {
    senecatype = JSON.parse(row[SENECA_TYPE_COLUMN])
  }

  if (!_.isUndefined(ent) && !_.isUndefined(row)) {
    fields.forEach(function (field) {
      if (SENECA_TYPE_COLUMN != field) {
        if (_.isUndefined(senecatype[field])) {
          entp[field] = row[field]
        }
        else if (senecatype[field] == OBJECT_TYPE) {
          entp[field] = JSON.parse(row[field])
        }
        else if (senecatype[field] == ARRAY_TYPE) {
          entp[field] = JSON.parse(row[field])
        }
        else if (senecatype[field] == DATE_TYPE) {
          entp[field] = new Date(row[field])
        }
      }
    })
  }

  return ent.make$(entp)
}


module.exports.tablename = function (entity) {
  var canon = entity.canon$({object: true})

  return (canon.base ? canon.base + '_' : '') + canon.name
}

