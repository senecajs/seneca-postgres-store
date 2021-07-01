const Assert = require('assert')

const Q = {
  insertstm(args) {
    const {
      into,
      values,
      escapeIdentifier,
      on_conflict = null
    } = args

    const col_names = Object.keys(values)
    const col_vals = Object.values(values)


    let bindings = []
    let sql = ''
    let param_no = 1


    sql += 'insert into ' + escapeIdentifier(into)


    const safe_cols = col_names.map(col_name => escapeIdentifier(col_name))
    sql += ' (' + safe_cols.join(', ') + ')'


    const val_placeholders = col_vals.map(_ => Q.valuePlaceholder(param_no++))
    sql += ' values (' + val_placeholders.join(', ') + ')'
    bindings = bindings.concat(col_vals)


    if (null != on_conflict) {
      const {
        columns: confl_cols,
        do_update: { set }
      } = on_conflict


      const safe_confl_cols =
        confl_cols.map(col_name => escapeIdentifier(col_name))

      sql += ' on conflict (' + safe_confl_cols + ')'

      const set_q = setstm({
        set,
        escapeIdentifier,
        first_param_no: param_no
      })

      sql += ' do update set ' + set_q.text
      bindings = bindings.concat(set_q.values)
      param_no = set_q.next_param_no
    }


    sql += ' returning *'


    return { text: sql, values: bindings }
  },


  selectstm(args) {
    const {
      escapeIdentifier,
      from,
      columns = '*',
      where = null,
      offset = null,
      limit = null,
      order_by = null
    } = args


    let bindings = []
    let sql = ''
    let param_no = 1


    sql += 'select '


    if ('*' === columns) {
      sql += '*'
    } else {
      const safe_columns = columns.map(col_name => escapeIdentifier(col_name))
      sql += safe_columns.join(', ')
    }


    sql += ' from ' + escapeIdentifier(from)


    if (null != where) {
      const where_q = wherestm({
        where,
        escapeIdentifier,
        first_param_no: param_no
      })

      sql += ' where ' + where_q.text
      bindings = bindings.concat(where_q.values)
      param_no = where_q.next_param_no
    }


    if (null != order_by) {
      const order_q = orderbystm({
        order_by,
        escapeIdentifier,
        first_param_no: param_no
      })

      sql += ' order by ' + order_q.text
      bindings = bindings.concat(order_q.values)
      param_no = order_q.next_param_no
    }


    if (null != limit) {
      sql += ' limit ' + Q.valuePlaceholder(param_no++)
      bindings.push(limit)
    }


    if (null != offset) {
      sql += ' offset ' + Q.valuePlaceholder(param_no++)
      bindings.push(offset)
    }


    return { text: sql, values: bindings }
  },


  deletestm(args) {
    const {
      escapeIdentifier,
      from,
      where = null,
      limit = null
    } = args

    let sql = ''
    let bindings = []
    let param_no = 1

    sql += 'delete from ' + escapeIdentifier(from)

    if (null != where) {
      const where_q = wherestm({
        where,
        escapeIdentifier,
        first_param_no: param_no
      })

      sql += ' where ' + where_q.text
      bindings = bindings.concat(where_q.values)
      param_no = bindings.next_param_no
    }

    if (null != limit) {
      sql += ' limit ' + Q.valuePlaceholder(param_no++)
      bindings.push(limit)
    }

    sql += ' returning *'

    return { text: sql, values: bindings }
  },


  updatestm(args) {
    const {
      table,
      set,
      where,
      escapeIdentifier
    } = args


    let bindings = []
    let sql = ''
    let param_no = 1


    sql += 'update ' + escapeIdentifier(table) + ' set '

    const set_q = setstm({ set, escapeIdentifier, first_param_no: param_no })
    sql += set_q.text
    bindings = bindings.concat(set_q.values)
    param_no = set_q.next_param_no


    const where_q = wherestm({
      where,
      first_param_no: param_no,
      escapeIdentifier
    })

    sql += ' where ' + where_q.text
    bindings = bindings.concat(where_q.values)
    param_no = where_q.next_param_no


    sql += ' returning *'


    return { text: sql, values: bindings }
  },


  valuePlaceholder(param_no) {
    Assert.strictEqual(typeof param_no, 'number', 'param_no')
    return '$' + param_no
  },


  isOp(k) {
    const OPERATORS = [
      'ne$', 'gte$', 'gt$', 'lte$', 'lt$', 'eq$', 'in$', 'nin$', 'or$', 'and$'
    ]

    return OPERATORS.includes(k)
  }
}


function isObject(x) {
  return null != x && '[object Object]' === toString.call(x)
}


function normalizeOpsInQuery(q) {
  /*
   * NOTE: A query may include operators, which may take the following forms
   * inside a query, that may look like so:
   *
   * ```
   * const q = {
   *   in$: { color: ['orange', 'grey'] },
   *   price: { gt$: '9.95' }
   * }
   * ```
   *
   * The purpose of this function is to leave non-operators as is, and
   * put the operators in the form: [operator]: { [col0]: vals0, ..., [colN]: valsN }
   * This is the format the Qbuilder expects as it should facilitate conversion of
   * a query into a SQL expression.
   */
   
  const out = []
  
  for (const k in q) {
    if (isObject(q[k])) {
      for (const kk in q[k]) {
        if (Q.isOp(kk)) {
          out[kk] = { [k]: q[k][kk] }
        } else {
          out[k] = out[k] || {}
          out[k][kk] = q[k][kk]
        }
      }
    } else {
      out[k] = q[k]
    }
  }
  
  return out
}


function inexprstm(args) {
  const {
    column,
    in: ary,
    escapeIdentifier,
    negate = false,
    first_param_no = 1
  } = args
  
  Assert(Array.isArray(ary), 'ary must be an array')

  let sql = ''
  let bindings = []
  let param_no = first_param_no
  
  const val_placeholders = ary
    .map(_ => Q.valuePlaceholder(param_no++))
    .join(', ')

  if (0 === val_placeholders.length) {
    sql += 'false'
  } else {
    sql += escapeIdentifier(column)
    
    if (negate) {
      sql += ' not'
    }
    
    sql += ' in (' + val_placeholders + ')'
    bindings = bindings.concat(ary)
  }
  
  return { text: sql, values: bindings, next_param_no: param_no }
}


function eqexprstm(args) {
  const {
    column,
    value,
    escapeIdentifier,
    negate = false,
    first_param_no = 1
  } = args

  let sql = ''
  let bindings = []
  let param_no = first_param_no


  if (null == value) {
    sql += escapeIdentifier(column) + ' is' +
      (negate ? ' not' : '') + ' null'
  } else {
    sql += escapeIdentifier(column) +
      (negate ? ' != ' : ' = ') + Q.valuePlaceholder(param_no++)

    bindings.push(value)
  }

  return { text: sql, values: bindings, next_param_no: param_no }
}


function ltexprstm(args) {
  const {
    column,
    value,
    escapeIdentifier,
    first_param_no = 1
  } = args

  let sql = ''
  let bindings = []
  let param_no = first_param_no


  sql += escapeIdentifier(column) + ' < ' +
    Q.valuePlaceholder(param_no++)

  bindings.push(value)


  return { text: sql, values: bindings, next_param_no: param_no }
}


function lteexprstm(args) {
  const {
    column,
    value,
    escapeIdentifier,
    first_param_no = 1
  } = args

  let sql = ''
  let bindings = []
  let param_no = first_param_no


  sql += escapeIdentifier(column) + ' <= ' +
    Q.valuePlaceholder(param_no++)

  bindings.push(value)


  return { text: sql, values: bindings, next_param_no: param_no }
}


function gtexprstm(args) {
  const {
    column,
    value,
    escapeIdentifier,
    first_param_no = 1
  } = args

  let sql = ''
  let bindings = []
  let param_no = first_param_no


  sql += escapeIdentifier(column) + ' > ' +
    Q.valuePlaceholder(param_no++)

  bindings.push(value)


  return { text: sql, values: bindings, next_param_no: param_no }
}


function gteexprstm(args) {
  const {
    column,
    value,
    escapeIdentifier,
    first_param_no = 1
  } = args

  let sql = ''
  let bindings = []
  let param_no = first_param_no


  sql += escapeIdentifier(column) + ' >= ' +
    Q.valuePlaceholder(param_no++)

  bindings.push(value)


  return { text: sql, values: bindings, next_param_no: param_no }
}


function wherestm(args) {
  const {
    escapeIdentifier,
    first_param_no = 1
  } = args
  
  const where = normalizeOpsInQuery(args.where)


  let sql = ''
  let bindings = []
  let param_no = first_param_no
  

  const update_all = 0 === Object.keys(where).length

  if (update_all) {
    sql += 'true'
  } else {
    const where_cols = Object.keys(where)


    const qs = where_cols.reduce((acc, where_col) => {
      const where_val = where[where_col]
      
      
      if ('in$' === where_col) {
        const in_qs = Object.keys(where_val).map(column => {
          const in_q = inexprstm({
            column,
            in: where_val[column],
            escapeIdentifier,
            first_param_no: param_no
          })
          
          param_no = in_q.next_param_no
          
          return in_q
        })

        return acc.concat(in_qs)
      }


      if ('nin$' === where_col) {
        const nin_qs = Object.keys(where_val).map(column => {
          const nin_q = inexprstm({
            column,
            in: where_val[column],
            negate: true,
            escapeIdentifier,
            first_param_no: param_no
          })
          
          param_no = nin_q.next_param_no
          
          return nin_q
        })

        return acc.concat(nin_qs)
      }


      if ('eq$' === where_col) {
        const eq_qs = Object.keys(where_val).map(column => {
          const eq_q = eqexprstm({
            column,
            value: where_val[column],
            escapeIdentifier,
            first_param_no: param_no
          })

          param_no = eq_q.next_param_no

          return eq_q
        })
        
        return acc.concat(eq_qs)
      }


      if ('ne$' === where_col) {
        const neq_qs = Object.keys(where_val).map(column => {
          const neq_q = eqexprstm({
            column,
            value: where_val[column],
            negate: true,
            escapeIdentifier,
            first_param_no: param_no
          })

          param_no = neq_q.next_param_no

          return neq_q
        })
        
        return acc.concat(neq_qs)
      }


      if ('lt$' === where_col) {
        const lt_qs = Object.keys(where_val).map(column => {
          const lt_q = ltexprstm({
            column,
            value: where_val[column],
            escapeIdentifier,
            first_param_no: param_no
          })

          param_no = lt_q.next_param_no

          return lt_q
        })

        return acc.concat(lt_qs)
      }


      if ('lte$' === where_col) {
        const lte_qs = Object.keys(where_val).map(column => {
          const lte_q = lteexprstm({
            column,
            value: where_val[column],
            escapeIdentifier,
            first_param_no: param_no
          })

          param_no = lte_q.next_param_no

          return lte_q
        })

        return acc.concat(lte_qs)
      }


      if ('gt$' === where_col) {
        const gt_qs = Object.keys(where_val).map(column => {
          const gt_q = gtexprstm({
            column,
            value: where_val[column],
            escapeIdentifier,
            first_param_no: param_no
          })

          param_no = gt_q.next_param_no

          return gt_q
        })

        return acc.concat(gt_qs)
      }


      if ('gte$' === where_col) {
        const gte_qs = Object.keys(where_val).map(column => {
          const gte_q = gteexprstm({
            column,
            value: where_val[column],
            escapeIdentifier,
            first_param_no: param_no
          })

          param_no = gte_q.next_param_no

          return gte_q
        })

        return acc.concat(gte_qs)
      }
      
      
      if ('or$' === where_col) {
        const or_qs = where_val.map((or_where) => {
          const or_q = wherestm({
            where: or_where,
            first_param_no: param_no,
            escapeIdentifier
          })
          
          param_no = or_q.next_param_no
          
          return or_q
        })
        
        const or_q = or_qs.reduce((acc, or_q) => {
          return {
            text: acc.text + ' or ' + or_q.text,
            values: acc.values.concat(or_q.values)
          }
        })
        
        return acc.concat({ ...or_q, text: '(' + or_q.text + ')' })
      }


      if ('and$' === where_col) {
        const and_qs = where_val.map((and_where) => {
          const and_q = wherestm({
            where: and_where,
            first_param_no: param_no,
            escapeIdentifier
          })
          
          param_no = and_q.next_param_no
          
          return and_q
        })

        const and_q = and_qs.reduce((acc, and_q) => {
          return {
            text: acc.text + ' and ' + and_q.text,
            values: acc.values.concat(and_q.values)
          }
        })

        return acc.concat({ ...and_q, text: '(' + and_q.text + ')' })
      }


      if (Array.isArray(where_val)) {
        const in_q = inexprstm({
          column: where_col,
          in: where_val,
          escapeIdentifier,
          first_param_no: param_no
        })

        param_no = in_q.next_param_no
        
        return acc.concat(in_q)
      }


      const eq_q = eqexprstm({
        column: where_col,
        value: where_val,
        escapeIdentifier,
        first_param_no: param_no
      })

      param_no = eq_q.next_param_no
      
      
      return acc.concat(eq_q)
    }, [])
    
    
    const out = qs.reduce((acc, q) => {
      return {
        text: acc.text + ' and ' + q.text,
        values: acc.values.concat(q.values)
      }
    })
    
    sql += out.text
    bindings = out.values
  }


  return { text: sql, values: bindings, next_param_no: param_no }
}


function orderbystm(args) {
  const {
    order_by,
    escapeIdentifier,
    first_param_no = 1
  } = args


  let sql = ''
  let bindings = []
  let param_no = first_param_no


  let first_pair = true

  for (const order_col in order_by) {
    if (!first_pair) {
      sql += ', '
    }

    first_pair = false


    const order_val = order_by[order_col]
    const order = 0 <= order_val ? 'asc' : 'desc'

    sql += escapeIdentifier(order_col) + ' ' + order
  }


  return { text: sql, values: bindings, next_param_no: param_no }
}

function setstm(args) {
  const {
    set,
    escapeIdentifier,
    first_param_no = 1
  } = args


  let sql = ''
  let bindings = []
  let param_no = first_param_no

  let first_set = true

  for (const set_col in set) {
    const set_val = set[set_col]

    if (!first_set) {
      sql += ','
    }


    sql += ' ' + escapeIdentifier(set_col) + ' = ' +
      Q.valuePlaceholder(param_no++)

    bindings.push(set_val)


    first_set = false
  }

  return { text: sql, values: bindings, next_param_no: param_no }
}



module.exports = Q
