'use strict'

var assert = require('assert')
var seneca = require('seneca')
var Lab = require('lab')
var lab = exports.lab = Lab.script()

var describe = lab.describe
var it = lab.it

var shared = require('seneca-store-test')
var default_config = require('./default_config.json')

var si = seneca()
si.use(require('..'), default_config)

describe('Basic Test', function () {
  shared.basictest({
    seneca: si,
    script: lab
  })

  shared.sorttest({
    seneca: si,
    script: lab
  })

  shared.limitstest({
    seneca: si,
    script: lab
  })

  shared.sqltest({
    seneca: si,
    script: lab
  })
})

describe('postgres', function () {
  it('save with passing an id$', function (done) {
    var product = si.make('foo')

    product.id$ = '12345'
    product.p1 = 'pear'

    si.act({role: 'entity', cmd: 'save', ent: product},
      function (err, product) {
        assert(!err)
        assert.equal(product.id, '12345')
        done()
      })
  })
})

