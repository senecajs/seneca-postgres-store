'use strict'

var assert = require('assert')
var seneca = require('seneca')
var Lab = require('lab')
var lab = exports.lab = Lab.script()
var Async = require('async')

var describe = lab.describe
var before = lab.before
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

describe('postgres store API V2.0.0', function () {
  before(function (done) {
    var Product = si.make('product')

    Async.series([
      function clear (next) {
        Product.remove$({ all$: true }, next)
      },
      function create (next) {
        var products = [
          Product.make$({ name: 'apple', price: 100 }),
          Product.make$({ name: 'pear', price: 200 }),
          Product.make$({ name: 'cherry', price: 300 })
        ]

        function saveproduct (product, saved) {
          product.save$(saved)
        }

        Async.forEach(products, saveproduct, next)
      }
    ], function (err) {
      assert(!err)
      done()
    })
  })

  it('use not equal ne$', function (done) {
    var product = si.make('product')

    product.list$({ price: {ne$: 200}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(2, lst.length)
      assert.equal('apple', lst[0].name)
      assert.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use not equal ne$ string', function (done) {
    var product = si.make('product')

    product.list$({ name: {ne$: 'pear'}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(2, lst.length)
      assert.equal('apple', lst[0].name)
      assert.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use eq$', function (done) {
    var product = si.make('product')

    product.list$({ price: {eq$: 200} }, function (err, lst) {
      assert(!err)

      assert.equal(1, lst.length)
      assert.equal('pear', lst[0].name)
      done()
    })
  })

  it('use eq$ string', function (done) {
    var product = si.make('product')

    product.list$({ name: {eq$: 'pear'} }, function (err, lst) {
      assert(!err)

      assert.equal(1, lst.length)
      assert.equal('pear', lst[0].name)
      done()
    })
  })

  it('use gte$', function (done) {
    var product = si.make('product')

    product.list$({ price: {gte$: 200}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(2, lst.length)
      assert.equal('pear', lst[0].name)
      assert.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use gt$', function (done) {
    var product = si.make('product')

    product.list$({ price: {gt$: 200}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(1, lst.length)
      assert.equal('cherry', lst[0].name)
      done()
    })
  })

  it('use lte$', function (done) {
    var product = si.make('product')

    product.list$({ price: {lte$: 200}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(2, lst.length)
      assert.equal('apple', lst[0].name)
      assert.equal('pear', lst[1].name)
      done()
    })
  })

  it('use lt$', function (done) {
    var product = si.make('product')

    product.list$({ price: {lt$: 200}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(1, lst.length)
      assert.equal('apple', lst[0].name)
      done()
    })
  })

  it('use in$', function (done) {
    var product = si.make('product')

    product.list$({ price: {in$: [200, 300]}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(2, lst.length)
      assert.equal('pear', lst[0].name)
      assert.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use in$ string', function (done) {
    var product = si.make('product')

    product.list$({ name: {in$: ['cherry', 'pear']}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(2, lst.length)
      assert.equal('pear', lst[0].name)
      assert.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use in$ one matching', function (done) {
    var product = si.make('product')

    product.list$({ price: {in$: [200, 500, 700]}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(1, lst.length)
      assert.equal('pear', lst[0].name)
      done()
    })
  })

  it('use in$ no matching', function (done) {
    var product = si.make('product')

    product.list$({ price: {in$: [250, 500, 700]}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(0, lst.length)
      done()
    })
  })

  it('use nin$ three matching', function (done) {
    var product = si.make('product')

    product.list$({ price: {nin$: [250, 500, 700]}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(3, lst.length)
      done()
    })
  })

  it('use nin$ one matching', function (done) {
    var product = si.make('product')

    product.list$({ price: {nin$: [200, 500, 300]}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(1, lst.length)
      assert.equal('apple', lst[0].name)
      done()
    })
  })

  it('use complex in$ and nin$', function (done) {
    var product = si.make('product')

    product.list$({ price: {nin$: [250, 500, 300], in$: [200, 300]}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(1, lst.length)
      assert.equal('pear', lst[0].name)
      done()
    })
  })

  it('use nin$ string', function (done) {
    var product = si.make('product')

    product.list$({ name: {nin$: ['cherry', 'pear']}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(1, lst.length)
      assert.equal('apple', lst[0].name)
      done()
    })
  })
})

