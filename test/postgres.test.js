'use strict'

var Asserrt = require('assert')
var Seneca = require('seneca')
var Lab = require('lab')
var lab = exports.lab = Lab.script()
var Async = require('async')

var describe = lab.describe
var before = lab.before
var it = lab.it

var Shared = require('seneca-store-test')
var DefaultConfig = require('./default_config.json')

var si = Seneca()
si.use(require('..'), DefaultConfig)
si.use(require('seneca-store-query'))

describe('Basic Test', function () {
  Shared.basictest({
    seneca: si,
    script: lab
  })

  Shared.sorttest({
    seneca: si,
    script: lab
  })

  Shared.limitstest({
    seneca: si,
    script: lab
  })

  Shared.sqltest({
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
          Asserrt(!err)
          Asserrt.equal(product.id, '12345')
          done()
        })
  })
})

describe('postgres store API V2.0.0', function () {
  before(function (done) {
    var Product = si.make('product')

    Async.series([
      function clear (next) {
        Product.remove$({all$: true}, next)
      },
      function create (next) {
        var products = [
          Product.make$({name: 'apple', price: 100}),
          Product.make$({name: 'pear', price: 200}),
          Product.make$({name: 'cherry', price: 300})
        ]

        function saveproduct (product, saved) {
          product.save$(saved)
        }

        Async.forEach(products, saveproduct, next)
      }
    ], function (err) {
      Asserrt(!err)
      done()
    })
  })

  it('use not equal ne$', function (done) {
    var product = si.make('product')

    product.list$({price: {ne$: 200}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(2, lst.length)
      Asserrt.equal('apple', lst[0].name)
      Asserrt.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use not equal ne$ string', function (done) {
    var product = si.make('product')

    product.list$({name: {ne$: 'pear'}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(2, lst.length)
      Asserrt.equal('apple', lst[0].name)
      Asserrt.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use eq$', function (done) {
    var product = si.make('product')

    product.list$({price: {eq$: 200}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('pear', lst[0].name)
      done()
    })
  })

  it('use eq$ string', function (done) {
    var product = si.make('product')

    product.list$({name: {eq$: 'pear'}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('pear', lst[0].name)
      done()
    })
  })

  it('use gte$', function (done) {
    var product = si.make('product')

    product.list$({price: {gte$: 200}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(2, lst.length)
      Asserrt.equal('pear', lst[0].name)
      Asserrt.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use gt$', function (done) {
    var product = si.make('product')

    product.list$({price: {gt$: 200}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('cherry', lst[0].name)
      done()
    })
  })

  it('use lte$', function (done) {
    var product = si.make('product')

    product.list$({price: {lte$: 200}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(2, lst.length)
      Asserrt.equal('apple', lst[0].name)
      Asserrt.equal('pear', lst[1].name)
      done()
    })
  })

  it('use lt$', function (done) {
    var product = si.make('product')

    product.list$({price: {lt$: 200}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('apple', lst[0].name)
      done()
    })
  })

  it('use in$', function (done) {
    var product = si.make('product')

    product.list$({price: {in$: [200, 300]}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(2, lst.length)
      Asserrt.equal('pear', lst[0].name)
      Asserrt.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use in$ string', function (done) {
    var product = si.make('product')

    product.list$({name: {in$: ['cherry', 'pear']}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(2, lst.length)
      Asserrt.equal('pear', lst[0].name)
      Asserrt.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use in$ one matching', function (done) {
    var product = si.make('product')

    product.list$({price: {in$: [200, 500, 700]}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('pear', lst[0].name)
      done()
    })
  })

  it('use in$ no matching', function (done) {
    var product = si.make('product')

    product.list$({price: {in$: [250, 500, 700]}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(0, lst.length)
      done()
    })
  })

  it('use nin$ three matching', function (done) {
    var product = si.make('product')

    product.list$({price: {nin$: [250, 500, 700]}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(3, lst.length)
      done()
    })
  })

  it('use nin$ one matching', function (done) {
    var product = si.make('product')

    product.list$({price: {nin$: [200, 500, 300]}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('apple', lst[0].name)
      done()
    })
  })

  it('use complex in$ and nin$', function (done) {
    var product = si.make('product')

    product.list$({price: {nin$: [250, 500, 300], in$: [200, 300]}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('pear', lst[0].name)
      done()
    })
  })

  it('use nin$ string', function (done) {
    var product = si.make('product')

    product.list$({name: {nin$: ['cherry', 'pear']}, sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('apple', lst[0].name)
      done()
    })
  })

  it('use or$', function (done) {
    var product = si.make('product')

    product.list$({or$: [{name: 'cherry'}, {price: 200}], sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(2, lst.length)
      Asserrt.equal('pear', lst[0].name)
      Asserrt.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use and$', function (done) {
    var product = si.make('product')

    product.list$({and$: [{name: 'cherry'}, {price: 300}], sort$: {price: 1}}, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('cherry', lst[0].name)
      done()
    })
  })

  it('use and$ & or$', function (done) {
    var product = si.make('product')

    product.list$({
      or$: [{price: {gte$: 200}}, {and$: [{name: 'cherry'}, {price: 300}]}],
      sort$: {price: 1}
    }, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(2, lst.length)
      Asserrt.equal('pear', lst[0].name)
      Asserrt.equal('cherry', lst[1].name)
      done()
    })
  })

  it('use and$ & or$ and limit$', function (done) {
    var product = si.make('product')

    product.list$({
      or$: [{price: {gte$: 200}}, {and$: [{name: 'cherry'}, {price: 300}]}],
      sort$: {price: 1},
      limit$: 1,
      fields$: ['name']
    }, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('pear', lst[0].name)
      Asserrt(!lst[0].price)
      done()
    })
  })

  it('use and$ & or$ and limit$, fields$ and skip$', function (done) {
    var product = si.make('product')

    product.list$({
      price: {gte$: 200},
      sort$: {price: 1},
      limit$: 1,
      fields$: ['name'],
      skip$: 1
    }, function (err, lst) {
      Asserrt(!err)

      Asserrt.equal(1, lst.length)
      Asserrt.equal('cherry', lst[0].name)
      Asserrt(!lst[0].price)
      done()
    })
  })
})
