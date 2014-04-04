/*jslint node: true*/
/*jslint asi: true */
/*global describe:true, it:true*/
"use strict";

var assert = require('assert')
var seneca = require('seneca')
var async = require('async')

var shared = seneca.test.store.shared


var config = {
  log:'print'
};
var si = seneca();
si.use(require('..'), {
  name: 'senecatest',
  host: '127.0.0.1',
  port: 5432,
  username: 'senecatest',
  password: 'senecatest',
  options: { }
})

si.__testcount = 0
var testcount = 0


describe('postgres', function () {
  it('basic', function (done) {
    testcount++
    shared.basictest(si, done)
  })

  it('save with passing an id$', function(done) {

    var product = si.make('foo')

    product.p1 = 'pear'

    si.act(
      { role:'entity', cmd:'save', ent: product, id$:'12345'},
      function( err, product ) {
        console.log(arguments)
        assert(!err)
        assert.equal(product.id, '12345')
        done()
      })
  })

  it('close', function (done) {
    shared.closetest(si, testcount, done)
  })
})

