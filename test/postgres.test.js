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

  it('close', function (done) {
    shared.closetest(si, testcount, done)
  })
})

