const Assert = require('assert')

const intern = {
  sTypes: {
    escape: '"',
    prepared: '$'
  },

  asyncMethod(f) {
    return function (msg, done) {
      const seneca = this
      const p = f.call(seneca, msg)

      Assert('function' === typeof p.then &&
      'function' === typeof p.catch,
      'The function must be async, i.e. return a promise.')

      return p
        .then(result => done(null, result))
        .catch(done)
    }
  }
}

module.exports = { intern }
