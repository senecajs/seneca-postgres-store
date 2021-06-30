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
  },

  compact(obj) {
    return Object.keys(obj)
      .map(k => [k, obj[k]])
      .filter(([, v]) => undefined !== v)
      .reduce((acc, [k, v]) => {
        acc[k] = v
        return acc
      }, {})
  }
}

module.exports = { intern }
