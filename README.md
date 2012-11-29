seneca-postgres
===============

seneca-postgres is a [PostgreSQL][postgresqlorg] database plugin for the [Seneca][seneca] MVP toolkit. The plugin is using the
[node-postgres][nodepg] driver.

Usage:

    var seneca = require('seneca');
    var store = require('seneca-postgres');

    var config = {}
    var storeopts = {
      name:'dbname',
      host:'127.0.0.1',
      port:5432,
      username:'user',
      password:'password'
    };

    ...

    var si = seneca(config)
    si.use(store, storeopts)
    si.ready(function() {
      var product = si.make('product')
      ...
    })
    ...

[postgresqlorg]: http://www.postgresql.org/
[seneca]: http://senecajs.org/
[nodepg]: https://github.com/brianc/node-postgres