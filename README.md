![Seneca](http://senecajs.org/files/assets/seneca-logo.png)
> A [Seneca.js](http://senecajs.org) data storage plugin

# @seneca/postgres-store

## Install

```sh
npm install seneca
npm install @seneca/postgres-store
```

## Quick Example

```js
const Seneca = require('seneca')
var seneca = Seneca().use('postgres-store', { ... })
```

## More Examples

See [test/](test/) for usage examples.

## Motivation

A Postgres data store plugin for the Seneca framework.

## Support

If you are having difficulty, open an issue on the GitHub repo.

## API

See [README](README.md) and Seneca docs for message patterns.

## Contributing
The [Senecajs org][] encourages open participation. If you feel you
can help in any way, be it with documentation, examples, extra
testing, or new features please get in touch.

### Running tests with Docker

Build the PostgreSQL Docker image:

```sh
npm run build

```

Start the PostgreSQL container:
```sh
npm run start
```

Stop the PostgreSQL container:
```sh
npm run stop
```

While the container is running you can run the tests into another terminal:
```sh
npm run test
```

#### Testing for Mac users
Before the tests can be run you must run `docker-machine env default` and copy the docker host address (example: '192.168.99.100').
This address must be inserted into the test/default_config.json file as the value for the host variable. The tests can now be run.

## Background

This plugin uses the [pg](https://node-postgres.com/) driver.


[![npm version][npm-badge]][npm-url]
[![Build Status][travis-badge]][travis-url]
[![Dependency Status][david-badge]][david-url]
[![Coveralls][BadgeCoveralls]][Coveralls]
[![Gitter][gitter-badge]][gitter-url]

| ![Voxgig](https://www.voxgig.com/res/img/vgt01r.png) | This open source module is sponsored and supported by [Voxgig](https://www.voxgig.com). |
|---|---|
