# Run the tests locally

Goal: run `npm test` against a real PostgreSQL started by Docker.

1. Use Node.js 24 or later and install dependencies:

   ```sh
   npm install
   ```

2. Start PostgreSQL 18 on host port 55432. The schema in
   `test/support/dbschema.sql` is loaded on first start:

   ```sh
   npm run services:up
   ```

   This runs `docker compose up -d --wait` and returns once the
   container `seneca-postgres-store-postgres` is healthy.

3. Run the tests:

   ```sh
   npm test
   ```

4. Stop the database and delete its data:

   ```sh
   npm run services:down
   ```

## Connection settings

`npm test` does not start Docker. It reads these variables, whose
defaults match `docker-compose.yml`:

| Variable | Default |
| -------- | ------- |
| `SENECA_TEST_PG_HOST` | `127.0.0.1` |
| `SENECA_TEST_PG_PORT` | `55432` |
| `SENECA_TEST_PG_USER` | `senecatest` |
| `SENECA_TEST_PG_PASSWORD` | `senecatest_2086hab80y` |
| `SENECA_TEST_PG_DATABASE` | `senecatest_71v94h` |

To use another server, create the tables from
`test/support/dbschema.sql` there and set the variables:

```sh
SENECA_TEST_PG_HOST=db.local SENECA_TEST_PG_PORT=5432 npm test
```

## Test against an unreleased Seneca build

```sh
npm install --no-save /path/to/seneca-4.x.y.tgz
npm test
npm install   # restore the devDependency
```

## Transaction tests

The transaction tests run only when `seneca-entity` 21.0.x is
installed, the only release whose transaction API the store supports.
`@seneca/entity`, the development dependency, has no transaction API,
so by default they are reported as skipped. To run them:

```sh
npm install --no-save seneca-entity@21.0.2
npm test
npm install   # remove it again
```
