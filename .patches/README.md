# Patches

Changes to `.github/workflows/` that could not be pushed from the
session that prepared this branch (pushing workflow files needs the
GitHub `workflow` scope). Apply them with:

```sh
git am .patches/*.patch
```

`0001-ci-postgres-service.patch` rewrites `.github/workflows/build.yml`:

* triggers on `push` and `pull_request` for `master` and `main`
  (the old workflow used `main` only; the default branch is `master`);
* matrix Node 24.x and 22.x;
* a `postgres:18` service container on host port 55432 with a
  `pg_isready` health check, matching `docker-compose.yml`;
* loads `test/support/dbschema.sql` with `psql` (service containers
  start before checkout, so the schema cannot be mounted as an init
  script as `docker-compose.yml` does);
* sets the `SENECA_TEST_PG_*` variables read by `test/support/db/config.js`.
