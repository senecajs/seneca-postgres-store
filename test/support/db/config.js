// Connection settings for the test database. The defaults match
// docker-compose.yml (npm run services:up) and the CI service container.
const env = process.env

module.exports = {
  name: env.SENECA_TEST_PG_DATABASE || 'senecatest_71v94h',
  host: env.SENECA_TEST_PG_HOST || '127.0.0.1',
  port: parseInt(env.SENECA_TEST_PG_PORT || '55432', 10),
  username: env.SENECA_TEST_PG_USER || 'senecatest',
  password: env.SENECA_TEST_PG_PASSWORD || 'senecatest_2086hab80y',
  options: {},
}
