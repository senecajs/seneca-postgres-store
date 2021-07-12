function getConfig() {
  if (process.env.CI) {
    return {
      name: "senecatest_ci_629vv14",
      host: "localhost",
      port: 5432,
      username: "senecatest",
      password: "senecatest_ci_07y71809h1",
      options: {}
    }
  }

  return {
    name: "senecatest_71v94h",
    host: "localhost",
    port: 5432,
    username: "senecatest",
    password: "senecatest_2086hab80y",
    options: {}
  }
}

module.exports = getConfig()
