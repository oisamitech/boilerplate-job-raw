require('dotenv').config()
const fromEnv = (env) => process.env[env]

module.exports = fromEnv
