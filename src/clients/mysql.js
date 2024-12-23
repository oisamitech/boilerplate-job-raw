const mysql = require('mysql')
const { fromEnv } = require('../utils')

class MySQL {
  constructor () {
    this.pool = mysql.createPool({
      host: fromEnv('MYSQL_HOST'),
      port: fromEnv('MYSQL_PORT'),
      user: fromEnv('MYSQL_USER'),
      password: fromEnv('MYSQL_PASSWORD'),
      database: fromEnv('MYSQL_DATABASE'),
      connectionLimit: fromEnv('MYSQL_POOL_CONNECTION_LIMIT'),
      multipleStatements: true,
      supportBigNumbers: true,
      bigNumberStrings: true
    })
  }

  async pooledConnection (action) {
    const connection = await new Promise((resolve, reject) =>
      this.pool.getConnection((error, conn) => error ? reject(error) : resolve(conn)))

    try {
      return await action(connection)
    } finally {
      connection.release()
    }
  }

  health () {
    return this.query('SELECT NOW()')
      .then(data => data)
      .catch(err => { throw new Error(err) })
  }

  queryStream (query, values = []) {
    return this.pool.query(query, values).stream()
  }

  async query (query, options = {}, values = []) {
    return this.pooledConnection(connection => new Promise((resolve, reject) =>
      connection.query({ sql: query, ...options }, values, (error, rows) => error ? reject(error) : resolve(rows))))
  }

  async close () {
    await this.pool.end()
  }

  getInformationSchema (table) {
    return this.query(`SELECT COLUMN_NAME ,DATA_TYPE ,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'clinical' AND TABLE_NAME = '${table}';`)
  }

  getTotal (table) {
    return this.query(`SELECT COUNT(*) as total FROM ${table};`)
  }

  findAll (query = {}) {
    return this.query(`SELECT * FROM ${query.table} LIMIT ${query.limit} OFFSET ${query.offset};`)
  }
  
}

module.exports = new MySQL()
