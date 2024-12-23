const { mysql } = require('../clients')

class ClinicalService {

    async formatSchema(rows) {
        const types = {
          'char': 'UTF8',
          'varchar': 'UTF8',
          'enum': 'UTF8',
          'text': 'UTF8',
          'int': 'INT64',
          'bigint': 'INT64',
          'smallint': 'INT64',
          'tinyint': 'INT64',
          'decimal': 'DOUBLE',
          'timestamp': 'TIMESTAMP_MILLIS',
          'datetime': 'TIMESTAMP_MILLIS',
          'date': 'TIMESTAMP_MILLIS',
        }
    
        const optionals = {
          'YES': true,
          'NO': false
        }
    
        const columns = {}
    
        for (const row of rows) {
          columns[row.COLUMN_NAME] = {
            type: types[row.DATA_TYPE],
            optional: optionals[row.IS_NULLABLE]
          }
        }
    
        return columns
    }

    async generateData(table) {
        const dataFull = []

        const total = await mysql.getTotal(table)

        if (Number(total[0].total) > 0) {
            const limit = 1000
            var pages = Math.ceil(Number(total[0].total) / limit)
            var offset = 0
            
            for (let page = 0; page < pages; page++) {
                const data = await mysql.findAll({table: table, limit: limit, offset: offset})
                offset = offset + limit

                if (data) {
                    dataFull.push(data)
                }
            }
        }

        return dataFull
    }

    async generateSchema(table) {
        const columns = await mysql.getInformationSchema(table)

        return await this.formatSchema(columns)
    }
}

module.exports = new ClinicalService()
