const { Readable, PassThrough } = require('stream')
const parquet = require('parquetjs')
const { ParquetTransformer } = require('parquetjs')
const { clinical } = require('../consts')
const ClinicalService = require('../services/clinical')
const GoogleStorage = require('../clients/google')
const { fromEnv } = require('../utils')

class ClinicalUseCase {

    async saveFile (data, table) {
        const optionsStorage = {
            maxRetries: 5,
            retryDelay: 2000,
            timeout: 30000,
            metadata: { cacheControl: '3600' },
            onRetry: (attempt, error) => {
              console.log(`Tentativa ${attempt} falhou: ${error.message}`)
            }
        }

        const filePath = `${fromEnv('STORAGE_BUCKET_APP_FILE')}/${table}.parquet`
        
        await GoogleStorage.create(filePath, data, optionsStorage)
    }

    async execute () {
        try {
            for (const table of clinical) {
                const dataArray = await ClinicalService.generateData(table.table)
                
                if (dataArray) {
                    const destination = new PassThrough()
                    const reader = Readable.from(dataArray)
                    const schema = await ClinicalService.generateSchema(table.table)
                    const parquetSchema = new parquet.ParquetSchema(schema)

                    reader
                        .pipe(new ParquetTransformer(parquetSchema))
                        .pipe(destination)
                    
                    await this.saveFile(destination, table.table)
                }
            }
        } catch (err) {
           console.log(err)
        }
    }

}

module.exports = new ClinicalUseCase()