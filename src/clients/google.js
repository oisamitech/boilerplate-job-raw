const { Storage } = require('@google-cloud/storage')
const { fromEnv } = require('../utils')

class GoogleStorage {
  constructor () {
    this.storage = new Storage({
      credentials: JSON.parse(fromEnv('GOOGLE_CLOUD_STORAGE_KEY'))
    })
    this.maxRetries = 3
    this.retryDelay = 1000
  }

  async get (fileName) {
    try {
        const bucket = await this.storage.bucket(fromEnv('STORAGE_BUCKET_NAME'))
        const file = await this.storage.bucket(fromEnv('STORAGE_BUCKET_NAME')).file(fileName)
        try {
        const [fileExists] = await file.exists()
    } catch(error) {
        console.log(error)
    }
        if (!fileExists) return []
    
        return await this.retryOperation(() => file.download())
    } catch(error) {
        console.log(error)
    }
    
  }

  extractBucketName (gcsUri) {
    const [bucketName] = gcsUri.replace('gs://', '').split('/')

    return bucketName
  }

  extractFileName (gcsUri) {
    const parts = gcsUri.trim().split('/')

    return parts[parts.length - 1] || ''
  }

  async move (
    gcsUri,
    destinationFileName
  ) {
    try {
      const sourceBucketName = this.extractBucketName(gcsUri)
      const sourceFileName = this.extractFileName(gcsUri)

      const destinationBucketName = fromEnv('STORAGE_BUCKET_NAME')

      const sourceBucket = this.storage.bucket(sourceBucketName)
      const destinationBucket = this.storage.bucket(destinationBucketName)

      const file = sourceBucket.file(sourceFileName)

      // Copia o arquivo para o bucket de destino
      await file.copy(destinationBucket.file(destinationFileName))
      console.log(`Arquivo copiado para gs://${destinationBucketName}/${destinationFileName}`)

      // Exclui o arquivo original
      await file.delete()
      console.log(`Arquivo original gs://${sourceBucketName}/${sourceFileName} excluído.`)
    } catch (error) {
      throw new Error(`Erro ao mover arquivo ${error}`)
    }
  }

  async create (fileName, payload, options = {}) {
    const file = this.storage.bucket(fromEnv('STORAGE_BUCKET_NAME')).file(fileName)

    return new Promise((resolve, reject) => {
      const writeStream = file.createWriteStream({
        resumable: true,
        validation: true,
        metadata: {
          contentType: 'application/json',
          ...options.metadata
        }
      })

      // Limpeza do stream para evitar múltiplos listeners
      const cleanup = () => writeStream.removeAllListeners()

      // Quando o upload for concluído
      writeStream.on('finish', () => {
        console.log(`Upload do arquivo ${fileName} concluído com sucesso`)
        cleanup()
        resolve()
      })

      // Tratamento de erros
      writeStream.on('error', (error) => {
        console.error(`Erro no upload do arquivo ${fileName}:`, error)
        cleanup()
        reject(error)
      })

      // Configuração de timeout (se especificado)
      if (options.timeout) {
        setTimeout(() => {
          cleanup()
          reject(new Error(`Upload timeout após ${options.timeout}ms`))
        }, options.timeout)
      }

      // Escreve o payload no arquivo
      try {
        writeStream.write(JSON.stringify(payload))
        writeStream.end()
      } catch (error) {
        console.error(`Erro ao escrever o payload para o arquivo ${fileName}:`, error)
        cleanup()
        reject(error)
      }
    })
  }

  async delete (fileName) {
    //const file = await this.storage.bucket(fromEnv('STORAGE_BUCKET_NAME')).file(fileName)
    const file = await this.storage.bucket('interoperabilidade-development').file(fileName)
    const [fileExists] = await file.exists()

    if (fileExists) {
      await this.retryOperation(() => file.delete())
    }
  }

  async retryOperation (operation) {
    let lastError

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        return await operation()
      } catch (error) {
        lastError = error
        console.error(`Tentativa ${attempt} falhou:`, error.message)

        if (attempt < this.maxRetries) {
          const delay = this.retryDelay * Math.pow(2, attempt - 1)
          console.log(`Aguardando ${delay}ms antes da próxima tentativa...`)
          await new Promise(resolve => setTimeout(resolve, delay))
        }
      }
    }

    throw new Error(`Operação falhou após ${this.maxRetries} tentativas. Último erro: ${lastError.message}`)
  }
}

module.exports = new GoogleStorage()
