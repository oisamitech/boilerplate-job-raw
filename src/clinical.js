const ClinicalUseCase = require('./usecases/clinical')

const main = async () => {
  try {
    console.log('Started job to get data from Clinical Database.')
    await ClinicalUseCase.execute()
    console.log('Finished job to get data from Clinical Database.')
    process.exit(0)
  } catch (err) {
    console.log(err)
    process.exit(1)
  }
}

main()