const mongodb = require('mongodb')
const fs = require('fs')
const dotenv = require('dotenv')
dotenv.config()

async function coronaDataFramesRequest(props) {
    const { client, dataFramesFrom, dataFramesTo } = props

    return await client.db().collection("dataFrames").aggregate([
        { $match: { pageUpdatedUTC: { $gt: dataFramesFrom, $lt: dataFramesTo }}},
        { $sort: { pageUpdatedUTC: 1}}
    ]).toArray()
}

function coronaProcessing(dataFrames, mode) {

    let n = n => n > 9 ? "" + n : "0" + n

    let previousPageUpdate
    let infected = {}
    let recovered = {}
    let deaths = {}
    let deathMinAge = {}
    let deathMaxAge = {}
    let deathsAverageAge = {}
    let homeQuarantine = {}
    let sampling = {}
    let worldInfected = {}
    let worldRecovered = {}
    let worldDied = {}

    function addNewFrame(dataFrame) {

        let actualFrameDate = dataFrame.pageUpdatedUTC
        let days = ["va.", "hé.", "ke.", "sze.", "csü.", "pé.", "szo."]
        let frameDateStyled = `${actualFrameDate.getFullYear()}-${n(actualFrameDate.getMonth()+1)}-${n(actualFrameDate.getDate())} ${days[actualFrameDate.getDay()]}`
        
        infected[frameDateStyled] = dataFrame.infected
        recovered[frameDateStyled] = dataFrame.recovered
        deaths[frameDateStyled] = dataFrame.deaths
        deathMinAge[frameDateStyled] = dataFrame.deathMinAge
        deathMaxAge[frameDateStyled] = dataFrame.deathMaxAge
        deathsAverageAge[frameDateStyled] = dataFrame.deathsAverageAge
        homeQuarantine[frameDateStyled] = dataFrame.homeQuarantine
        sampling[frameDateStyled] = dataFrame.sampling
        worldInfected[frameDateStyled] = dataFrame.worldInfected
        worldRecovered[frameDateStyled] = dataFrame.worldRecovered
        worldDied[frameDateStyled] = dataFrame.worldDied
    }

    dataFrames.forEach( dataFrame => {

        if(previousPageUpdate) {
            if(previousPageUpdate < dataFrame.pageUpdatedUTC) {
                previousPageUpdate = dataFrame.pageUpdatedUTC
                addNewFrame(dataFrame)
            }
        } else {
            previousPageUpdate = dataFrame.pageUpdatedUTC
            addNewFrame(dataFrame)
        }
          
    })

    if(mode === "stat") {
        var framesCalculated = [{
            IndicatorName: "Fertőzött",
            ...infected
        },
        {
            IndicatorName: "Gyógyult",
            ...recovered
        },
        {
            IndicatorName: "Elhunytak",
            ...deaths
        },
        {
            IndicatorName: "Hatósági házi karanténban",
            ...homeQuarantine
        },
        {
            IndicatorName: "Mintavétel",
            ...sampling
        }]
    } else if(mode === "dStat") {
        var framesCalculated = [{
            IndicatorName: "Legidosebb Elhunyt",
            ...deathMaxAge
        },
        {
            IndicatorName: "Legfiatalabb Elhunyt",
            ...deathMinAge
        },
        {
            IndicatorName: "Elhunytak Atlageletkora",
            ...deathsAverageAge
        }]
    } else if(mode === "wStat") {
        var framesCalculated = [{
            IndicatorName: "Fertozottek a világon",
            ...worldInfected
        },
        {
            IndicatorName: "Gyógyultak a világon",
            ...worldRecovered
        },
        {
            IndicatorName: "Elhunytak a világon",
            ...worldDied
        }]
    }

    return framesCalculated
}


async function coronaDataFramesProcessing(props) {
    const { client, dataFramesFrom, dataFramesTo, mode } = props

    const dataFramesFromMongo = await coronaDataFramesRequest({ client, dataFramesFrom, dataFramesTo })

    return coronaProcessing(dataFramesFromMongo, mode)
}

mongodb.connect(
    process.env.CONNECTIONSTRING, 
    {useNewUrlParser: true, useUnifiedTopology: true}, 
    function(err, client) {

        coronaDataFramesProcessing({
            client,
            dataFramesFrom: new Date("2020-03-04T00:00:00.000+0100"),
            dataFramesTo: new Date("2020-04-29T00:00:30.000+0100"),
            mode: "wStat"
          }).then( res => {
            let fileName = "coronaDataFramesProcessed.json"
            fs.writeFile("framesProcessed/" + fileName, JSON.stringify(res), err => {
              if(err) throw err
              console.log(fileName + ", Saved!")
            })
          })
        
    }
)