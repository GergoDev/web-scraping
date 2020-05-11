const mongodb = require('mongodb')
const fs = require('fs')
const dotenv = require('dotenv')
dotenv.config()

async function coronaDataFramesRequest(props) {
    const { client, dataFramesFrom, dataFramesTo } = props

    return await client.db().collection("dataFrames").aggregate([
        { $match: { pageUpdatedUTC: { $gt: dataFramesFrom, $lt: dataFramesTo } } },
        { $sort: { pageUpdatedUTC: 1 } }
    ]).toArray()
}

function coronaProcessing(dataFrames, deathList, mode) {

    let n = n => n > 9 ? "" + n : "0" + n

    let previousPageUpdate
    let infected = {}
    let activeInfected = {}
    let recovered = {}
    let deaths = {}
    let deathMinAge = {}
    let deathMaxAge = {}
    let deathsAverageAge = {}
    let deathMale = {}
    let deathFemale = {}
    let homeQuarantine = {}
    let sampling = {}
    let worldInfected = {}
    let worldRecovered = {}
    let worldDied = {}

    function addNewFrame(dataFrame) {

        let actualFrameDate = dataFrame.pageUpdatedUTC
        let days = ["vasárnap", "hétfő", "kedd", "szerda", "csütörtök", "péntek", "szombat"]
        let frameDateStyled = `${actualFrameDate.getFullYear()}-${n(actualFrameDate.getMonth() + 1)}-${n(actualFrameDate.getDate())} ${days[actualFrameDate.getDay()]}`

        let male = 0
        let female = 0
        if(dataFrame.deaths) {
            deathList.slice(0, dataFrame.deaths).forEach( people => {
                if(people.sex === "Férfi")
                    male++
                else
                    female++
            })
        }

        infected[frameDateStyled] = dataFrame.infected
        activeInfected[frameDateStyled] = dataFrame.infected - dataFrame.recovered - dataFrame.deaths
        recovered[frameDateStyled] = dataFrame.recovered ? dataFrame.recovered : 0
        deaths[frameDateStyled] = dataFrame.deaths ? dataFrame.deaths : 0
        deathMinAge[frameDateStyled] = dataFrame.deathMinAge ? dataFrame.deathMinAge : 0
        deathMaxAge[frameDateStyled] = dataFrame.deathMaxAge ? dataFrame.deathMaxAge : 0
        deathsAverageAge[frameDateStyled] = dataFrame.deathsAverageAge ? dataFrame.deathsAverageAge : 0
        deathMale[frameDateStyled] = male
        deathFemale[frameDateStyled] = female
        homeQuarantine[frameDateStyled] = dataFrame.homeQuarantine ? dataFrame.homeQuarantine : 0
        sampling[frameDateStyled] = dataFrame.sampling ? dataFrame.sampling : 0
        worldInfected[frameDateStyled] = dataFrame.worldInfected
        worldRecovered[frameDateStyled] = dataFrame.worldRecovered
        worldDied[frameDateStyled] = dataFrame.worldDied
    }

    dataFrames.forEach(dataFrame => {

        if (previousPageUpdate) {
            if (previousPageUpdate < dataFrame.pageUpdatedUTC) {
                previousPageUpdate = dataFrame.pageUpdatedUTC
                addNewFrame(dataFrame)
            }
        } else {
            previousPageUpdate = dataFrame.pageUpdatedUTC
            addNewFrame(dataFrame)
        }

    })

    if (mode === "stat") {
        var framesCalculated = [{
            IndicatorName: "Fertőzött",
            ...infected
        },
        {
            IndicatorName: "Aktív Fertőzött",
            ...activeInfected
        },
        {
            IndicatorName: "Gyógyult",
            ...recovered
        },
        {
            IndicatorName: "Elhunyt",
            ...deaths
        }]
    } else if (mode === "dStat") {
        var framesCalculated = [{
            IndicatorName: "Legidősebb Elhunyt",
            ...deathMaxAge
        },
        {
            IndicatorName: "Legfiatalabb Elhunyt",
            ...deathMinAge
        },
        {
            IndicatorName: "Átlagéletkor",
            ...deathsAverageAge
        }]
    } else if(mode === "sexStat") {
        var framesCalculated = [
            {
                IndicatorName: "Elhunyt Férfi",
                ...deathMale
            },
            {
                IndicatorName: "Elhunyt Nő",
                ...deathFemale
            }
        ]
    } else if (mode === "wStat") {
        var framesCalculated = [{
            IndicatorName: "Fertőzött",
            ...worldInfected
        },
        {
            IndicatorName: "Gyógyult",
            ...worldRecovered
        },
        {
            IndicatorName: "Elhunyt",
            ...worldDied
        }]
    } else if(mode === "quarantine") {
        var framesCalculated = [
            {
                IndicatorName: "Karanténban",
                ...homeQuarantine
            },
            {
                IndicatorName: "Mintavétel",
                ...sampling
            }
        ]
    } else if (mode === "allIn") {
        var framesCalculated = [{
            IndicatorName: "Fertőzött",
            ...infected
        },
        {
            IndicatorName: "Aktív Fertőzött",
            ...activeInfected
        },
        {
            IndicatorName: "Gyógyult",
            ...recovered
        },
        {
            IndicatorName: "Elhunyt",
            ...deaths
        },
        {
            IndicatorName: "Karanténban",
            ...homeQuarantine
        },
        {
            IndicatorName: "Mintavétel",
            ...sampling
        },
        {
            IndicatorName: "Legidősebb Elhunyt",
            ...deathMaxAge
        },
        {
            IndicatorName: "Legfiatalabb Elhunyt",
            ...deathMinAge
        },
        {
            IndicatorName: "Átlagéletkor",
            ...deathsAverageAge
        },
        {
            IndicatorName: "Elhunyt Férfi",
            ...deathMale
        },
        {
            IndicatorName: "Elhunyt Nő",
            ...deathFemale
        },
        {
            IndicatorName: "Fertőzött",
            ...worldInfected
        },
        {
            IndicatorName: "Gyógyult",
            ...worldRecovered
        },
        {
            IndicatorName: "Elhunyt",
            ...worldDied
        }]
    }

    return framesCalculated
}


async function coronaDataFramesProcessing(props) {
    const { client, dataFramesFrom, dataFramesTo, mode } = props

    const dataFramesFromMongo = await coronaDataFramesRequest({ client, dataFramesFrom, dataFramesTo })

    let deathList = await client.db().collection("deathList").aggregate([
        { $sort: { number: 1}}
    ]).toArray()

    return coronaProcessing(dataFramesFromMongo, deathList, mode)
}

mongodb.connect(
    process.env.CONNECTIONSTRING,
    { useNewUrlParser: true, useUnifiedTopology: true },
    function (err, client) {

        coronaDataFramesProcessing({
            client,
            dataFramesFrom: new Date("2020-03-04T00:00:00.000+0100"),
            dataFramesTo: new Date("2020-05-11T00:00:30.000+0100"),
            mode: "allIn"
        }).then(res => {
            let fileName = "coronaDataFramesProcessed.json"
            fs.writeFile("framesProcessed/" + fileName, JSON.stringify(res), err => {
                if (err) throw err
                console.log(fileName + ", Saved!")
                client.close()
            })
        })

    }
)