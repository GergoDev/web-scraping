const mongodb = require('mongodb')
const dotenv = require('dotenv')
dotenv.config()

mongodb.connect(
    process.env.CONNECTIONSTRING, 
    {useNewUrlParser: true, useUnifiedTopology: true}, 
    async function(err, client) {

        let pageUpdated = new Date("2020.04.08. 06:19")
        pageUpdatedUTC = new Date(pageUpdated.getTime() - (60 * 60 * 1000))
        let infected = "895"
        let recovered = "94"
        let deaths = "58"
        deaths = Number(deaths.split(" ").join(""))
        let homeQuarantine = "15 481"
        let sampling = "25 748"
        let worldUpdated = new Date("2020.04.08. 06:23")
        worldUpdatedUTC = new Date(worldUpdated.getTime() - (60 * 60 * 1000))
        let worldInfected = "1 430 141"
        let worldRecovered = "301 130"
        let worldDied = "82 119"

        if(deaths != 0) {
            let deathListTillDate = await client.db().collection("deathList").aggregate([
                { $sort: { number: 1}},
                { $limit: deaths}
            ]).toArray()

            var sumAge = 0
            var deathsAverageAge = 0
            var deathMinAge = 200
            var deathMaxAge = 0
            deathListTillDate.forEach( (person, index) => {
                sumAge += person.age
                if(person.age < deathMinAge) deathMinAge = person.age
                if(person.age > deathMaxAge) deathMaxAge = person.age 
                if(deathListTillDate.length === (index+1)) 
                    deathsAverageAge = Math.round(sumAge / deaths)
            })
        } else {
            deaths = ""
            var deathsAverageAge = ""
            var deathMinAge = ""
            var deathMaxAge = ""
        }

        console.log({
            pageUpdatedUTC, 
            infected: Number(infected.split(" ").join("")), 
            recovered: (recovered === "0") ? "" : Number(recovered.split(" ").join("")), 
            deaths,
            deathMinAge,
            deathMaxAge,
            deathsAverageAge,
            homeQuarantine: Number(homeQuarantine.split(" ").join("")), 
            sampling: Number(sampling.split(" ").join("")),
            worldUpdatedUTC,
            worldInfected: Number(worldInfected.split(" ").join("")),
            worldRecovered: Number(worldRecovered.split(" ").join("")),
            worldDied: Number(worldDied.split(" ").join("")),
            dataFrameDate: new Date()
        })

        client.db().collection("dataFrames").insertOne({
            pageUpdatedUTC, 
            infected: Number(infected.split(" ").join("")), 
            recovered: (recovered === "0") ? "" : Number(recovered.split(" ").join("")), 
            deaths,
            deathMinAge,
            deathMaxAge,
            deathsAverageAge,
            homeQuarantine: Number(homeQuarantine.split(" ").join("")), 
            sampling: Number(sampling.split(" ").join("")),
            worldUpdatedUTC,
            worldInfected: Number(worldInfected.split(" ").join("")),
            worldRecovered: Number(worldRecovered.split(" ").join("")),
            worldDied: Number(worldDied.split(" ").join("")),
            dataFrameDate: new Date()
        }).then( _ => {
            console.log("done", new Date())
            client.close()
        })
        
    }
)