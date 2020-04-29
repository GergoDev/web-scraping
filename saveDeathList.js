const puppeteer = require("puppeteer")
const mongodb = require('mongodb')
const dotenv = require('dotenv')
dotenv.config()

async function scrapeData(url) {
    const browser = await puppeteer.launch({headless: false})
    const page = await browser.newPage()

    let tableData = []
    let goToNextPage = true
    let pageNumber = 0
    let pagination = ""

    while(goToNextPage) {

        if(pageNumber) pagination = "?page="+pageNumber

        pageNumber++

        await page.goto(url+pagination)

        const [deathsHeader] = await page.$x('//*[@id="block-block-22"]/div/h1')
        if(deathsHeader) {
            let actualTableData = await page.evaluate(() => {
                const tds = Array.from(document.querySelectorAll('table tr td'))
                return tds.map(td => td.innerHTML.trim())
            })
            tableData = tableData.concat(actualTableData)
        } else {
            goToNextPage = false
        }

    }

    let x = 1
    let extractedData = []
    let data = {}
    tableData.forEach( item => {
        if(x === 1) {
            data = {}
            data["number"] = Number(item) 
        } else if(x === 2) {
            data["sex"] = item
        } else if(x === 3) {
            data["age"] = Number(item) 
        } else if(x === 4) {
            data["diseases"] = item
            extractedData.push(data)
        }
        if(x === 4) 
            x = 1 
        else 
            x++
    })

    await browser.close()

    return extractedData
}


mongodb.connect(
    process.env.CONNECTIONSTRING, 
    {useNewUrlParser: true, useUnifiedTopology: true}, 
    function(err, client) {

        scrapeData("https://koronavirus.gov.hu/elhunytak").then( res => {
            console.log(res.length, "elements on the list")
            client.db().collection("deathList").insertMany(res).then( _ => console.log("done"))
        })

    }
)