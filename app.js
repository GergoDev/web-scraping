const puppeteer = require("puppeteer")
const fs = require('fs');
const axios = require('axios');
const schedule = require('node-schedule')
const dataFrames = require('./db').db().collection('dataFrames')

const downloadImage = (url, image_path) =>
  axios({
    url,
    responseType: 'stream',
  }).then(
    response =>
      new Promise((resolve, reject) => {
        response.data
          .pipe(fs.createWriteStream(image_path))
          .on('finish', () => resolve())
          .on('error', e => reject(e));
      }),
  );

async function scrapeData(url, url2) {
    const browser = await puppeteer.launch()
    const page = await browser.newPage()
    await page.goto(url)

    const [el] = await page.$x('//*[@id="block-block-1"]/div/p')
    const pageUpdatedValueProperty = await el.getProperty("textContent")
    let pageUpdated = await pageUpdatedValueProperty.jsonValue()
    pageUpdated = new Date(pageUpdated.split("dátuma: ")[1].trim())
    pageUpdatedUTC = new Date(pageUpdated.getTime() - (60 * 60 * 1000))

    const [el2] = await page.$x('//*[@id="block-system-main"]/div/div[1]/div[2]/div/div[2]/div[1]/div/span/div/span[1]')
    const infectedValueProperty = await el2.getProperty("textContent")
    const infectedValue = await infectedValueProperty.jsonValue()

    const [el3] = await page.$x('//*[@id="block-system-main"]/div/div[1]/div[2]/div/div[2]/div[2]/div/span/div/span[1]')
    const recoveredValueProperty = await el3.getProperty("textContent")
    const recovered = await recoveredValueProperty.jsonValue()

    const [el4] = await page.$x('//*[@id="block-system-main"]/div/div[1]/div[2]/div/div[2]/div[4]/div/span/div/span[1]')
    const homeQuarantineValueProperty = await el4.getProperty("textContent")
    const homeQuarantine = await homeQuarantineValueProperty.jsonValue()

    const [el5] = await page.$x('//*[@id="block-system-main"]/div/div[1]/div[2]/div/div[2]/div[5]/div/span/div/span[1]')
    const samplingValueProperty = await el5.getProperty("textContent")
    const sampling = await samplingValueProperty.jsonValue()

    const [el6] = await page.$x('//*[@id="block-system-main"]/div/div[1]/div[3]/div/div[1]/div/a/div/img')
    const mapSrcProperty = await el6.getProperty("src")
    const mapSrc = await mapSrcProperty.jsonValue()
    
    const [el7] = await page.$x('//*[@id="block-block-2"]/div/p')
    const worldUpdatedProperty = await el7.getProperty("textContent")
    let worldUpdated = await worldUpdatedProperty.jsonValue()
    worldUpdated = new Date(worldUpdated.split("dátuma: ")[1].trim())
    worldUpdatedUTC = new Date(worldUpdated.getTime() - (60 * 60 * 1000))

    const [el8] = await page.$x('//*[@id="block-system-main"]/div/div[1]/div[4]/div/div[2]/div[1]/div/span/div/span[1]')
    const worldInfectedValueProperty = await el8.getProperty("textContent")
    const worldInfected = await worldInfectedValueProperty.jsonValue()

    const [el9] = await page.$x('//*[@id="block-system-main"]/div/div[1]/div[4]/div/div[2]/div[2]/div/span/div/span[1]')
    const worldRecoveredValueProperty = await el9.getProperty("textContent")
    const worldRecovered = await worldRecoveredValueProperty.jsonValue()

    const [el10] = await page.$x('//*[@id="block-system-main"]/div/div[1]/div[4]/div/div[2]/div[3]/div/span/div/span[1]')
    const worldDiedValueProperty = await el10.getProperty("textContent")
    const worldDied = await worldDiedValueProperty.jsonValue()

    let tableData = []
    let goToNextPage = true
    let pageNumber = 0
    let pagination = ""

    while(goToNextPage) {

        if(pageNumber) pagination = "?page="+pageNumber

        pageNumber++

        await page.goto(url2+pagination)

        const [deathsHeader] = await page.$x('//*[@id="block-block-22"]/div/h1')
        if(deathsHeader) {
            let actualTableData = await page.evaluate(() => {
                const tds = Array.from(document.querySelectorAll('table tr td'))
                return tds.map(td => td.innerHTML.trim())
            })
            tableData = tableData.concat(actualTableData)
        } 
        else {
            goToNextPage = false
        }

    }

    const deathsValue = tableData.length/4

    let x = 1
    let y = 0
    let sumAge = 0
    let deathsAverageAge = 0
    let deathMinAge = 200
    let deathMaxAge = 0
    tableData = [0, ...tableData]
    tableData.forEach( item => {
        if(x%4 == 0) {
            let age = Number(item)
            sumAge += age
            y++
            if(age < deathMinAge) deathMinAge = age
            if(age > deathMaxAge) deathMaxAge = age
        }  
        x++
        if(tableData.length == x) deathsAverageAge = sumAge / y 
    })

    deathsAverageAge = Math.round(deathsAverageAge)

    browser.close()

    return {
        mapSrc,
        dataFrame: {
            pageUpdatedUTC, 
            infected: Number(infectedValue.split(" ").join("")), 
            recovered: Number(recovered.split(" ").join("")), 
            deaths: deathsValue,
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
        }
    }
}

schedule.scheduleJob("*/5 * * * * *", () => {

    scrapeData("https://koronavirus.gov.hu/", "https://koronavirus.gov.hu/elhunytak").then( async data => {

        let lastPageUpdate = await dataFrames.aggregate([ {$sort: {dataFrameDate: -1}} ]).toArray()
        
        console.log("WEB SCRAPING TASK RAN", new Date())

        if(lastPageUpdate[0].pageUpdatedUTC < data.dataFrame.pageUpdatedUTC) {
            dataFrames.insertOne(data.dataFrame).then( async _ => {
                let d = new Date()
                await downloadImage(data.mapSrc, `infectionMaps/${d.getFullYear()}-${d.getMonth()+1}-${d.getDate()}_${d.getHours()}.${d.getMinutes()}.jpg`)
                console.log(data.dataFrame)
            })
        }

        console.log("")

    })

})