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

async function scrapeData(url) {
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

    browser.close()

    return {
        pageUpdatedUTC, 
        infectedValue: Number(infectedValue.split(" ").join("")), 
        recovered: Number(recovered.split(" ").join("")), 
        homeQuarantine: Number(homeQuarantine.split(" ").join("")), 
        sampling: Number(sampling.split(" ").join("")),
        mapSrc,
        worldUpdatedUTC,
        worldInfected: Number(worldInfected.split(" ").join("")),
        worldRecovered: Number(worldRecovered.split(" ").join("")),
        worldDied: Number(worldDied.split(" ").join("")),
        dataFrameDate: new Date()
    }
}

schedule.scheduleJob("*/5 * * * * *", () => {

    console.time("⏱ ")
    scrapeData("https://koronavirus.gov.hu/").then( dataFrame => {

        dataFrames.insertOne(dataFrame).then( async _ => {
            let d = new Date()
            await downloadImage(dataFrame.mapSrc, `infectionMaps/${d.getFullYear()}-${d.getMonth()+1}-${d.getDate()}_${d.getHours()}.${d.getMinutes()}.jpg`)
            console.timeEnd("⏱ ")
            console.log(dataFrame)
        })
        
    })

})