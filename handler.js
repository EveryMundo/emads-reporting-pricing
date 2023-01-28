'use strict'
const logr = require('@everymundo/simple-logr')
const { inChunks } = require('@everymundo/array-helpers');
const mongoHelper = require('./lib/db/mongodb')
const searchEngineOps = require('./lib/db/search-engine-operations')
const s3Operations = require('./lib/s3Operations')
const req = require('./lib/external/request')
const fHandler = require('./lib/fileHandler')

const getAdgAggregation = (tenant, query) => {
  return searchEngineOps.aggregateAdGroupsV1(tenant, [{
    $match: query
  },
  {
    $group: {
      _id: {
        "accountId": "$_id.accountId",
        "campaignId": "$_id.campaignId",
      },
      campaigns: {
        $addToSet: {
          "accountId": "$_id.accountId",
          "campaignId": "$_id.campaignId",
          "campaignName": "$campaignName",
        }
      },
      total: { $sum: 1 }
    }
  },
  {
    $sort: { total: -1 }
  }])
}
const getDocsAggregation = (tenant) => {
  return searchEngineOps.aggregateFareWireCustomizerItemsV2(tenant, [
    {
      $match: {
        "$or": [{ price: { $eq: 0 } }, { price: { $eq: "" } }]
      }
    },
    {
      $group: {
        _id: {
          "origin": "$origin",
          // "destination": "$destination",
        },
        destinations: {
          $addToSet: {
            "destination": "$destination",
            "currency": "$priceFormat.isoCode"
          }
        },
        total: { $sum: 1 }
      }
    },
    {
      $sort: { total: -1 }
    }

  ])
}

const handler = async (event, context) => {
  try {
    // let tenants = await mongoHelper.getAllSearchEngineDbs()
    let tenants = ["a3"]
    const reportForClient = `client-report_${new Date().toLocaleDateString().replace(/\//g, "_")}.csv`
    const reportForDev = `dev-report_${new Date().toLocaleDateString().replace(/\//g, "_")}.csv`
    for (const tenant of tenants) {

      let stringReport = `Routes report without prices ${new Date().toLocaleDateString()}`
      stringReport += "\ntenant, Total routes without prices"
      try {
        const { journeyType, lookaheadWindow } = await req.getSettings(tenant)
        console.log({ journeyType, lookaheadWindow })

        const routes = await getDocsAggregation(tenant)
        stringReport += `\n${tenant},${routes.length}\n`
        console.log(`${tenant},${routes.length}`)
        await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, stringReport)
        stringReport = `\nOrigin, Destination, Currency, Available currencies, Report Notes`
        await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, stringReport)
        let i = 0;
        console.log(`${routes.length} routes`)
        const routesToQueryAdgroups = []
        for (const route of routes) {
          console.log(`Origin:${route._id.origin} with ${route.destinations.length} destinations`)
          const arrayOutput = inChunks(route.destinations, 20);
          const pricesResultArray = await Promise.all(arrayOutput.map(chunks => req.getPrices(tenant, journeyType, lookaheadWindow, route._id.origin, chunks.map(dest => dest.destination))))
          const priceResultFlatted = pricesResultArray.flatMap(priceArr => priceArr) // flatted sputnik Results
          let fileBuffer1 = ""
          let fileBuffer2 = ""
          route.destinations.forEach((destInfo) => {
            if (!priceResultFlatted.length) {
              fileBuffer1 += `\n${route._id.origin},${destInfo.destination},${destInfo.currency},0, No prices found with this currency`
            } else {
              const availableCurrencies = new Array(...new Set(priceResultFlatted.filter(price => price.outboundFlight.arrivalAirportIataCode == destInfo.destination).map(price => price.priceSpecification.currencyCode)))
              const curcAvailable = availableCurrencies.length && req.isCurrencyAvailable(destInfo.currency, availableCurrencies)
              const notes = curcAvailable ? "Review by DevTeam"
                : "Low probability of this route having prices - update to a available currency if existent"
              if (curcAvailable) {
                fileBuffer2 += `\n${route._id.origin},${destInfo.destination},${destInfo.currency},${availableCurrencies.join("/")}, ${notes}`
              } else {
                fileBuffer1 += `\n${route._id.origin},${destInfo.destination},${destInfo.currency},${availableCurrencies.join("/")}, ${notes}`
              }

              if (!curcAvailable && availableCurrencies.length) {
                routesToQueryAdgroups.push({ query: { "routeIdentifier.o": route._id.origin, "routeIdentifier.d": destInfo.destination, "routeIdentifier.curC": destInfo.currency, mapped: true }, availableCurrencies })
              }
            }
          }) // flatted empty routes
          await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, fileBuffer1)
          await fHandler.appendToFile(`./reports/${tenant}-${reportForDev}`, fileBuffer2)
        }
        // for naming convention - report campaigns only
        if (routesToQueryAdgroups.length) {
          await fHandler.appendToFile(`./reports/${tenant}-detailed-${reportForClient}`, "Account Id, Campaign Id, Campaign Name, Currency Used, Available Currency")
          const routesChunk = inChunks(routesToQueryAdgroups, 5);
          // const dbQueries = routesChunk.map((adgroups) => {
          for (const routesArray of routesChunk) {
            let fileBuffer3 = ""
            const fullfiledCampaigns = await Promise.all(routesArray.map(data => getAdgAggregation(tenant, data.query)))
            // console.log({ fullfiledCampaigns })
            routesArray.forEach((route, index) => {
              fileBuffer3 += fullfiledCampaigns[index].flatMap(x => x.campaigns).map((dest) => {
                return `\n${dest.accountId},${dest.campaignId},${dest.campaignName},${route.query["routeIdentifier.curC"]}, ${route.availableCurrencies.join("/")}`
              }).join("\n")
            })
            await fHandler.appendToFile(`./reports/${tenant}-detailed-${reportForClient}`, fileBuffer3)
          }
        }
      } catch (error) {
        console.log(error);
        continue;
      }
    }


    console.log("Finished!")
    return tenants
  } catch (error) {
    logr.error(error);
  }
};
try {
  handler()

} catch (error) {
  console.log({ error })
}
module.exports = {
  handler
}