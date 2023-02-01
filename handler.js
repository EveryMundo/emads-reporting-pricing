'use strict'
const logr = require('@everymundo/simple-logr')
const { inChunks } = require('@everymundo/array-helpers');
const mongoHelper = require('./lib/db/mongodb')
const searchEngineOps = require('./lib/db/search-engine-operations')
const s3Operations = require('./lib/s3Operations')
const req = require('./lib/external/request')
const fHandler = require('./lib/fileHandler')

const getAdgAggregation = (tenant, namingConvention, query) => {
  let pipeline = [{
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
  }]

  if (!namingConvention) {
    pipeline[1]['$group']["_id"]["adgroupId"] = "$_id.adgroupId"
    pipeline[1]['$group']["campaigns"]["$addToSet"]["adgroupId"] = "$_id.adgroupId"
    pipeline[1]['$group']["campaigns"]["$addToSet"]["adgroupName"] = "$name"

  }
  return searchEngineOps.aggregateAdGroupsV1(tenant, pipeline)
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
          "destination": "$destination",
          "currency": "$priceFormat.isoCode",
        },
        data: {
          $addToSet: {
            "origin": "$origin",
            "destination": "$destination",
            "currency": "$priceFormat.isoCode",
            "targeting": "$targeting"
          }
        },
        total: { $sum: 1 }
      }
    },
    {
      $sort: { "_id.origin": 1 }
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
      stringReport += "\ntenant, Total origins without prices"
      try {
        const { journeyType, lookaheadWindow, namingConvention } = await req.getSettings(tenant)
        console.log({ journeyType, lookaheadWindow })

        const routes = await getDocsAggregation(tenant)
        const aggregatedByOrigin = {}
        routes.forEach((b) => {
          if (aggregatedByOrigin[`${b._id.origin}`] == undefined) {
            aggregatedByOrigin[`${b._id.origin}`] = { data: b.data, destinations: [b._id.destination] }
          } else {
            aggregatedByOrigin[`${b._id.origin}`].data = aggregatedByOrigin[`${b._id.origin}`].data.concat(b.data)
            aggregatedByOrigin[`${b._id.origin}`].destinations.push(b._id.destination)

          }
        })
        stringReport += `\n${tenant},${routes.length}\n`
        // console.log(`${tenant},${routes.length}`)
        await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, stringReport)
        stringReport = `\nOrigin, Destination, Currency, Available currencies, Report Notes, Recommendation`
        await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, stringReport)
        stringReport = namingConvention ? `\nOrigin, Destination, Currency, Available currencies, campaignId, campaignName` : `\nOrigin, Destination, Currency, Available currencies, campaignId, campaignName, adgroupId, adgroupName`
        await fHandler.appendToFile(`./reports/${tenant}-detailed-${reportForClient}`, stringReport)
        // console.log(`${routes.length} routes`)
        const routesToQueryAdgroups = []
        for (const origin of Object.keys(aggregatedByOrigin)) {
          console.log(`Origin:${origin} with ${aggregatedByOrigin[origin].destinations.length} destinations`)
          const destinationsInChunks = inChunks(aggregatedByOrigin[origin].destinations, 20);
          const pricesResultArray = await Promise.all(destinationsInChunks.map(chunks => req.getPrices(tenant, journeyType, lookaheadWindow, origin, chunks.map(dest => dest.destination))))
          const priceResultFlatted = pricesResultArray.flatMap(priceArr => priceArr)
          let fileBuffer1 = ""
          let fileBuffer2 = ""
          let fileBuffer3 = ""
          aggregatedByOrigin[origin].destinations.forEach((destination, key) => {
            // console.log(`Origin:${origin}, Destination: ${destination}`)
            const OAndD = aggregatedByOrigin[origin].data.filter(route => route.origin == origin && route.destination == destination)
            if (!priceResultFlatted.length) {
              fileBuffer1 += `\n${origin},${destination},Any currency,, Route not found, Adgroups using this route shouldn't have Ads with prices`
              fileBuffer3 += OAndD.map((o_d) => `\n${o_d.origin},${o_d.destination},${o_d.currency},, ${Object.values(o_d.targeting).join(",")}`)
            } else {

              const availableCurrencies = new Array(...new Set(priceResultFlatted.filter(price => price.outboundFlight.departureAirportIataCode == origin && price.outboundFlight.arrivalAirportIataCode == destination).map(price => price.priceSpecification.currencyCode)))
              OAndD.forEach((o_d) => {
                let notes = availableCurrencies.length ? (req.isCurrencyAvailable(o_d.currency, availableCurrencies) ? " Dev needs to review" : "Route was found but with other currencies, The origin/destination/currency may not be valid") : "Route not found, Adgroups using this route shouldn't have Ads with prices "
                if (req.isCurrencyAvailable(o_d.currency, availableCurrencies)) {
                  fileBuffer2 = `\n${o_d.origin},${o_d.destination},${o_d.currency}, ${availableCurrencies.join("/")}, ${notes}`
                } else {
                  fileBuffer1 = `\n${o_d.origin},${o_d.destination},${o_d.currency}, ${availableCurrencies.join("/")}, ${notes}`
                  fileBuffer3 = `\n${o_d.origin},${o_d.destination},${o_d.currency}, ${availableCurrencies.join("/")}, ${Object.values(o_d.targeting).join(",")}`
                }
              })
            }

          })
          await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, fileBuffer1)
          await fHandler.appendToFile(`./reports/${tenant}-${reportForDev}`, fileBuffer2)
          await fHandler.appendToFile(`./reports/${tenant}-detailed-${reportForClient}`, fileBuffer3)
        }

        // for (const route of routes) {
        //   console.log(`Origin:${route._id.origin} with ${route.destinations.length} destinations`)
        //   const arrayOutput = inChunks(route.destinations, 20);
        //   const pricesResultArray = await Promise.all(arrayOutput.map(chunks => req.getPrices(tenant, journeyType, lookaheadWindow, route._id.origin, chunks.map(dest => dest.destination))))
        //   const priceResultFlatted = pricesResultArray.flatMap(priceArr => priceArr) // flatted sputnik Results
        //   let fileBuffer1 = ""
        //   let fileBuffer2 = ""
        //   route.destinations.forEach((destInfo) => {
        //     if (!priceResultFlatted.length) {
        //       fileBuffer1 += `\n${route._id.origin},${destInfo.destination},${destInfo.currency},, No prices available in the next 365 days`
        //     } else {
        //       const availableCurrencies = new Array(...new Set(priceResultFlatted.filter(price => price.outboundFlight.arrivalAirportIataCode == destInfo.destination).map(price => price.priceSpecification.currencyCode)))
        //       const curcAvailable = availableCurrencies.length && req.isCurrencyAvailable(destInfo.currency, availableCurrencies)
        //       const notes = curcAvailable ? ""
        //         : (availableCurrencies.length == 0 ? "No prices available in the next 365 days" : "Update to a available currency")
        //       if (curcAvailable) {
        //         fileBuffer2 += `\n${route._id.origin},${destInfo.destination},${destInfo.currency},${availableCurrencies.join("/")}, ${notes}`
        //       } else {
        //         fileBuffer1 += `\n${route._id.origin},${destInfo.destination},${destInfo.currency},${availableCurrencies.join("/")}, ${notes}`
        //       }

        //       if (!curcAvailable && availableCurrencies.length) {
        //         routesToQueryAdgroups.push({ query: { "routeIdentifier.o": route._id.origin, "routeIdentifier.d": destInfo.destination, "routeIdentifier.curC": destInfo.currency, mapped: true }, availableCurrencies })
        //       }
        //     }
        //   }) // flatted empty routes
        //   await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, fileBuffer1)
        //   await fHandler.appendToFile(`./reports/${tenant}-${reportForDev}`, fileBuffer2)
        // }
        // for naming convention - report campaigns only
        // if (routesToQueryAdgroups.length < 200) {
        //   console.log(`${routesToQueryAdgroups.length} queries`)
        //   if (namingConvention) {
        //     await fHandler.appendToFile(`./reports/${tenant}-detailed-${reportForClient}`, "Account Id, Campaign Id, Campaign Name, Currency Used, Available Currency")
        //   } else {
        //     await fHandler.appendToFile(`./reports/${tenant}-detailed-${reportForClient}`, "Account Id, Campaign Id, Campaign Name,Adgroup ID, AdGroup Name, Currency Used, Available Currency")
        //   }
        //   const routesChunk = inChunks(routesToQueryAdgroups, 100);
        //   // const dbQueries = routesChunk.map((adgroups) => {
        //   for (const routesArray of routesChunk) {
        //     let fileBuffer3 = ""
        //     const fullfiledCampaigns = await Promise.all(routesArray.map(data => getAdgAggregation(tenant, namingConvention, data.query)))
        //     // console.log({ fullfiledCampaigns })
        //     routesArray.forEach((route, index) => {
        //       fileBuffer3 += fullfiledCampaigns[index].flatMap(x => x.campaigns).map((dest) => {
        //         return namingConvention ?
        //           `\n${dest.accountId},${dest.campaignId},${dest.campaignName},${route.query["routeIdentifier.curC"]}, ${route.availableCurrencies.join("/")}`
        //           : `\n${dest.accountId},${dest.campaignId},${dest.campaignName}, ${dest.adgroupId}, ${dest.adgroupName},${route.query["routeIdentifier.curC"]}, ${route.availableCurrencies.join("/")}`
        //       }).join("\n")
        //     })
        //     await fHandler.appendToFile(`./reports/${tenant}-detailed-${reportForClient}`, fileBuffer3)
        //   }
        // } else {
        //   console.log("Query too big. run separately")
        // }
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