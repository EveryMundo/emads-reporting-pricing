'use strict'
const logr = require('@everymundo/simple-logr')
const { inChunks } = require('@everymundo/array-helpers');
const mongoHelper = require('./lib/db/mongodb')
const searchEngineOps = require('./lib/db/search-engine-operations')
const s3Operations = require('./lib/s3Operations')
const req = require('./lib/external/request')
const fHandler = require('./lib/fileHandler')

const handler = async (event, context) => {
  try {
    // let tenants = await mongoHelper.getAllSearchEngineDbs()
    const reportForClient = `Client-Report_${new Date().toLocaleDateString().replace(/\//g, "_")}.xlsx`
    const reportForDev = `Dev-Report_${new Date().toLocaleDateString().replace(/\//g, "_")}.xlsx`
    let tenants = ["a3"]
    for (const tenant of tenants) {

      let stringReport = `Routes report without prices ${new Date().toLocaleDateString()}`
      stringReport += "\ntenant, Total routes without prices"
      try {
        const { journeyType, lookaheadWindow } = await req.getSettings(tenant)
        console.log({ journeyType, lookaheadWindow })

        const routes = await searchEngineOps.aggregateFareWireCustomizerItemsV2(tenant, [
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
        stringReport += `\n${tenant},${routes.length}\n`
        console.log(`${tenant},${routes.length}`)
        await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, stringReport)
        stringReport = `\nOrigin, Destination, Currency, Available currencies, Report Notes`
        await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, stringReport)
        let i = 0;
        console.log(`${routes.length} routes`)
        for (const route of routes) {
          console.log(`Origin:${route._id.origin} with ${route.destinations.length} destinations`)
          const arrayOutput = inChunks(route.destinations, 100);
          for (const destinations of arrayOutput) {
            const prices = await req.getPrices(tenant, journeyType, lookaheadWindow, route._id.origin, destinations.map(dest => dest.destination))
            for (const destInfo of destinations) {
              if (!prices.length) {
                stringReport = `\n${route._id.origin},${destInfo.destination},${destInfo.currency},0, No prices found with this currency`
                await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, stringReport)
              } else {
                // Review destination
                const availableCurrencies = prices.filter(price => price.outboundFlight.arrivalAirportIataCode == destInfo.destination).map(price => price.priceSpecification.currencyCode)
                const curcAvailable = req.isCurrencyAvailable(destInfo.currency, availableCurrencies)

                const notes = curcAvailable ? "Review by DevTeam"
                  : "Low probability of this route having prices - update to a available currency if existent"
                stringReport = `\n${route._id.origin},${destInfo.destination},${destInfo.currency},${availableCurrencies.join("/")}, ${notes}`
                if (curcAvailable) {
                  await fHandler.appendToFile(`./reports/${tenant}-${reportForDev}`, stringReport)
                } else {
                  await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, stringReport)
                }
                /**
                 * Get a detailed report
                 *
                 */
                // if (!curcAvailable) {
                //   const adgroups = await searchEngineOps.getAdGroupsV1(tenant, { "routeIdentifier.o": route._id.origin, "routeIdentifier.d": destInfo.destination, "routeIdentifier.curC": destInfo.currency })
                //   stringReport = "\nAccountId, CampaignId, Campaign Name, AdGroup Id, AdGroup Name,Currency used, Sugested currencies\n" + adgroups.map((adgroup) => {
                //     return `${adgroup._id.accountId},${adgroup._id.campaignId},${adgroup.campaignName},${adgroup._id.adgroupId},${adgroup.name},${adgroup.routeIdentifier.curC},${availableCurrencies.join(" ")}`
                //   }).join("\n")
                //   await fHandler.appendToFile(`./reports/${tenant}-detailed${reportForClient}`, stringReport)
                // }
              }
            }

          }

          i++;
        }
      } catch (error) {
        console.log(error);
        continue;
      }

      // fs.writeFile(`./reports/${tenant}-all-pricing-report_${new Date().toLocaleDateString().replace(/\//g, "_")}.xlsx`, stringReport, "utf-8", (err) => {
      //   if (err) console.log(err);
      //   console.log("Report Saved!")
      // });
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