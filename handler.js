'use strict'
const logr = require('@everymundo/simple-logr')
const { inChunks } = require('@everymundo/array-helpers');
const mongoHelper = require('./lib/db/mongodb')
const searchEngineOps = require('./lib/db/search-engine-operations')
const s3Operations = require('./lib/s3Operations')
const req = require('./lib/external/request')
const fHandler = require('./lib/fileHandler')

const fs = require('fs');
const { env } = require('process')

const handler = async (event, context) => {
  try {
    // let tenants = await mongoHelper.getAllSearchEngineDbs()
    let tenants = ["a3"]
    for (const tenant of tenants) {
      let stringReport = ""
      stringReport += `Routes report without prices ${new Date().toLocaleDateString()}\n`
      stringReport += "tenant, Total routes without prices\n"
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
        stringReport += `${tenant},${routes.length}\n`
        console.log(`${tenant},${routes.length}`)
        await fHandler.appendToFile(`./reports/${tenant}-all-pricing-report_${new Date().toLocaleDateString().replace(/\//g, "_")}.xlsx`, stringReport)
        stringReport = `\nOrigin, Destination, Currency, Available currencies, Report Notes\n`
        let i = 0;
        console.log(`${routes.length} routes`)
        for (const route of routes) {
          console.log(`Origin:${route._id.origin} with ${route.destinations.length} destinations`)
          const arrayOutput = inChunks(route.destinations, 100);
          for (const destinations of arrayOutput) {
            // console.log({ destinations })
            const prices = await req.getPrices(tenant, journeyType, lookaheadWindow, route._id.origin, destinations.map(dest => dest.destination))
            for (const destInfo of destinations) {
              if (!prices.length) {
                stringReport += `\n${route._id.origin},${destInfo.destination},${destInfo.currency},0, No prices found with this currency\n`
              } else {
                // Review destination
                const availableCurrencies = prices.filter(price => price.outboundFlight.arrivalAirportIataCode == destInfo.destination).map(price => price.priceSpecification.currencyCode)
                const curcAvailable = req.isCurrencyAvailable(destInfo.currency, availableCurrencies)
                const notes = curcAvailable ? "There was a price. This route needs review by development team"
                  : "Low probability of this route having prices - update to a available currency if available"
                stringReport += `\n${route._id.origin},${destInfo.destination},${destInfo.currency},${availableCurrencies.join("/")}, ${notes}\n`
                await fHandler.appendToFile(`./reports/${tenant}-all-pricing-report_${new Date().toLocaleDateString().replace(/\//g, "_")}.xlsx`, stringReport)
                /**
                 * Get a detailed report
                 *
                 */
                // if (!curcAvailable) {
                //   const adgroups = await searchEngineOps.getAdGroupsV1(tenant, { "routeIdentifier.o": route._id.origin, "routeIdentifier.d": destInfo.destination, "routeIdentifier.curC": destInfo.currency })
                //   stringReport = "\nAccountId, CampaignId, Campaign Name, AdGroup Id, AdGroup Name" + adgroups.map((adgroup) => {
                //     return `${adgroup._id.accountId},${adgroup._id.campaignId},${adgroup.campaignName},${adgroup._id.adgroupId},${adgroup.name}`
                //   }).join("\n")
                //   await fHandler.appendToFile(`./reports/${tenant}-all-pricing-report_${new Date().toLocaleDateString().replace(/\//g, "_")}.xlsx`, stringReport)
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