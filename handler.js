'use strict'
const logr = require('@everymundo/simple-logr')
const { inChunks } = require('@everymundo/array-helpers');
const mongoHelper = require('./lib/db/mongodb')
const searchEngineOps = require('./lib/db/search-engine-operations')
const s3Operations = require('./lib/s3Operations')
const req = require('./lib/external/request')
const fHandler = require('./lib/fileHandler')

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
        const organizationSettings = await req.getOrganizationSettings(tenant, 'settings/display-default')
        const organizationConfig = await req.getOrganizationSettings(tenant, 'settings/configuration')
        const envConfig = organizationConfig.domains.find(dom => dom.environment == "PRODUCTION")
        const siteEditions = organizationSettings.siteEditionConfigurations.map(({ siteEdition, currencyConfigurations }) => {
          return {
            n: siteEdition.name,
            l: siteEdition.language,
            c: siteEdition.country,
            g: siteEdition.global,
            config: currencyConfigurations,
            gPrefix: (siteEdition.country !== null ? `GS:${siteEdition.language.toLowerCase()}-${siteEdition.country.toUpperCase()}` : `GS:${siteEdition.language.toLowerCase()}`)
          }
        })

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
        stringReport = namingConvention ? `\nOrigin, Destination, Currency, Available currencies, campaignId, campaignName, Matching TRFX site edition, Probability of having a price - with other currency` : `\nOrigin, Destination, Currency, Available currencies, campaignId, campaignName, adgroupId, adgroupName,  Probability of having a price`
        await fHandler.appendToFile(`./reports/${tenant}-detailed-${reportForClient}`, stringReport)
        const routesToQueryAdgroups = []
        for (const origin of Object.keys(aggregatedByOrigin)) {
          console.log(`Origin:${origin} with ${aggregatedByOrigin[origin].destinations.length} destinations`)
          const destinationsInChunks = inChunks(aggregatedByOrigin[origin].destinations, 20);
          const pricesResultArray = await Promise.all(destinationsInChunks.map(chunks => req.getPrices(tenant, organizationSettings.journeyType, lookaheadWindow, organizationSettings.dataExpirationWindow, origin, chunks.map(dest => dest.destination))))
          const priceResultFlatted = pricesResultArray.flatMap(priceArr => priceArr)
          let fileBuffer1 = ""
          let fileBuffer2 = ""
          let fileBuffer3 = ""
          aggregatedByOrigin[origin].destinations.forEach((destination, key) => {
            // console.log(`Origin:${origin}, Destination: ${destination}`)
            const OAndD = aggregatedByOrigin[origin].data.filter(route => route.origin == origin && route.destination == destination)
            if (!priceResultFlatted.length) {
              fileBuffer1 += `\n${origin},${destination},Any currency,, Route not found, Adgroups using this route shouldn't have Ads with prices`
              fileBuffer3 += OAndD.map((o_d) => `\n${o_d.origin},${o_d.destination},${o_d.currency},, ${Object.values(o_d.targeting).join(",")},,Very Low`)
            } else {

              const availableCurrencies = new Array(...new Set(priceResultFlatted.filter(price => price.outboundFlight.departureAirportIataCode == origin && price.outboundFlight.arrivalAirportIataCode == destination).map(price => price.priceSpecification.currencyCode)))
              OAndD.forEach((o_d) => {
                const isCurrencyInList = req.isCurrencyAvailable(o_d.currency, availableCurrencies)
                let notes = availableCurrencies.length ? (isCurrencyInList ? " Dev needs to review" : "Route was found but with other currencies, Use suggested currency on cases where trfx configuration match. See detailed report for details") : "Route not found, Adgroups using this route shouldn't have Ads with prices "
                if (isCurrencyInList) {
                  fileBuffer2 = `\n${o_d.origin},${o_d.destination},${o_d.currency}, ${availableCurrencies.join("/")}, ${notes}`
                } else {
                  const matSE = matchingSE(o_d.targeting.campaignName, siteEditions)
                  const cListMatchSE = availableCurrencies.filter((acurc) => matSE.find((se) => se.c.indexOf(acurc) !== -1))
                  const redirectURL = matSE.length > 0 ? (`https://${envConfig.url}/redirect?edition=${matSE[0].n}&orig=${o_d.origin}&dest=${o_d.destination}&tc=cici`) : ""
                  fileBuffer1 = `\n${o_d.origin},${o_d.destination},${o_d.currency}, ${availableCurrencies.join("/")}, ${notes}`
                  fileBuffer3 = `\n${o_d.origin},${o_d.destination},${o_d.currency}, ${availableCurrencies.join("/")}, ${Object.values(o_d.targeting).join(",")},${namingConvention ? matSE.map(a => `${a.n}:${a.c}`).join(" ") : "This campaign does not match a site edition"}, ${cListMatchSE.length > 0 ? "High" : "Low"}, ${redirectURL}`
                }
              })
            }

          })
          await fHandler.appendToFile(`./reports/${tenant}-${reportForClient}`, fileBuffer1)
          await fHandler.appendToFile(`./reports/${tenant}-${reportForDev}`, fileBuffer2)
          await fHandler.appendToFile(`./reports/${tenant}-detailed-${reportForClient}`, fileBuffer3)
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

const matchingSE = (campaignName, siteEditions) => {
  return siteEditions.filter((sE) => campaignName.indexOf(sE.gPrefix) !== -1).map(se => { return { n: se.n, c: se.config.map(c => c.currencyCode) } })
}

try {
  handler()

} catch (error) {
  console.log({ error })
}
module.exports = {
  handler
}