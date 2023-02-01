const httpClient = require('@everymundo/http-client')
const logr = require('@everymundo/simple-logr')
const sputnik = require('./sputnik')

const getPrices = async (tenant, journeyType, lookaheadWindow, dataExpirationWindow, origin, destinations) => {
  try {
    const URL = `https://openair-california.airtrfx.com/airfare-sputnik-service/v3/${tenant.toLowerCase()}/fares/search/`
    logr.debug(URL)
    const endpoint = httpClient.urlToEndpoint(URL, {
      'EM-API-Key': process.env.EM_BE_API_KEY,
      'content-type': 'application/json'
    })
    let data = sputnik.createPayload(tenant, journeyType, 365, dataExpirationWindow)
    data.origins = [origin]
    data.destinations = destinations
    if (journeyType == "CUSTOM")
      delete data.journeyType
    const response = await httpClient.promisePost(endpoint, data)
    return JSON.parse(response.responseText)
  } catch (error) {
    console.log({ error })
    return []
  }
}

const getSettings = async (tenantCode) => {
  const URL = `${process.env.AIRSEM_MANAGER_HOST}/tenants/sem-settings/${tenantCode.toLowerCase()}`
  // logr.debug(URL)
  const endpoint = httpClient.urlToEndpoint(URL, {
    EM_API_KEY: process.env.EM_BE_API_KEY,
    'content-type': 'application/json',
    'em-team': 'EMAds'
  })

  try {
    const response = await httpClient.promiseGet(endpoint)
    // console.log(`Settings for ${tenantCode.toUpperCase()}: ${response.responseText}`)
    return JSON.parse(response.responseText)
  } catch (error) {
    if (error.resTxt) {
      console.log(`Error getting settings for ${tenantCode}: ${error.resTxt}`)
    } else {
      console.log(`Error getting settings for ${tenantCode}: ${error.message}`)
    }
  }
}

const getOrganizationSettings = async (tenantCode, path) => {
  const URL = `${process.env.OPENAIR_HOST}/organization-service/v1/tenants/code/${tenantCode.toLowerCase()}/${path}`
  logr.debug(URL)
  const endpoint = httpClient.urlToEndpoint(URL, {
    'EM-API-Key': process.env.EM_BE_API_KEY,
    'content-type': 'application/json',
    'em-team': 'EMAds'
  })

  try {
    const response = await httpClient.promiseGet(endpoint)
    // console.log(`Settings for ${tenantCode.toUpperCase()}: ${response.responseText}`)
    return JSON.parse(response.responseText)
  } catch (error) {
    if (error.resTxt) {
      console.log(`Error getting settings for ${tenantCode}: ${error.resTxt}`)
    } else {
      console.log(`Error getting settings for ${tenantCode}: ${error.message}`)
    }
  }
}

const isCurrencyAvailable = (currency, arrayOfCurrencies) => {
  // If the currency was not found on the available list, then we have no prices
  return !(arrayOfCurrencies.indexOf(currency) === -1)
}
module.exports = {
  getSettings,
  getPrices,
  isCurrencyAvailable,
  getOrganizationSettings
}