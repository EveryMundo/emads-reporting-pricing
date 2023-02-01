const minDateOfDeparture = (today) => {
  return new Date(today.getFullYear(), today.getMonth(), today.getDate() + 1)
}
const maxDateOfDeparture = (today, daysInTheFuture) => {
  return new Date(today.getFullYear(), today.getMonth(), today.getDate() + daysInTheFuture)
}

const createPayload = (tenant, journeyType, lookaheadWindow, dataExpirationWindow) => {
  return {
    journeyType,
    "origins": [],
    "destinations": [],
    "outputFormat": {
      "price": {
        "decimalSeparator": ".",
        "thousandSeparator": ",",
        "decimalPlaces": 0
      },
      "datePattern": "MM/dd/yy",
      "languageCode": "en"
    },
    "faresPerRoute": 5,
    "routesLimit": 100,
    "faresLimit": 100,
    "dataExpirationWindow": dataExpirationWindow,
    "departureDaysInterval": {
      "start": 0,
      "end": lookaheadWindow
    },
    "fareSorting": [
      {
        "priceSpecification.usdTotalPrice": "ASC"
      }
    ],
    "outputFields": [
      "passengerDetails.count",
      "datacoreId",
      "origin.city.name",
      "origin.city.image",
      "destination.city.name",
      "destination.city.image",
      "origin.country.name",
      "destination.country.name",
      "origin.city.image",
      "destination.city.image",
      "origin.country.image",
      "destination.country.image",
      "airline.iataCode"
    ]
  }

}

module.exports = {
  createPayload
}