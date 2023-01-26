'use strict'

const { MongoClient } = require('mongodb')

const logr = require('@everymundo/simple-logr')

let client
const routeItemsCollection = {}
const routeItemsRemarketingCollection = {}
const reportCollection = {}
const reportErrorsCollection = {}
const activeConnections = {}

const connect = (client, db) => new Promise((resolve, reject) => {
  client.connect(error => {
    if (error) {
      logr.debug(error)
      return reject(error)
    }
    if (!activeConnections[db]) {
      logr.debug('Connecting to', db, 'Database')
      activeConnections[db] = client.db(db)
    }
    resolve(activeConnections[db])
  })
})

const findRouteItems = async (tenantPrefix, routeIdentifier) => {
  try {
    console.log(process.env.MONGO_URI)
    if (!routeItemsCollection[tenantPrefix]) {
      client = new MongoClient(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
      const db = await connect(client, tenantPrefix.toLowerCase() + "_dpi")
      routeItemsCollection[tenantPrefix] = db.collection('RouteItemsV1')
    }
    const query = {
      'routeIdentifier.airline': routeIdentifier.airline.toUpperCase(),
      'routeIdentifier.o': routeIdentifier.o.toUpperCase(),
      'routeIdentifier.d': routeIdentifier.d.toUpperCase(),
      'routeIdentifier.curC': routeIdentifier.curC.toUpperCase(),
      'routeIdentifier.bound': routeIdentifier.bound.toUpperCase(),
      'feedId': { '$exists': true }
      // 'maxPrice': { '$gte': price || 0 }
    }
    const cursor = routeItemsCollection[tenantPrefix].find(query)
    return cursor.toArray()
  } catch (e) {
    logr.info(e)
    throw e
  }
}

const findRouteItemsRemarketing = async (tenantPrefix, routeIdentifier) => {
  try {
    console.log(process.env.MONGO_URI)
    if (!routeItemsRemarketingCollection[tenantPrefix]) {
      client = new MongoClient(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
      const db = await connect(client, tenantPrefix.toLowerCase() + "_dpi")
      routeItemsRemarketingCollection[tenantPrefix] = db.collection('DynamicRemarketingRouteItemsV1')
    }
    const query = {
      'routeIdentifier.airline': routeIdentifier.airline.toUpperCase(),
      'routeIdentifier.o': routeIdentifier.o.toUpperCase(),
      'routeIdentifier.d': routeIdentifier.d.toUpperCase(),
      'routeIdentifier.curC': routeIdentifier.curC.toUpperCase(),
      'routeIdentifier.bound': routeIdentifier.bound.toUpperCase(),
      'feedId': { '$exists': true }
      // 'maxPrice': { '$gte': price || 0 }
    }
    const cursor = routeItemsRemarketingCollection[tenantPrefix].find(query)
    return cursor.toArray()
  } catch (e) {
    logr.info(e)
    throw e
  }
}
const saveRouteItemsPrices = async (tenantPrefix, routeIdentifier, price, updatedAt) => {
  try {
    console.log(process.env.MONGO_URI)
    if (!routeItemsCollection[tenantPrefix]) {
      client = new MongoClient(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
      const db = await connect(client, tenantPrefix.toLowerCase() + "_dpi")
      routeItemsCollection[tenantPrefix] = db.collection('RouteItemsV1')
    }
    const query = {
      'routeIdentifier.airline': routeIdentifier.airline.toUpperCase(),
      'routeIdentifier.o': routeIdentifier.o.toUpperCase(),
      'routeIdentifier.d': routeIdentifier.d.toUpperCase(),
      'routeIdentifier.curC': routeIdentifier.curC.toUpperCase(),
      'routeIdentifier.bound': routeIdentifier.bound.toUpperCase()
    }
    return routeItemsCollection[tenantPrefix].updateMany(query, { $set: { price, updatedAt } })
  } catch (e) {
    logr.info(e)
    throw e
  }
}

const saveRouteDRItemsPrices = async (tenantPrefix, routeIdentifier, price, updatedAt) => {
  try {
    console.log(process.env.MONGO_URI)
    if (!routeItemsRemarketingCollection[tenantPrefix]) {
      client = new MongoClient(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
      const db = await connect(client, tenantPrefix.toLowerCase() + "_dpi")
      routeItemsRemarketingCollection[tenantPrefix] = db.collection('DynamicRemarketingRouteItemsV1')
    }
    const query = {
      'routeIdentifier.airline': routeIdentifier.airline.toUpperCase(),
      'routeIdentifier.o': routeIdentifier.o.toUpperCase(),
      'routeIdentifier.d': routeIdentifier.d.toUpperCase(),
      'routeIdentifier.curC': routeIdentifier.curC.toUpperCase(),
      'routeIdentifier.bound': routeIdentifier.bound.toUpperCase()
    }
    return routeItemsRemarketingCollection[tenantPrefix].updateMany(query, { $set: { price, updatedAt } })
  } catch (e) {
    logr.info(e)
    throw e
  }
}
const saveUpdates = async (tenantPrefix, document) => {
  try {
    if (!reportCollection[tenantPrefix]) {
      client = new MongoClient(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
      const db = await connect(client, 'dpi_kinesis_google')
      reportCollection[tenantPrefix] = db.collection('ApiUpdates' + tenantPrefix)
    }
    document.updatedAt = new Date()
    return reportCollection[tenantPrefix].updateOne({ _id: tenantPrefix }, { $set: document }, { upsert: true })
  } catch (e) {
    logr.error(e)
    return e
  }
}

const saveApiErrors = async (document) => {
  try {
    if (!reportErrorsCollection['global']) {
      client = new MongoClient(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
      const db = await connect(client, 'GoogleApiErrors')
      reportErrorsCollection['global'] = db.collection('RemarketingErrorsLocal')
    }
    return reportErrorsCollection['global'].insertOne(document)
  } catch (e) {
    logr.error(e)
    return e
  }
}

const getAllSearchEngineDbs = async () => {
	const client = new MongoClient(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
	const clientConnection = await client.connect();

	const admin = await clientConnection.db().admin();
	const { databases } = await admin.listDatabases();

	return databases
		.filter(({ name }) => /[a-z0-9]_search_engine_operations/.test(name))
		.map((_) => _.name.split('_')[0]);
};

module.exports = {
  connect,
  findRouteItems,
  findRouteItemsRemarketing,
  saveRouteItemsPrices,
  saveRouteDRItemsPrices,
  saveUpdates,
  saveApiErrors,
  getAllSearchEngineDbs
}
