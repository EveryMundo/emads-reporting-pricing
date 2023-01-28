'use strict'

const { MongoClient } = require('mongodb')

const logr = require('@everymundo/simple-logr')

let client, client_np
const activeConnections = {}
const npActiveConn = {}

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

const aggregateFareWireCustomizerItemsV2 = async (tenantCode, pipeline) => {
  try {
    // console.log(process.env.MONGO_URI)
    client = new MongoClient(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
    const db = await connect(client, tenantCode.toLowerCase() + "_search_engine_operations")
    const coll = db.collection('FareWireCustomizerItemsV2')
    return coll.aggregate(pipeline).toArray()
  } catch (e) {
    throw e
  }
}

const getAdGroupsV1 = async (tenantCode, query) => {
  try {
    // console.log(process.env.MONGO_URI)
    client_np = new MongoClient(process.env.MONGO_URI_NON_PRICING, { useNewUrlParser: true, useUnifiedTopology: true })
    const db = await connect(client_np, tenantCode.toLowerCase() + "_airsem_settings")
    const coll = db.collection('AdGroupsV1')
    return coll.find(query).toArray()
  } catch (e) {
    throw e
  }
}

const aggregateAdGroupsV1 = async (tenantCode, pipeline) => {
  try {
    // console.log(process.env.MONGO_URI)
    client_np = new MongoClient(process.env.MONGO_URI_NON_PRICING, { useNewUrlParser: true, useUnifiedTopology: true })
    const db = await connect(client_np, tenantCode.toLowerCase() + "_airsem_settings")
    const coll = db.collection('AdGroupsV1')
    return coll.aggregate(pipeline).toArray()
  } catch (e) {
    throw e
  }
}

module.exports = {
  connect,
  aggregateFareWireCustomizerItemsV2,
  getAdGroupsV1,
  aggregateAdGroupsV1
}