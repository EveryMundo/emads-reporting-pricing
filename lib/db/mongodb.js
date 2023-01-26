'use strict'

const { MongoClient } = require('mongodb')

const logr = require('@everymundo/simple-logr')

let client
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
  getAllSearchEngineDbs
}
