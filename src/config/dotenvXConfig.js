const dotenvx = require('@dotenvx/dotenvx');
dotenvx.config();

module.exports = {
  HOST: 'localhost' || 'NO ENCONTRE VARIABLE DE ENTORNO',
  PORT: 3333 || 'NO ENCONTRE PORT',
  API_URL: '/api/v1' || '/api/v1',


  CONNECTION_STRING: process.env.MONGODB_CONNECTION_STRING || 'SIN Cadena de CONEXION A LA BD MONGO',
  DATABASE: process.env.MONGODB_DATABASE || 'db_default',
  MONGODB_USER: process.env.MONGODB_USER || 'admin',
  MONGODB_PASSWORD: process.env.MONGODB_PASSWORD || 'admin',


  // Variables de entorno para Azure Cosmos DB
  COSMOSDB_ENDPOINT: process.env.COSMOSDB_ENDPOINT || 'NO SE ENCONTRO VARIABLE DE ENTORNO',
  COSMOSDB_KEY: process.env.COSMOSDB_KEY || 'NO SE ENCONTRO VARIABLE DE ENTORNO',
  COSMOSDB_DATABASE: process.env.COSMOSDB_DATABASE || 'NO SE ENCONTRO VARIABLE DE ENTORNO',
  COSMOSDB_CONTAINER: process.env.COSMOSDB_CONTAINER || 'NO SE ENCONTRO VARIABLE DE ENTORNO',
}
