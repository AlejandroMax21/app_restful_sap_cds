const express = require('express');
const cds = require('@sap/cds');
const cors = require('cors');
const router = express.Router();
const dotenvXConfig = require('./src/config/dotenvXConfig');
const mongoose = require('./src/config/connectToMongoDB');
require('./src/api/models/mongodb/security/ztlabels');
const errorHandler = require('./src/middlewares/errorHandler'); 

module.exports = async (o) => {
  try {
    let app = express();
    app.express = express;

    // Captura JSON inválido ANTES que llegue a las rutas
    app.use(express.json({ limit: '500kb' }));
    app.use(cors());

    // Rutas
    app.use('/api', router);

    // Middleware de errores al final
    app.use(errorHandler);

    // Arranque del servidor CAP
    o.app = app;
    o.app.httpServer = await cds.server(o);

    return o.app.httpServer;

  } catch (error) {
    console.error('Error starting server', error);
    process.exit(1);
  }
};
