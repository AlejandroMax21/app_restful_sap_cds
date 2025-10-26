// src/api/services/common/unsupportedDb.js
const { AddMSG, FAIL } = require('../../../middlewares/respPWA.handler');

function handleUnsupported(bitacora, data, db) {
  data.status = 400;
  data.messageUSR = 'DB no soportada';
  data.messageDEV = `DBServer no soportado: ${db}`;
  bitacora = AddMSG(bitacora, data, 'FAIL');
  return { bitacora, result: FAIL(bitacora) };
}

module.exports = { handleUnsupported };
