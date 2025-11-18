// src/api/services/sec-gruposet-service.js

// Modelo Mongo
const EXECUTION = require('../models/mongodb/execution');

// Bitácora / respuesta
const { OK, FAIL, BITACORA, DATA, AddMSG } = require('../../middlewares/respPWA.handler');

// Modular DB handlers for gruposet (single mongo module)
const mongo = require('./gruposet/mongo');
const { handleUnsupported } = require('./common/unsupportedDb');

//const { connectToAzureCosmosDB } = require('../../config/connectToAzureCosmosDB');
//const dotenvXConfig = require('../../config/dotenvXConfig');

// ========================== Helpers ==========================
const today = () => new Date().toISOString().slice(0, 10);
const nowHHMMSS = () => new Date().toISOString().slice(11, 19);

async function crudExecute(req) {
  let bitacora = BITACORA();
  let data = DATA();

  // Manejar diferentes estructuras de req posibles en SAP CDS
  const query = (req.req?.query || req.query || req._.query || {});
  const { ProcessType, LoggedUser, DBServer } = query;

  console.log(ProcessType, LoggedUser, DBServer);
  
  
  const db = DBServer.toLowerCase();

  // Parámetros útiles para pasar a métodos locales
  const params = {
    paramsQuery : req.req.query || {},
    paramString : req.req.query ? new URLSearchParams(req.req.query).toString().trim() : '',
    body        : req.req.body || {}
  };

  bitacora.loggedUser = LoggedUser;
  bitacora.processType = ProcessType;
  bitacora.dbServer = DBServer;

  try {
    switch (ProcessType) {

      // ======== GETs → 200 ========
      case 'GetAll': {
        bitacora = await GetAllExecute(bitacora, params, db);
        if (!bitacora.success) { bitacora.finalRes = true; throw bitacora; }
        break;
      }

      // ======== POSTs → 201 ========
      case 'CreateExecute': {
        bitacora = await AddExecute(bitacora, params, db);
        if (!bitacora.success) { bitacora.finalRes = true; throw bitacora; }
        break;
      }

      case 'UpdateExecute':{
        bitacora = await UpdateExecute(bitacora, params, db);
        if (!bitacora.success) { bitacora.finalRes = true; throw bitacora; }
        break;
      }

      case 'DeleteExecute': {
        bitacora = await DeleteExecute(bitacora, params, db);
        if (!bitacora.success) { bitacora.finalRes = true; throw bitacora; }
        break;
      }

      default: {
        data.status = 400;
        data.messageUSR = 'Tipo de proceso inválido';
        data.messageDEV = `Proceso no reconocido: ${ProcessType}`;
        AddMSG(bitacora, data, 'FAIL');
        throw bitacora;
      }
    }

    // <<< AQUI fijamos el HTTP status real >>>
    req._.res.status(bitacora.status || 200);

    // Respuesta OK centralizada
    return OK(bitacora);

  } catch (errorBita) {
    // NO tumbar el servidor; siempre FALL controlado
    if (!errorBita?.finalRes) {
      data.status     = data.status || 500;
      data.messageDEV = data.messageDEV || errorBita.message;
      data.messageUSR = data.messageUSR || '<<ERROR CATCH>> El proceso no se completó';
      data.dataRes    = data.dataRes || errorBita;
      errorBita = AddMSG(bitacora, data, 'FAIL');
    }

    // Notificación OData con status correcto
    req.error({
      code: 'Internal-Server-Error',
      status: errorBita.status || 500,
      message: errorBita.messageUSR,
      target: errorBita.messageDEV,
      numericSeverity: 1,
      innererror: errorBita
    });

    return FAIL(errorBita);
  }
}

// GET (GetAll) → 200
async function GetAllExecute(bitacora, options = {}, db) {
  const { paramsQuery } = options;
  const data = DATA();
  data.processType = bitacora.processType;
  data.process = 'Lectura de ZTGRUPOSET';
 
  try {
  
    data.method  = 'GET';
    data.api     = '/crud?ProcessType=Get*';

    switch (db) {
      case 'mongodb': {
        
        const result = await EXECUTION.find().lean();
        console.log("El resultado es: "+ result);

        data.status = 200; // GET -> 200
        data.messageUSR = '<<OK>> La extracción <<SI>> tuvo éxito.';
        data.dataRes = result;
        bitacora = AddMSG(bitacora, data, 'OK', 200, true);
        bitacora.status = 200; // <-- refleja en bitácora para el dispatcher
        return OK(bitacora);
      }

      default: {
        // Reutilizar manejador común para DB no soportadas
        return handleUnsupported(bitacora, data, bitacora.dbServer).result;
      }
    }
  } catch (error) {
    data.status     = data.status || 500;
    data.messageDEV = data.messageDEV || error.message;
    data.messageUSR = data.messageUSR || '<<ERROR>> La extracción <<NO>> tuvo éxito.';
    data.dataRes    = data.dataRes || error;
    bitacora = AddMSG(bitacora, data, 'FAIL');
    return FAIL(bitacora);
  }
}

async function AddExecute(bitacora, options = {}, db) {
  const { body } = options;
  const data = DATA();
  data.process = 'Alta de Execution';

  try {
    data.processType = bitacora.processType;
    data.method  = 'POST';
    data.api     = '/crud?ProcessType=Create';

    if (db !== 'mongodb' && db !== 'azure') {
      return handleUnsupported(bitacora, data, bitacora.dbServer).result;
    }

    let payload = body?.data || body || null;

    if (!payload) {
      data.status = 400;
      data.messageUSR = 'Falta body.data';
      data.messageDEV = 'El payload debe venir en body.data';
      throw new Error(data.messageDEV);
    }

    const arr = Array.isArray(payload) ? payload : [payload];

    // construyes los documentos a insertar
    const newExDocs = arr.map(ex => ({
      EXEC_ID: ex.EXEC_ID,
      ORDER_ID: ex.ORDER_ID,
      TS: ex.TS || today,
      PRICE: ex.PRICE,
      QTY: ex.QTY,
      COMMISSION: ex.COMMISSION,
      PNL: ex.PNL
    }));

    switch (db) {
      case 'mongodb': {
        const inserted = await EXECUTION.insertMany(newExDocs);
        data.status = 201;
        data.messageUSR = '<<OK>> Alta realizada.';
        data.dataRes = JSON.parse(JSON.stringify(inserted));
        bitacora = AddMSG(bitacora, data, 'OK', 201, true);
        return OK(bitacora);
      }

      default: {
        return handleUnsupported(bitacora, data, bitacora.dbServer).result;
      }
    }

  } catch (error) {
    data.status     = data.status || 500;
    data.messageDEV = data.messageDEV || error.message;
    data.messageUSR = data.messageUSR || '<<ERROR>> Alta <<NO>> exitosa.';
    data.dataRes    = data.dataRes || error;
    bitacora = AddMSG(bitacora, data, 'FAIL');
    return FAIL(bitacora);
  }
}


async function UpdateExecute(bitacora, options = {}, db) {
  const {  body } = options;
  const data = DATA();
  data.process = 'Actualización de Execution';

  try {
    data.process = 'Actualización de Execution';
    data.processType = bitacora.processType;
    data.method  = 'POST';
    data.api     = '/crud?ProcessType=UpdateOne';

    if (db !== 'mongodb' && db !== 'azure') {
      return handleUnsupported(bitacora, data, bitacora.dbServer).result;
    }

    const changes = (body?.data && !Array.isArray(body.data)) ? body.data : body;

    if (!changes || typeof changes !== 'object') {
      data.status = 400;
      data.messageUSR = 'Falta body.data con cambios';
      data.messageDEV = 'Debe enviar un objeto con los campos a actualizar';
      throw new Error(data.messageDEV);
    }

    if(body.EXEC_ID === undefined){
      data.status = 400;
      data.messageUSR = `Falta parámetro de llave: EXEC_ID`;
      data.messageDEV = `Query.EXEC_ID es requerido`;
      throw new Error(data.messageDEV);
    }

    const upEx={};
    const id = body.EXEC_ID;
    if (body.ORDER_ID !== undefined) upEx.ORDER_ID = body.ORDER_ID;
    if (body.TS !== undefined) upEx.TS = body.TS;
    if (body.PRICE !== undefined) upEx.PRICE = body.PRICE;
    if (body.QTY !== undefined) upEx.QTY = body.QTY;
    if (body.COMMISSION !== undefined) upEx.COMMISSION = body.COMMISSION;
    if (body.PNL !== undefined) upEx.PNL = body.PNL;

    switch (db) {
      case 'mongodb': {
        const updated = await EXECUTION.findOneAndUpdate({EXEC_ID:id}, { $set: upEx}, { new: true, upsert: false });
        if (!updated) {
          data.status = 404;
          data.messageUSR = 'No se encontró registro a actualizar';
          data.messageDEV = 'findOneAndUpdate retornó null';
          throw new Error(data.messageDEV);
        }

        data.status = 201; // POST -> 201
        data.messageUSR = '<<OK>> Actualización realizada.';
        data.dataRes = JSON.parse(JSON.stringify(updated));
        bitacora = AddMSG(bitacora, data, 'OK', 201, true);
        return OK(bitacora);
      }

      default: {
        return handleUnsupported(bitacora, data, bitacora.dbServer).result;
      }
    }

  } catch (error) {
    data.status     = data.status || 500;
    data.messageDEV = data.messageDEV || error.message;
    data.messageUSR = data.messageUSR || '<<ERROR>> Actualización <<NO>> exitosa.';
    data.dataRes    = data.dataRes || error;
    bitacora = AddMSG(bitacora, data, 'FAIL');
    return FAIL(bitacora);
  }
}

async function DeleteExecute(bitacora, options = {}, db) {
  const {  body } = options;
  const data = DATA();
  data.processType = bitacora.processType;
  data.process = 'Borrado físico de Execution';

  try {
    data.method  = 'POST';
    data.api     = '/crud?ProcessType=DeleteHard';

    if (!body || Object.keys(body).length === 0) {
      data.status = 400;
      data.messageUSR = 'El cuerpo (body) está vacío.';
      data.messageDEV = 'No se recibieron datos en el body.';
      throw new Error(data.messageDEV);
    }

    if(body.EXEC_ID === undefined){
      data.status = 400;
      data.messageUSR = `Falta parámetro de llave: EXEC_ID`;
      data.messageDEV = `Query.EXEC_ID es requerido`;
      throw new Error(data.messageDEV);
    }

    switch (db) {
      case 'mongodb': {
        const deleted = await EXECUTION.findOneAndDelete({ EXEC_ID: body.EXEC_ID });
        if (!deleted) {
          data.status = 404;
          data.messageUSR = 'No se encontró registro a eliminar';
          data.messageDEV = 'findOneAndDelete retornó null';
          throw new Error(data.messageDEV);
        }

        data.status = 201; // POST -> 201
        data.messageUSR = '<<OK>> Borrado físico realizado.';
        data.dataRes = { message: 'Eliminado' };
        bitacora = AddMSG(bitacora, data, 'OK', 201, true);
        return OK(bitacora);
      }

      default: {
        return handleUnsupported(bitacora, data, bitacora.dbServer).result;
      }
    }

  } catch (error) {
    data.status     = data.status || 500;
    data.messageDEV = data.messageDEV || error.message;
    data.messageUSR = data.messageUSR || '<<ERROR>> Borrado físico <<NO>> exitoso.';
    data.dataRes    = data.dataRes || error;
    bitacora = AddMSG(bitacora, data, 'FAIL');
    return FAIL(bitacora);
  }
}

module.exports = {
  crudExecute,
};