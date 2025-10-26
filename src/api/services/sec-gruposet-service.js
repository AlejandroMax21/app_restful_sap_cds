// src/api/services/sec-gruposet-service.js

// Modelo Mongo
const ZTGRUPOSET = require('../models/mongodb/ztgruposet');

// Bitácora / respuesta
const { OK, FAIL, BITACORA, DATA, AddMSG } = require('../../middlewares/respPWA.handler');

// Modular DB handlers for gruposet (single mongo module)
const mongo = require('./gruposet/mongo');
const { handleUnsupported } = require('./common/unsupportedDb');

// ========================== Helpers ==========================
const today = () => new Date().toISOString().slice(0, 10);
const nowHHMMSS = () => new Date().toISOString().slice(11, 19);

function buildFilter(q = {}) {
  const f = {};
  if (q.IDSOCIEDAD != null) f.IDSOCIEDAD = parseInt(q.IDSOCIEDAD);
  if (q.IDCEDI     != null) f.IDCEDI     = parseInt(q.IDCEDI);
  if (q.IDETIQUETA)         f.IDETIQUETA = String(q.IDETIQUETA);
  if (q.IDVALOR)            f.IDVALOR    = String(q.IDVALOR);
  if (q.IDGRUPOET)          f.IDGRUPOET  = String(q.IDGRUPOET);
  if (q.ID)                 f.ID         = String(q.ID);
  if (q.ACTIVO  !== undefined)  f.ACTIVO  = (q.ACTIVO  === 'true' || q.ACTIVO  === true);
  if (q.BORRADO !== undefined)  f.BORRADO = (q.BORRADO === 'true' || q.BORRADO === true);
  return f;
}
const hasFullKey = f =>
  f.IDSOCIEDAD!=null && f.IDCEDI!=null && f.IDETIQUETA && f.IDVALOR && f.IDGRUPOET && f.ID;

// ============================================================
//                    Dispatcher (acción CRUD)
// ============================================================
/**
 * Endpoint acción: POST /api/security/gruposet/crud
 * Query: ProcessType, LoggedUser, DBServer, (llaves...)
 * Body:  { data: {...} } | { data: [{...},{...}] }
 */
async function crudGruposet(req) {
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
      case 'GetById':
      case 'GetAll':
      case 'GetSome': {
        bitacora = await GetFiltersGruposetMethod(bitacora, params, db);
        if (!bitacora.success) { bitacora.finalRes = true; throw bitacora; }
        break;
      }

      // ======== POSTs → 201 ========
      case 'Create':
      case 'AddOne':
      case 'AddMany': {
        bitacora = await AddManyGruposetMethod(bitacora, params, db);
        if (!bitacora.success) { bitacora.finalRes = true; throw bitacora; }
        break;
      }

      case 'UpdateOne':
      case 'UpdateMany': {
        bitacora = await UpdateOneGruposetMethod(bitacora, params, db);
        if (!bitacora.success) { bitacora.finalRes = true; throw bitacora; }
        break;
      }

      case 'DeleteOne': {
        bitacora = await DeleteOneGruposetMethod(bitacora, params, db);
        if (!bitacora.success) { bitacora.finalRes = true; throw bitacora; }
        break;
      }

      case 'DeleteHard': {
        bitacora = await DeleteHardGruposetMethod(bitacora, params, db);
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

// ============================================================
//                    MÉTODOS LOCALES
// ============================================================

// GET (GetById / GetSome / GetAll) → 200
async function GetFiltersGruposetMethod(bitacora, options = {}, db) {
  const { paramsQuery } = options;
  const data = DATA();
  data.processType = bitacora.processType;
  data.process = 'Lectura de ZTGRUPOSET';
 
  try {
  
    data.method  = 'GET';
    data.api     = '/crud?ProcessType=Get*';

    switch (db) {
      case 'mongodb': {
        // Delegar la lectura a módulo Mongo
        const result = await mongo.get(paramsQuery, options.body);

        data.status = 200; // GET -> 200
        data.messageUSR = '<<OK>> La extracción <<SI>> tuvo éxito.';
        data.dataRes = result;
        bitacora = AddMSG(bitacora, data, 'OK', 200, true);
        bitacora.status = 200; // <-- refleja en bitácora para el dispatcher
        return OK(bitacora);
      }

      case 'azure': {
        // Simulación: Azure aún no implementada. Devolver OK en bitácora con mensaje aclaratorio.
        data.status = 200;
        data.messageUSR = '<<OK>> Simulación de lectura en Azure (DB no implementada).';
        data.dataRes = [];
        bitacora = AddMSG(bitacora, data, 'OK', 200, true);
        bitacora.status = 200;
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

// CREATE (uno o varios) → 201
async function AddManyGruposetMethod(bitacora, options = {}, db) {
  const { body } = options;
  const data = DATA();
  data.process = 'Alta de ZTGRUPOSET';

  try {
    data.process = 'Alta de ZTGRUPOSET';
    data.processType = bitacora.processType;
    data.method  = 'POST';
    data.api     = '/crud?ProcessType=Create';

    if (db !== 'mongodb' && db !== 'azure') {
      return handleUnsupported(bitacora, data, bitacora.dbServer).result;
    }

    let payload =
      body?.data ||        // formato CAP: { data: {...} }
      body?.gruposet ||    // formato Express clásico
      body || null; 

    if (!payload) {
      data.status = 400;
      data.messageUSR = 'Falta body.data';
      data.messageDEV = 'El payload debe venir en body.data';
      throw new Error(data.messageDEV);
    }

    const arr = Array.isArray(payload) ? payload : [payload];

    const docs = arr.map(d => ({
      ...d,
      IDSOCIEDAD: parseInt(d.IDSOCIEDAD),
      IDCEDI:     parseInt(d.IDCEDI),
      FECHAREG:   d.FECHAREG   ?? today(),
      HORAREG:    d.HORAREG    ?? nowHHMMSS(),
      USUARIOREG: d.USUARIOREG ?? (bitacora.loggedUser || 'SYSTEM'),
      ACTIVO:     d.ACTIVO     ?? true,
      BORRADO:    d.BORRADO    ?? false
    }));

    switch (db) {
      case 'mongodb': {
        const inserted = await mongo.create(docs);

        data.status = 201; // POST -> 201
        data.messageUSR = '<<OK>> Alta realizada.';
        data.dataRes = JSON.parse(JSON.stringify(inserted));
        bitacora = AddMSG(bitacora, data, 'OK', 201, true);
        return OK(bitacora);
      }

      case 'azure': {
        // Simulación de creación en Azure
        data.status = 201;
        data.messageUSR = '<<OK>> Simulación de alta en Azure (DB no implementada).';
        data.dataRes = { insertedCount: docs.length };
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

// UPDATE por llave compuesta (body parcial) → 201
async function UpdateOneGruposetMethod(bitacora, options = {}, db) {
  const { paramsQuery, body } = options;
  const data = DATA();
  data.process = 'Actualización de ZTGRUPOSET';

  try {
    data.process = 'Actualización de ZTGRUPOSET';
    data.processType = bitacora.processType;
    data.method  = 'POST';
    data.api     = '/crud?ProcessType=UpdateOne';

    if (db !== 'mongodb' && db !== 'azure') {
      return handleUnsupported(bitacora, data, bitacora.dbServer).result;
    }

    const filter = buildFilter(body);
    const need = ['IDSOCIEDAD','IDCEDI','IDETIQUETA','IDVALOR','IDGRUPOET','ID'];
    for (const k of need) if (!filter[k]) {
      data.status = 400;
      data.messageUSR = `Falta parámetro de llave: ${k}`;
      data.messageDEV = `Query.${k} es requerido`;
      throw new Error(data.messageDEV);
    }

    const changes = (body?.data && !Array.isArray(body.data)) ? body.data : body;
    if (!changes || typeof changes !== 'object') {
      data.status = 400;
      data.messageUSR = 'Falta body.data con cambios';
      data.messageDEV = 'Debe enviar un objeto con los campos a actualizar';
      throw new Error(data.messageDEV);
    }

    changes.FECHAULTMOD = today();
    changes.HORAULTMOD  = nowHHMMSS();
    changes.USUARIOMOD  = bitacora.loggedUser || 'SYSTEM';

    switch (db) {
      case 'mongodb': {
        const updated = await mongo.update(filter, changes);
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

      case 'azure': {
        // Simulación de actualización en Azure
        data.status = 201;
        data.messageUSR = '<<OK>> Simulación de actualización en Azure (DB no implementada).';
        data.dataRes = { message: 'Simulated update' };
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

// DELETE lógico → 201
async function DeleteOneGruposetMethod(bitacora, options = {}, db) {
  const { body } = options;
  const data = DATA();
  data.process = 'Borrado lógico de ZTGRUPOSET';
   
  try {
    data.process = 'Borrado lógico de ZTGRUPOSET';
    data.processType = bitacora.processType;
    data.method  = 'POST';
    data.api     = '/crud?ProcessType=DeleteOne';

    if (!body || Object.keys(body).length === 0) {
      data.status = 400;
      data.messageUSR = 'El cuerpo (body) está vacío.';
      data.messageDEV = 'No se recibieron datos en el body.';
      throw new Error(data.messageDEV);
    }

    const filter = buildFilter(body);
    const need = ['IDSOCIEDAD','IDCEDI','IDETIQUETA','IDVALOR','IDGRUPOET','ID'];
    for (const k of need) if (!body[k]) {
      data.status = 400;
      data.messageUSR = `Falta parámetro de llave: ${k}`;
      data.messageDEV = `Query.${k} es requerido`;
      throw new Error(data.messageDEV);
    }

    switch (db) {
      case 'mongodb': {
        const updates = {
          ACTIVO: false,
          BORRADO: true,
          FECHAULTMOD: today(),
          HORAULTMOD:  nowHHMMSS(),
          USUARIOMOD:  bitacora.loggedUser || 'SYSTEM'
        };
        const updated = await mongo.logicalDelete(filter, updates);

        if (!updated) {
          data.status = 404;
          data.messageUSR = 'No se encontró registro para marcar como borrado';
          data.messageDEV = 'findOneAndUpdate retornó null';
          throw new Error(data.messageDEV);
        }

        data.status = 201; // POST -> 201
        data.messageUSR = '<<OK>> Borrado lógico realizado.';
        data.dataRes = JSON.parse(JSON.stringify(updated));
        bitacora = AddMSG(bitacora, data, 'OK', 201, true);
        return OK(bitacora);
      }

      case 'azure': {
        // Simulación de borrado lógico en Azure
        data.status = 201;
        data.messageUSR = '<<OK>> Simulación de borrado lógico en Azure (DB no implementada).';
        data.dataRes = { message: 'Simulated logical delete' };
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
    data.messageUSR = data.messageUSR || '<<ERROR>> Borrado lógico <<NO>> exitoso.';
    data.dataRes    = data.dataRes || error;
    bitacora = AddMSG(bitacora, data, 'FAIL');
    return FAIL(bitacora);
  }
}

// DELETE físico → 201
async function DeleteHardGruposetMethod(bitacora, options = {}, db) {
  const {  body } = options;
  const data = DATA();
  data.processType = bitacora.processType;
  data.process = 'Borrado físico de ZTGRUPOSET';

  try {
    data.method  = 'POST';
    data.api     = '/crud?ProcessType=DeleteHard';

    if (!body || Object.keys(body).length === 0) {
      data.status = 400;
      data.messageUSR = 'El cuerpo (body) está vacío.';
      data.messageDEV = 'No se recibieron datos en el body.';
      throw new Error(data.messageDEV);
    }

    const filter = buildFilter(body);
    const need = ['IDSOCIEDAD','IDCEDI','IDETIQUETA','IDVALOR','IDGRUPOET','ID'];
    for (const k of need) if (!body[k]) {
      data.status = 400;
      data.messageUSR = `Falta parámetro de llave: ${k}`;
      data.messageDEV = `Query.${k} es requerido`;
      throw new Error(data.messageDEV);
    }

    switch (db) {
      case 'mongodb': {
        const deleted = await mongo.hardDelete(filter);
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

      case 'azure': {
        // Simulación de borrado físico en Azure
        data.status = 201;
        data.messageUSR = '<<OK>> Simulación de borrado físico en Azure (DB no implementada).';
        data.dataRes = { message: 'Simulated hard delete' };
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
  crudGruposet,
};
