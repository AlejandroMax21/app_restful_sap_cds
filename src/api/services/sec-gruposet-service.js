// src/api/services/sec-gruposet-service.js

// Modelo Mongo
const ZTGRUPOSET = require('../models/mongodb/ztgruposet');

// Bitácora / respuesta
const { OK, FAIL, BITACORA, DATA, AddMSG } = require('../../middlewares/respPWA.handler');

// Modular DB handlers for gruposet (single mongo module)
const mongo = require('./gruposet/mongo');
const { handleUnsupported } = require('./common/unsupportedDb');

const { connectToAzureCosmosDB } = require('../../config/connectToAzureCosmosDB');
const dotenvXConfig = require('../../config/dotenvXConfig');

// ========================== Helpers ==========================
const today = () => new Date().toISOString().slice(0, 10);
const nowHHMMSS = () => new Date().toISOString().slice(11, 19);

// Construir filtro desde query o body para mongoDB
function buildFilter(q = {}) {
  const f = {};
  // 🟢 CORRECCIÓN: Usar conversión explícita
  if (q.IDSOCIEDAD != null) f.IDSOCIEDAD = q.IDSOCIEDAD; // No convertir aquí, se hará después
  if (q.IDCEDI     != null) f.IDCEDI     = q.IDCEDI;     // No convertir aquí, se hará después
  if (q.IDETIQUETA)         f.IDETIQUETA = String(q.IDETIQUETA);
  if (q.IDVALOR)            f.IDVALOR    = String(q.IDVALOR);
  if (q.IDGRUPOET)          f.IDGRUPOET  = String(q.IDGRUPOET);
  if (q.ID)                 f.ID         = String(q.ID);
  if (q.ACTIVO  !== undefined)  f.ACTIVO  = (q.ACTIVO  === 'true' || q.ACTIVO  === true);
  if (q.BORRADO !== undefined)  f.BORRADO = (q.BORRADO === 'true' || q.BORRADO === true);
  return f;
}

// Construir consulta SQL para Cosmos DB desde query o body
function buildCosmosQuery(q = {}) {
  let query = "SELECT * FROM c"; // 'c' es el alias estándar del contenedor
  const parameters = [];
  const conditions = [];

  // Mapea tus filtros a parámetros SQL para evitar inyección SQL
  if (q.IDSOCIEDAD != null) {
    conditions.push("c.IDSOCIEDAD = @IDSOCIEDAD");
    parameters.push({ name: "@IDSOCIEDAD", value: parseInt(q.IDSOCIEDAD) });
  }
  if (q.IDCEDI != null) {
    conditions.push("c.IDCEDI = @IDCEDI");
    parameters.push({ name: "@IDCEDI", value: parseInt(q.IDCEDI) });
  }
  if (q.IDETIQUETA) {
    conditions.push("c.IDETIQUETA = @IDETIQUETA");
    parameters.push({ name: "@IDETIQUETA", value: String(q.IDETIQUETA) });
  }

  // --- CAMPOS QUE FALTABAN ---
  if (q.IDVALOR) {
    conditions.push("c.IDVALOR = @IDVALOR");
    parameters.push({ name: "@IDVALOR", value: String(q.IDVALOR) });
  }
  if (q.IDGRUPOET) {
    conditions.push("c.IDGRUPOET = @IDGRUPOET");
    parameters.push({ name: "@IDGRUPOET", value: String(q.IDGRUPOET) });
  }
  if (q.ID) {
    // Usamos 'c.id' porque así se llama en Cosmos (minúscula)
    conditions.push("c.id = @ID");
    parameters.push({ name: "@ID", value: String(q.ID) });
  }
  if (q.ACTIVO !== undefined) {
    conditions.push("c.ACTIVO = @ACTIVO");
    parameters.push({ name: "@ACTIVO", value: (q.ACTIVO === 'true' || q.ACTIVO === true) });
  }
  if (q.BORRADO !== undefined) {
    conditions.push("c.BORRADO = @BORRADO");
    parameters.push({ name: "@BORRADO", value: (q.BORRADO === 'true' || q.BORRADO === true) });
  }
  // --- FIN DE CAMPOS QUE FALTABAN ---

  // Si hay condiciones, las une con 'AND'
  if (conditions.length > 0) {
    query += " WHERE " + conditions.join(" AND ");
  }

  return {
    query: query,
    parameters: parameters
  };
}

const hasFullKey = f =>
  f.IDSOCIEDAD != null && f.IDCEDI != null && f.IDETIQUETA && f.IDVALOR && f.IDGRUPOET && f.ID;

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
    paramsQuery: req.req.query || {},
    paramString: req.req.query ? new URLSearchParams(req.req.query).toString().trim() : '',
    body: req.req.body || {}
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
      data.status = data.status || 500;
      data.messageDEV = data.messageDEV || errorBita.message;
      data.messageUSR = data.messageUSR || '<<ERROR CATCH>> El proceso no se completó';
      data.dataRes = data.dataRes || errorBita;
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

    data.method = 'GET';
    data.api = '/crud?ProcessType=Get*';

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
        // 1. Obtener el contenedor
        const container = connectToAzureCosmosDB(dotenvXConfig.COSMOSDB_CONTAINER);

        // --- INICIO DE CORRECCIÓN ---
        // 2. Determinar el objeto de filtro real (puede estar anidado)
        //    Tu JSON es { "data": { "ID": "111" } }
        //    Pero buildCosmosQuery espera { "ID": "111" }
        const filterData = options.body?.data || options.body;

        // 3. CORRECCIÓN: Usar 'filterData' en lugar de 'options.body'
        const querySpec = buildCosmosQuery(filterData);
        // --- FIN DE CORRECCIÓN ---

        // 4. Ejecutar la consulta
        const { resources } = await container.items.query(querySpec).fetchAll();

        // 5. Manejar la respuesta (esto ya estaba bien)
        if (bitacora.processType === 'GetById') {
          data.dataRes = resources[0] || null;
          data.messageUSR = resources.length > 0 ? '<<OK>> Extracción exitosa.' : '<<OK>> No se encontró el registro.';
        } else {
          data.dataRes = resources;
          data.messageUSR = '<<OK>> Extracción de Azure Cosmos DB exitosa.';
        }

        data.status = 200;
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
    data.status = data.status || 500;
    data.messageDEV = data.messageDEV || error.message;
    data.messageUSR = data.messageUSR || '<<ERROR>> La extracción <<NO>> tuvo éxito.';
    data.dataRes = data.dataRes || error;
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
    data.method = 'POST';
    data.api = '/crud?ProcessType=Create';

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
      IDCEDI: parseInt(d.IDCEDI),
      FECHAREG: d.FECHAREG ?? today(),
      HORAREG: d.HORAREG ?? nowHHMMSS(),
      USUARIOREG: d.USUARIOREG ?? (bitacora.loggedUser || 'SYSTEM'),
      ACTIVO: d.ACTIVO ?? true,
      BORRADO: d.BORRADO ?? false
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
        // 1. Obtener el contenedor
        const container = connectToAzureCosmosDB(dotenvXConfig.COSMOSDB_CONTAINER);

        // 2. 'docs' ya es un array de objetos listos para insertar
        // (Asegúrate de que tus documentos tengan un campo 'id' único)
        const promises = docs.map(doc => {
          // Importante: Cosmos usa 'id' (minúscula) como identificador único.
          // Si tu 'ID' (mayúscula) es el ID único, mapéalo.
          if (doc.ID && !doc.id) {
            doc.id = doc.ID;
          }
          return container.items.create(doc);
        });

        // 3. Esperar a que todas las promesas de creación terminen
        const results = await Promise.all(promises);

        // 4. Extraer los items creados de la respuesta
        const createdItems = results.map(r => r.resource);

        data.status = 201;
        data.messageUSR = '<<OK>> Alta realizada en Azure Cosmos DB.';
        data.dataRes = createdItems;
        bitacora = AddMSG(bitacora, data, 'OK', 201, true);
        return OK(bitacora);
      }

      default: {
        return handleUnsupported(bitacora, data, bitacora.dbServer).result;
      }
    }

  } catch (error) {
    if (db === 'azure' && (error.code === 409 || (error.message && error.message.includes('Entity with the specified id already exists')))) {
      data.status = 409;
      data.messageUSR = 'Ya existe un registro con esos datos.';
      data.messageDEV = 'Azure Cosmos DB conflict (409): ' + error.message;
    } else if (db === 'mongodb' && error.code === 11000) {
      data.status = 409;
      data.messageUSR = 'Ya existe un registro con esos datos (llave duplicada).';
      data.messageDEV = 'MongoDB duplicate key error (11000): ' + error.message;
    } else {
      data.status = data.status || 500;
      data.messageDEV = data.messageDEV || error.message;
      data.messageUSR = data.messageUSR || '<<ERROR>> Alta <<NO>> exitosa.';
    }
    data.dataRes = data.dataRes || error;
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
    data.method = 'POST';
    data.api = '/crud?ProcessType=UpdateOne';

    if (db !== 'mongodb' && db !== 'azure') {
      return handleUnsupported(bitacora, data, bitacora.dbServer).result;
    }

    // 🟢 CORRECCIÓN: Normalizar tipos en el filtro original
    const originalFilter = buildFilter(body);
    // Convertir campos numéricos a number para comparación consistente
    if (originalFilter.IDSOCIEDAD != null) originalFilter.IDSOCIEDAD = parseInt(originalFilter.IDSOCIEDAD);
    if (originalFilter.IDCEDI != null) originalFilter.IDCEDI = parseInt(originalFilter.IDCEDI);

    const need = ['IDSOCIEDAD', 'IDCEDI', 'IDETIQUETA', 'IDVALOR', 'IDGRUPOET', 'ID'];
    for (const k of need) {
      if (originalFilter[k] === undefined || originalFilter[k] === null) {
        data.status = 400;
        data.messageUSR = `Falta parámetro de llave original: ${k}`;
        data.messageDEV = `Body.${k} es requerido para identificar el registro`;
        throw new Error(data.messageDEV);
      }
    }

    // 🟢 PASO 2: Obtener los cambios propuestos
    const changes = (body?.data && !Array.isArray(body.data)) ? body.data : {};
    if (!changes || typeof changes !== 'object') {
      data.status = 400;
      data.messageUSR = 'Falta body.data con cambios';
      data.messageDEV = 'Debe enviar un objeto con los campos a actualizar';
      throw new Error(data.messageDEV);
    }

    // 🟢 PASO 3: Construir la NUEVA llave compuesta (con los cambios propuestos)
    const newKey = {
      IDSOCIEDAD: changes.IDSOCIEDAD !== undefined ? parseInt(changes.IDSOCIEDAD) : originalFilter.IDSOCIEDAD,
      IDCEDI:     changes.IDCEDI     !== undefined ? parseInt(changes.IDCEDI)     : originalFilter.IDCEDI,
      IDETIQUETA: changes.IDETIQUETA !== undefined ? String(changes.IDETIQUETA)   : originalFilter.IDETIQUETA,
      IDVALOR:    changes.IDVALOR    !== undefined ? String(changes.IDVALOR)      : originalFilter.IDVALOR,
      IDGRUPOET:  changes.IDGRUPOET  !== undefined ? String(changes.IDGRUPOET)    : originalFilter.IDGRUPOET,
      ID:         changes.ID         !== undefined ? String(changes.ID)           : originalFilter.ID
    };

    // 🟢 PASO 4: Verificar si la nueva llave es DIFERENTE a la original
    const keyChanged =
      newKey.IDSOCIEDAD !== originalFilter.IDSOCIEDAD ||
      newKey.IDCEDI !== originalFilter.IDCEDI ||
      newKey.IDETIQUETA !== originalFilter.IDETIQUETA ||
      newKey.IDVALOR !== originalFilter.IDVALOR ||
      newKey.IDGRUPOET !== originalFilter.IDGRUPOET ||
      newKey.ID !== originalFilter.ID;

    // Agregar campos de auditoría
    changes.FECHAULTMOD = today();
    changes.HORAULTMOD = nowHHMMSS();
    changes.USUARIOMOD = bitacora.loggedUser || 'SYSTEM';

    switch (db) {
      case 'mongodb': {
        // 🟢 PASO 5: Si cambió la llave, verificar que NO exista duplicado
        if (keyChanged) {
          const duplicate = await mongo.findOne(newKey);
          if (duplicate) {
            data.status = 409; // Conflict
            data.messageUSR = 'Ya existe un registro con esos datos (llave duplicada)';
            data.messageDEV = `Duplicate key: ${JSON.stringify(newKey)}`;
            throw new Error(data.messageDEV);
          }
        }

        // 🟢 PASO 6: Actualizar el registro
        const updated = await mongo.update(originalFilter, changes);
        if (!updated) {
          data.status = 404;
          data.messageUSR = 'No se encontró registro a actualizar';
          data.messageDEV = 'findOneAndUpdate retornó null';
          throw new Error(data.messageDEV);
        }

        data.status = 201;
        data.messageUSR = '<<OK>> Actualización realizada.';
        data.dataRes = JSON.parse(JSON.stringify(updated));
        bitacora = AddMSG(bitacora, data, 'OK', 201, true);
        return OK(bitacora);
      }

      case 'azure': {
        const container = connectToAzureCosmosDB(dotenvXConfig.COSMOSDB_CONTAINER);

  const docId = originalFilter.ID;
  const partitionKey = originalFilter.ID;

  if (!docId || partitionKey === undefined) {
    throw new Error('Se requiere el "id" del documento y su "Partition Key".');
  }

  const { resource: currentItem } = await container.item(docId, partitionKey).read();

  if (!currentItem) {
    data.status = 404;
    data.messageUSR = 'No se encontró registro a actualizar';
    data.messageDEV = 'Item not found in Cosmos DB';
    throw new Error(data.messageDEV);
  }

  
  let validationErrors = [];
  for (const key in originalFilter) {
    if (originalFilter[key] !== undefined && originalFilter[key] !== null) {
      const dbValue = currentItem[key];
      const filterValue = originalFilter[key];

      
      // 🟢 COMPARACIÓN MÁS FLEXIBLE
      if (String(dbValue) !== String(filterValue)) {
        validationErrors.push(`${key}: DB='${dbValue}' vs Filter='${filterValue}'`);
        console.log(`   ❌ NO COINCIDEN`);
      } else {
        console.log(`   ✅ COINCIDEN`);
      }
    }
  }

  if (validationErrors.length > 0) {
    data.status = 404;
    data.messageUSR = 'No se encontró el registro con los criterios exactos.';
    data.messageDEV = `Falla de precondición: ${validationErrors.join(', ')}`;
    throw new Error(data.messageDEV);
  }

        // Si cambió la llave, verificar duplicados en Azure
        if (keyChanged) {
          const querySpec = buildCosmosQuery(newKey);
          const { resources } = await container.items.query(querySpec).fetchAll();
          
          if (resources.length > 0) {
            data.status = 409;
            data.messageUSR = 'Ya existe un registro con esos datos (llave duplicada)';
            data.messageDEV = `Duplicate key: ${JSON.stringify(newKey)}`;
            throw new Error(data.messageDEV);
          }
        }

        // 🟢 CORRECCIÓN: Aplicar cambios manteniendo tipos correctos
        const itemToUpdate = { 
          ...currentItem, 
          ...changes,
          // Asegurar que los campos numéricos se mantengan como números
          IDSOCIEDAD: changes.IDSOCIEDAD !== undefined ? parseInt(changes.IDSOCIEDAD) : currentItem.IDSOCIEDAD,
          IDCEDI: changes.IDCEDI !== undefined ? parseInt(changes.IDCEDI) : currentItem.IDCEDI
        };

        const { resource: updatedItem } = await container.item(docId, partitionKey).replace(itemToUpdate);

        data.status = 201;
        data.messageUSR = '<<OK>> Actualización realizada en Azure Cosmos DB.';
        data.dataRes = updatedItem;
        bitacora = AddMSG(bitacora, data, 'OK', 201, true);
        return OK(bitacora);
      }

      default: {
        return handleUnsupported(bitacora, data, bitacora.dbServer).result;
      }
    }

  } catch (error) {
    data.status = data.status || 500;
    data.messageDEV = data.messageDEV || error.message;
    data.messageUSR = data.messageUSR || '<<ERROR>> Actualización <<NO>> exitosa.';
    data.dataRes = data.dataRes || error;
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
    data.method = 'POST';
    data.api = '/crud?ProcessType=DeleteOne';

    if (!body || Object.keys(body).length === 0) {
      data.status = 400;
      data.messageUSR = 'El cuerpo (body) está vacío.';
      data.messageDEV = 'No se recibieron datos en el body.';
      throw new Error(data.messageDEV);
    }

    const filter = buildFilter(body);
    const need = ['IDSOCIEDAD', 'IDCEDI', 'IDETIQUETA', 'IDVALOR', 'IDGRUPOET', 'ID'];
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
          HORAULTMOD: nowHHMMSS(),
          USUARIOMOD: bitacora.loggedUser || 'SYSTEM'
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
        const container = connectToAzureCosmosDB(dotenvXConfig.COSMOSDB_CONTAINER);
        const filter = buildFilter(body);

        // 🟢 CORRECCIÓN: Usar ID como partition key
        const docId = filter.ID;
        const partitionKey = filter.ID; // Partition key = valor del campo ID

        if (!docId || partitionKey === undefined) {
          throw new Error('Para actualizar/borrar en Cosmos se requiere el "id" del documento y su "Partition Key".');
        }

        // 4. Leer el item actual
        const { resource: currentItem } = await container.item(docId, partitionKey).read();

        // 5. Manejar "No Encontrado"
        if (!currentItem) {
          data.status = 404;
          data.messageUSR = 'No se encontró registro para marcar como borrado';
          data.messageDEV = 'Item not found in Cosmos DB';
          throw new Error(data.messageDEV);
        }

        // 6. VALIDACIÓN ESTRICTA
        for (const key in filter) {
          if (currentItem[key] !== filter[key]) {
            data.status = 404;
            data.messageUSR = 'No se encontró el registro con los criterios exactos.';
            data.messageDEV = `Falla de precondición: El campo '${key}' no coincide. (DB: '${currentItem[key]}' vs Solicitud: '${filter[key]}')`;
            throw new Error(data.messageDEV);
          }
        }

        // 7. =========== LÓGICA DE TOGGLE (INTERRUPTOR) ===========
        // Lee el estado actual y lo invierte.
        // Si currentItem.ACTIVO era 'true', newState será 'false'.
        // Si currentItem.ACTIVO era 'false', newState será 'true'.
        const newState = !currentItem.ACTIVO;

        // Aplica los nuevos estados invertidos
        currentItem.ACTIVO = newState;
        currentItem.BORRADO = !newState; // El opuesto de ACTIVO
        currentItem.FECHAULTMOD = today();
        currentItem.HORAULTMOD = nowHHMMSS();
        currentItem.USUARIOMOD = bitacora.loggedUser || 'SYSTEM';
        // =========================================================

        // 8. Reemplazar el item con la versión modificada
        const { resource: updatedItem } = await container.item(docId, partitionKey).replace(currentItem);

        // 9. (Opcional) Mensaje de respuesta dinámico
        const message = newState ? '<<OK>> Registro Reactivado.' : '<<OK>> Borrado lógico realizado.';

        data.status = 201;
        data.messageUSR = message; // <-- Mensaje dinámico
        data.dataRes = updatedItem;
        bitacora = AddMSG(bitacora, data, 'OK', 201, true);
        return OK(bitacora);
      }

      default: {
        return handleUnsupported(bitacora, data, bitacora.dbServer).result;
      }
    }

  } catch (error) {
    data.status = data.status || 500;
    data.messageDEV = data.messageDEV || error.message;
    data.messageUSR = data.messageUSR || '<<ERROR>> Borrado lógico <<NO>> exitoso.';
    data.dataRes = data.dataRes || error;
    bitacora = AddMSG(bitacora, data, 'FAIL');
    return FAIL(bitacora);
  }
}

// DELETE físico → 201
async function DeleteHardGruposetMethod(bitacora, options = {}, db) {
  const { body } = options;
  const data = DATA();
  data.processType = bitacora.processType;
  data.process = 'Borrado físico de ZTGRUPOSET';

  try {
    data.method = 'POST';
    data.api = '/crud?ProcessType=DeleteHard';

    if (!body || Object.keys(body).length === 0) {
      data.status = 400;
      data.messageUSR = 'El cuerpo (body) está vacío.';
      data.messageDEV = 'No se recibieron datos en el body.';
      throw new Error(data.messageDEV);
    }

    const filter = buildFilter(body);
    const need = ['IDSOCIEDAD', 'IDCEDI', 'IDETIQUETA', 'IDVALOR', 'IDGRUPOET', 'ID'];
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
        const container = connectToAzureCosmosDB(dotenvXConfig.COSMOSDB_CONTAINER);
        const filter = buildFilter(body);

        // 🟢 CORRECCIÓN: Usar ID como partition key
        const docId = filter.ID;
        const partitionKey = filter.ID; // Partition key = valor del campo ID

        if (!docId || partitionKey === undefined) {
          throw new Error('Para borrar en Cosmos se requiere el "id" del documento y su "Partition Key".');
        }

        // 4. =========== NUEVO: VALIDACIÓN ESTRICTA ===========
        // Leemos el item PRIMERO para validarlo
        const { resource: currentItem } = await container.item(docId, partitionKey).read();

        // 5. Manejar "No Encontrado"
        if (!currentItem) {
          data.status = 404;
          data.messageUSR = 'No se encontró registro a eliminar';
          data.messageDEV = 'Item not found in Cosmos DB';
          throw new Error(data.messageDEV);
        }

        // 6. Comparación estricta
        for (const key in filter) {
          if (currentItem[key] !== filter[key]) {
            data.status = 404;
            data.messageUSR = 'No se encontró el registro con los criterios exactos.';
            data.messageDEV = `Falla de precondición: El campo '${key}' no coincide. (DB: '${currentItem[key]}' vs Solicitud: '${filter[key]}')`;
            throw new Error(data.messageDEV);
          }
        }
        // =======================================================

        // 7. Ejecutar borrado físico (AHORA SÍ es seguro)
        await container.item(docId, partitionKey).delete();

        // 8. Respuesta
        data.status = 201;
        data.messageUSR = '<<OK>> Borrado físico en Azure Cosmos DB.';
        data.dataRes = { message: 'Eliminado', id: docId, partitionKey: partitionKey };
        bitacora = AddMSG(bitacora, data, 'OK', 201, true);
        return OK(bitacora);
      }

      default: {
        return handleUnsupported(bitacora, data, bitacora.dbServer).result;
      }
    }

  } catch (error) {
    data.status = data.status || 500;
    data.messageDEV = data.messageDEV || error.message;
    data.messageUSR = data.messageUSR || '<<ERROR>> Borrado físico <<NO>> exitoso.';
    data.dataRes = data.dataRes || error;
    bitacora = AddMSG(bitacora, data, 'FAIL');
    return FAIL(bitacora);
  }
}

module.exports = {
  crudGruposet,
};