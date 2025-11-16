// src/api/services/gruposet/mongo.js
// Consolidated MongoDB operations for ZTGRUPOSET.

const { data } = require('@sap/cds/lib/dbs/cds-deploy');
const ZTGRUPOSET = require('../../models/mongodb/ztgruposet');

// Local helper: build filter from query/body
function buildFilter(q = {}) {
    const f = {};
    if (q.IDSOCIEDAD != null) f.IDSOCIEDAD = parseInt(q.IDSOCIEDAD);
    if (q.IDCEDI != null) f.IDCEDI = parseInt(q.IDCEDI);
    if (q.IDETIQUETA) f.IDETIQUETA = String(q.IDETIQUETA);
    if (q.IDVALOR) f.IDVALOR = String(q.IDVALOR);
    if (q.IDGRUPOET) f.IDGRUPOET = String(q.IDGRUPOET);
    if (q.ID) f.ID = String(q.ID);
    if (q.ACTIVO !== undefined) f.ACTIVO = (q.ACTIVO === 'true' || q.ACTIVO === true);
    if (q.BORRADO !== undefined) f.BORRADO = (q.BORRADO === 'true' || q.BORRADO === true);
    return f;
}

const hasFullKey = f =>
    f.IDSOCIEDAD != null && f.IDCEDI != null && f.IDETIQUETA && f.IDVALOR && f.IDGRUPOET && f.ID;


// GET operations (GetById, GetSome, GetAll)
async function get(paramsQuery = {}, body = {}) {
    const PT = (paramsQuery?.ProcessType || '').toLowerCase();
    if (PT === 'getbyid') {
        const id = body?.data?.ID ?? body?.ID;
        if (!id) {
            data.status = 400;
            data.messageUSR = 'Falta parámetro ID';
            data.messageDEV = 'Query.ID es requerido para GetById';
            throw new Error(data.messageDEV);
        };
        const result = await ZTGRUPOSET.findOne({ ID: String(id) }).lean();
        if (!result) {
            data.status = 404;
            data.messageUSR = 'No se encontró registro';
            data.messageDEV = `ZTGRUPOSET con ID='${id}' no existe`;
            throw new Error(data.messageDEV);
        }
        return result;
    }

    if (PT === 'getsome') {
        const filter = buildFilter(paramsQuery);
        const result = await ZTGRUPOSET.find(filter).lean();
        return result;
    }

    // GetAll
    return await ZTGRUPOSET.find().lean();
}

// CREATE
async function create(docs = []) {
    const inserted = await ZTGRUPOSET.insertMany(docs, { ordered: true });
    return inserted;
}

// UPDATE (findOneAndUpdate)
async function update(filter = {}, changes = {}) {
    const updated = await ZTGRUPOSET.findOneAndUpdate(filter, changes, { new: true, upsert: false });
    return updated;
}

// LOGICAL DELETE (mark as borrado)
async function logicalDelete(filter = {}, updates = {}) {
    const updated = await ZTGRUPOSET.findOneAndUpdate(filter, updates, { new: true });
    return updated;
}

// HARD DELETE
async function hardDelete(filter = {}) {
    const deleted = await ZTGRUPOSET.findOneAndDelete(filter);
    return deleted;
}

// Buscar UN registro por filtro
async function findOne(filter) {
    try {
        const doc = await ZTGRUPOSET.findOne(filter).lean();
        return doc;
    } catch (error) {
        console.error('Error en findOne:', error);
        throw error;
    }
}

module.exports = {
    get,
    create,
    update,
    logicalDelete,
    hardDelete,
    buildFilter, // export in case other modules expect it
    findOne
};
