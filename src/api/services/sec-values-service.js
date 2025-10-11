// src/api/services/sec-values-service.js
// Ajusta la ruta si tu modelo está en otra carpeta
const ZTVALUES = require('../models/mongodb/security/ztvalues');

// Helpers
const today = () => new Date().toISOString().slice(0, 10);
const nowHHMMSS = () => new Date().toISOString().slice(11, 19);

// Filtros básicos por query
function buildFilter(q = {}) {
  const f = {};
  if (q.VALUEID)   f.VALUEID   = String(q.VALUEID);
  if (q.LABELID)   f.LABELID   = String(q.LABELID);
  if (q.COMPANYID) f.COMPANYID = String(q.COMPANYID);
  if (q.ALIAS)     f.ALIAS     = String(q.ALIAS);
  return f;
}

/** GET: lista o por filtros */
async function GetAllValues(req) {
  try {
    const q = req.req?.query || {};
    const filter = buildFilter(q);
    const rows = await ZTVALUES.find(Object.keys(filter).length ? filter : {}).lean();
    return rows;
  } catch (error) {
    return { error: error.message };
  }
}

/** POST: inserta 1 o varios */
async function AddOneValue(req) {
  try {
    const payload = req.req?.body?.values || req.req?.body || [];
    const arr = Array.isArray(payload) ? payload : [payload];

    const docs = arr.map(d => ({
      ...d,
      FECHAREG:   d.FECHAREG   ?? today(),
      HORAREG:    d.HORAREG    ?? nowHHMMSS(),
      USUARIOREG: d.USUARIOREG ?? 'SYSTEM',
    }));

    const inserted = await ZTVALUES.insertMany(docs, { ordered: true });
    return JSON.parse(JSON.stringify(inserted));
  } catch (error) {
    return { error: error.message };
  }
}

/** POST: update por VALUEID (en query o en body) */
async function UpdateOneValue(req) {
  try {
    const body = req.req?.body?.values || req.req?.body || {};
    const id = req.req?.query?.VALUEID || body.VALUEID;
    if (!id) throw new Error("Falta 'VALUEID' para actualizar");

    const data = {
      ...body,
      FECHAULTMOD: today(),
      HORAULTMOD:  nowHHMMSS(),
      USUARIOMOD:  body.USUARIOMOD || 'SYSTEM',
    };

    const updated = await ZTVALUES.findOneAndUpdate({ VALUEID: String(id) }, data, { new: true, upsert: false });
    if (!updated) throw new Error(`No se encontró VALUEID=${id}`);

    return { message: 'Registro actualizado.', values: JSON.parse(JSON.stringify(updated)) };
  } catch (error) {
    return { error: error.message };
  }
}

/** POST: borrado lógico por VALUEID */
async function DeleteOneValue(req) {
  try {
    const id = req.req?.query?.VALUEID;
    if (!id) throw new Error("Falta 'VALUEID' para borrado lógico");

    const updated = await ZTVALUES.findOneAndUpdate(
      { VALUEID: String(id) },
      { DELETED: true, FECHAULTMOD: today(), HORAULTMOD: nowHHMMSS(), USUARIOMOD: 'SYSTEM' },
      { new: true }
    );
    if (!updated) throw new Error(`No se encontró VALUEID=${id}`);

    return { message: 'Registro marcado como DELETED.', values: JSON.parse(JSON.stringify(updated)) };
  } catch (error) {
    return { error: error.message };
  }
}

module.exports = {
  GetAllValues,
  AddOneValue,
  UpdateOneValue,
  DeleteOneValue
};