// src/api/services/sec-gruposet-service.js
// Modelo Mongoose (ajusta la ruta si fuera necesario)
const ZTGRUPOSET = require('../models/mongodb/ztgruposet');

// ==== Helpers ====
const today = () => new Date().toISOString().slice(0, 10);      // YYYY-MM-DD
const nowHHMMSS = () => new Date().toISOString().slice(11, 19);  // HH:MM:SS
const userOf = (req) => req?.user?.id || 'SYSTEM';

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
const hasFullKey = (f) =>
  f.IDSOCIEDAD!=null && f.IDCEDI!=null && f.IDETIQUETA && f.IDVALOR && f.IDGRUPOET && f.ID;

// ==== Services ====

// GET: lista o detalle por clave (siempre array)
async function GetAllGrupoSet(req) {
  try {
    const q = req.req?.query || {};
    const filter = buildFilter(q);

    if (hasFullKey(filter)) {
      const one = await ZTGRUPOSET.findOne(filter).lean();
      return one ? [one] : [];
    }

    const many = await ZTGRUPOSET.find(Object.keys(filter).length ? filter : {}).lean();
    return many;
  } catch (error) {
    return { error: error.message };
  }
}

async function GetByIdGrupoSet(req) {
  try {
    // OData -> req.data.ID ; alternativa query/params por si llaman como REST
    const id =
      req.data?.ID ??
      req.req?.query?.ID ??
      req.req?.params?.id ??
      null;

    if (!id) return { error: "Falta 'ID'" };

    const doc = await ZTGRUPOSET.findOne({ ID: String(id) }).lean();
    if (!doc) return { error: 'No se encontró registro' };

    return doc;
  } catch (error) {
    return { error: error.message };
  }
}


// POST: insertar 1..n
async function AddOneGrupoSet(req) {
  try {
    const payload = req.req?.body?.gruposet || req.req?.body || [];
    const arr = Array.isArray(payload) ? payload : [payload];
    const user = userOf(req.req);

    const docs = arr.map(d => ({
      ...d,
      IDSOCIEDAD: parseInt(d.IDSOCIEDAD),
      IDCEDI:     parseInt(d.IDCEDI),
      FECHAREG:   d.FECHAREG   ?? today(),
      HORAREG:    d.HORAREG    ?? nowHHMMSS(),
      USUARIOREG: d.USUARIOREG ?? user,
      ACTIVO:     d.ACTIVO     ?? true,
      BORRADO:    d.BORRADO    ?? false
    }));

    const inserted = await ZTGRUPOSET.insertMany(docs, { ordered: true });
    return JSON.parse(JSON.stringify(inserted));
  } catch (error) {
    return { error: error.message };
  }
}

// POST: actualizar por clave compuesta (body parcial)
async function UpdateOneGrupoSet(req) {
  try {
    const q = buildFilter(req.req?.query || {});
    const need = ['IDSOCIEDAD','IDCEDI','IDETIQUETA','IDVALOR','IDGRUPOET','ID'];
    for (const k of need) if (!q[k]) throw new Error(`Falta clave: ${k}`);

    const data = req.req?.body?.gruposet || req.req?.body || {};
    data.FECHAULTMOD = today();
    data.HORAULTMOD  = nowHHMMSS();
    data.USUARIOMOD  = userOf(req.req);

    const updated = await ZTGRUPOSET.findOneAndUpdate(q, data, { new: true, upsert: false });
    if (!updated) throw new Error('No se encontró el registro a actualizar.');

    return {
      message: 'Registro actualizado correctamente.',
      gruposet: JSON.parse(JSON.stringify(updated))
    };
  } catch (error) {
    return { error: error.message };
  }
}

// POST: borrado lógico
async function DeleteOneGrupoSet(req) {
  try {
    const q = buildFilter(req.req?.query || {});
    const need = ['IDSOCIEDAD','IDCEDI','IDETIQUETA','IDVALOR','IDGRUPOET','ID'];
    for (const k of need) if (!q[k]) throw new Error(`Falta clave: ${k}`);

    const updated = await ZTGRUPOSET.findOneAndUpdate(
      q,
      {
        ACTIVO: false,
        BORRADO: true,
        FECHAULTMOD: today(),
        HORAULTMOD:  nowHHMMSS(),
        USUARIOMOD:  userOf(req.req)
      },
      { new: true }
    );
    if (!updated) throw new Error('No se encontró el registro para marcar como BORRADO.');

    return {
      message: 'Registro marcado como BORRADO.',
      gruposet: JSON.parse(JSON.stringify(updated))
    };
  } catch (error) {
    return { error: error.message };
  }
}

// POST: borrado físico (opcional)
async function DeleteHardGrupoSet(req) {
  try {
    const q = buildFilter(req.req?.query || {});
    const need = ['IDSOCIEDAD','IDCEDI','IDETIQUETA','IDVALOR','IDGRUPOET','ID'];
    for (const k of need) if (!q[k]) throw new Error(`Falta clave: ${k}`);

    const deleted = await ZTGRUPOSET.findOneAndDelete(q);
    if (!deleted) throw new Error('No se encontró el registro a eliminar físicamente.');

    return { message: 'Registro eliminado físicamente.' };
  } catch (error) {
    return { error: error.message };
  }
}

module.exports = {
  GetAllGrupoSet,
  GetByIdGrupoSet,
  AddOneGrupoSet,
  UpdateOneGrupoSet,
  DeleteOneGrupoSet,
  DeleteHardGrupoSet
};
