// Ajusta la ruta si tu modelo está en otra carpeta:
const SEC_USERS = require('../models/mongodb/security/ztusers'); // p.ej. src/api/mongodb/security/ztusers.js

// Helpers
const today = () => new Date().toISOString().slice(0, 10);
const nowHHMMSS = () => new Date().toISOString().slice(11, 19);

// ========== QUERIES ==========

async function GetAllUsers(req) {
  try {
    const q = req.req?.query || {};
    // filtros básicos opcionales
    const filter = {};
    if (q.COMPANYID)  filter.COMPANYID = String(q.COMPANYID);
    if (q.USERNAME)   filter.USERNAME  = String(q.USERNAME);
    if (q.EMAIL)      filter.EMAIL     = String(q.EMAIL);
    if (q.DELETED !== undefined) filter.DELETED = (q.DELETED === 'true' || q.DELETED === true);

    const users = await SEC_USERS.find(Object.keys(filter).length ? filter : {}).lean();
    return users;
  } catch (error) {
    return { error: error.message };
  }
}

async function GetUserById(req) {
  try {
    const userId = req.data?.userid || req.req?.query?.USERID;
    if (!userId) throw new Error("Falta 'USERID'");

    const user = await SEC_USERS.findOne({ USERID: String(userId) }).lean();
    return user ?? {};
  } catch (error) {
    return { error: error.message };
  }
}

// ========== MUTACIONES ==========

async function CreateUser(req) {
  try {
    // En tu router, createUser(user: users) => un solo objeto; pero soportamos array también
    const payload = req.req?.body?.user || req.data?.user || req.req?.body || {};
    const arr = Array.isArray(payload) ? payload : [payload];

    const inserted = await SEC_USERS.insertMany(arr, { ordered: true });
    return JSON.parse(JSON.stringify(Array.isArray(payload) ? inserted : inserted[0]));
  } catch (error) {
    return { error: error.message };
  }
}

async function UpdateOneUser(req) {
  try {
    // En tu router, pasa el objeto completo; sacamos USERID del body o de query
    const bodyUser = req.req?.body?.user || req.data?.user || {};
    const userId = req.req?.query?.USERID || bodyUser.USERID;
    if (!userId) throw new Error("Falta 'USERID' para actualizar");

    // Ejemplo de auditoría simple
    const updateData = {
      ...bodyUser,
      FECHAULTMOD: today(),
      HORAULTMOD:  nowHHMMSS(),
    };

    const updated = await SEC_USERS.findOneAndUpdate({ USERID: String(userId) }, updateData, { new: true, upsert: false });
    if (!updated) throw new Error(`No se encontró USERID=${userId}`);

    return JSON.parse(JSON.stringify(updated));
  } catch (error) {
    return { error: error.message };
  }
}

// Borrado lógico
async function DeleteUsers(req) {
  try {
    const userId = req.req?.query?.USERID || req.data?.USERID;
    if (!userId) throw new Error("Falta 'USERID' para borrar (lógico)");

    const updated = await SEC_USERS.findOneAndUpdate(
      { USERID: String(userId) },
      { DELETED: true },                                // flag suave
      { new: true }
    );
    if (!updated) throw new Error(`No se encontró USERID=${userId}`);

    return JSON.parse(JSON.stringify(updated));
  } catch (error) {
    return { error: error.message };
  }
}

// Borrado físico
async function PhysicalDeleteUser(req) {
  try {
    const userId = req.data?.userid || req.req?.query?.USERID;
    if (!userId) throw new Error("Falta 'USERID' para borrado físico");

    const deleted = await SEC_USERS.findOneAndDelete({ USERID: String(userId) });
    if (!deleted) throw new Error(`No se encontró USERID=${userId}`);

    return 'Usuario eliminado físicamente.';
  } catch (error) {
    return { error: error.message };
  }
}

// Activar (quitar borrado lógico)
async function ActivateUsers(req) {
  try {
    const userId = req.req?.query?.USERID || req.data?.USERID;
    if (!userId) throw new Error("Falta 'USERID' para activar");

    const updated = await SEC_USERS.findOneAndUpdate(
      { USERID: String(userId) },
      { DELETED: false },
      { new: true }
    );
    if (!updated) throw new Error(`No se encontró USERID=${userId}`);

    return JSON.parse(JSON.stringify(updated));
  } catch (error) {
    return { error: error.message };
  }
}

module.exports = {
  GetAllUsers,
  GetUserById,
  CreateUser,
  UpdateOneUser,
  DeleteUsers,
  PhysicalDeleteUser,
  ActivateUsers
};
