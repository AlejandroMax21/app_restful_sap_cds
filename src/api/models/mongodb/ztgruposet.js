// src/api/mongodb/ztgruposet.js
const mongoose = require('mongoose');

const ztgruposetSchema = new mongoose.Schema({
  // Clave compuesta
  IDSOCIEDAD: { type: Number, required: true },
  IDCEDI:     { type: Number, required: true },
  IDETIQUETA: { type: String, required: true },
  IDVALOR:    { type: String, required: true },
  IDGRUPOET:  { type: String, required: true },
  ID:         { type: String, required: true },

  // Datos
  INFOAD:       { type: String, default: null },

  // Auditoría de alta
  FECHAREG:     { type: String },   // 'YYYY-MM-DD'
  HORAREG:      { type: String },   // 'HH:MM:SS'
  USUARIOREG:   { type: String },

  // Auditoría de modificación
  FECHAULTMOD:  { type: String },
  HORAULTMOD:   { type: String },
  USUARIOMOD:   { type: String },

  // Flags
  ACTIVO:       { type: Boolean, default: true },
  BORRADO:      { type: Boolean, default: false }
}, { collection: 'ZTGRUPOSET', versionKey: false });

// Índice único por la clave compuesta (opcional pero recomendado)
ztgruposetSchema.index(
  { IDSOCIEDAD: 1, IDCEDI: 1, IDETIQUETA: 1, IDVALOR: 1, IDGRUPOET: 1, ID: 1 },
  { unique: true, name: 'uk_ztgruposet_compound' }
);

module.exports = mongoose.model(
    'ZTGRUPOSET', 
    ztgruposetSchema, 
    'ZTGRUPOSET'
);
