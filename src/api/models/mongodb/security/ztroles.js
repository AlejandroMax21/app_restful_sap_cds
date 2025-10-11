// src/api/mongodb/security/zroles.js
const mongoose = require('mongoose');


// Subesquema: renglón de auditoría
const auditDetailRowSchema = new mongoose.Schema({
  CURRENT:   { type: Boolean, required: true, default: true },
  REGDATE:   { type: Date,    default: Date.now },
  REGTIME:   { type: String,  default: () => new Date().toTimeString().split(' ')[0] },
  REGUSER:   { type: String,  required: true }
}, { _id: false });

// Subesquema: bloque de auditoría completo
const auditDetailSchema = new mongoose.Schema({
  ACTIVO:         { type: Boolean, default: true },
  ELIMINADO:      { type: Boolean, default: false },
  DETAIL_ROW_REG: [ auditDetailRowSchema ]
}, { _id: false });

// Subesquema: privilegios por proceso
const privilegeSchema = new mongoose.Schema({
  PROCESSID:   { type: String, required: true },
  PRIVILEGEID: { type: String, required: true }
}, { _id: false });

// Esquema principal: Roles
const roleSchema = new mongoose.Schema({
  ROLEID:          { type: String, required: true, unique: true },
  ROLENAME:        { type: String, required: true },
  DESCRIPTION:     { type: String, required: true },

  // Arreglo de privilegios
  PRIVILEGES:      [ privilegeSchema ],

  // Auditoría “plana”
  ACTIVO:          { type: Boolean, default: true },
  ELIMINADO:       { type: Boolean, default: false },
  CURRENT:         { type: Boolean, default: true },
  FechaRegistro:   { type: Date,    default: Date.now },
  HoraRegistro:    { type: String,  default: () => new Date().toTimeString().split(' ')[0] },
  UsuarioRegistro: { type: String,  required: true },

  // Auditoría detallada (histórico)
  DETAIL_ROW:      auditDetailSchema
}, {
  collection: 'ZTRoles',
  timestamps: false
});

// Índices
roleSchema.index({ ROLEID: 1 }, { unique: true });
roleSchema.index({ ROLENAME: 1 });

module.exports = mongoose.model(
  'ZTRoles', 
  roleSchema, 
  'ZTRoles'
);
