const { Schema, model } = require('mongoose');

const ZTUsersSchema = new Schema({
  USERID:     { type: String, required: true, maxlength: 50, trim: true },
  USERNAME:   { type: String, required: true, maxlength: 100, trim: true },
  EMAIL:      { type: String, maxlength: 120, trim: true },
  FUNCTION:   { type: String, maxlength: 80 },
  DEPARTMENT: { type: String, maxlength: 80 },
  ACTIVO:     { type: Boolean, default: true },
  BORRADO:    { type: Boolean, default: false },
  FECHAREG:   { type: String },
  HORAREG:    { type: String },
  USUARIOREG: { type: String, maxlength: 20 },
  FECHAULTMOD:{ type: String },
  HORAULTMOD: { type: String },
  USUARIOMOD: { type: String, maxlength: 20 }
}, { collection: 'ztusers', versionKey: false });

ZTUsersSchema.index({ USERID: 1 }, { unique: true });

module.exports = model(
    'ZTUsers', 
    ZTUsersSchema
);
