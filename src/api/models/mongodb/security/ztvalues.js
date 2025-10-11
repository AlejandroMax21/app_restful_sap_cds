const { Schema, model } = require('mongoose');

const ZTValuesSchema = new Schema({
  LABELID:   { type: String, required: true, maxlength: 100, trim: true },  // IDETIQUETA
  VALUEID:   { type: String, required: true, maxlength: 100, trim: true },  // IDVALOR
  VALUEDESC: { type: String, maxlength: 255 },
  ACTIVO:    { type: Boolean, default: true },
  BORRADO:   { type: Boolean, default: false },
  FECHAREG:  { type: String },
  HORAREG:   { type: String },
  USUARIOREG:{ type: String, maxlength: 20 },
  FECHAULTMOD:{ type: String },
  HORAULTMOD: { type: String },
  USUARIOMOD: { type: String, maxlength: 20 }
}, { collection: 'ztvalues', versionKey: false });

ZTValuesSchema.index({ LABELID: 1, VALUEID: 1 }, { unique: true });

module.exports = model(
    'ZTValues', 
    ZTValuesSchema
);
