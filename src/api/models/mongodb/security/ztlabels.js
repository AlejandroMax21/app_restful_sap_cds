const { Schema, model } = require('mongoose');

const ZTLabelsSchema = new Schema({
  LABELID:    { type: String, required: true, maxlength: 100, trim: true },  // IDETIQUETA
  LABELDESC:  { type: String, maxlength: 255 },
  ACTIVO:     { type: Boolean, default: true },
  BORRADO:    { type: Boolean, default: false },
  FECHAREG:   { type: String },
  HORAREG:    { type: String },
  USUARIOREG: { type: String, maxlength: 20 },
  FECHAULTMOD:{ type: String },
  HORAULTMOD: { type: String },
  USUARIOMOD: { type: String, maxlength: 20 }
}, { collection: 'ztlabels', versionKey: false });

ZTLabelsSchema.index({ LABELID: 1 }, { unique: true });

module.exports = model('ZTLabels', ZTLabelsSchema);
