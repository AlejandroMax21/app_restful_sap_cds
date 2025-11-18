const mongoose = require("mongoose");
/**
* Execution
* Detalla cada llenado proveniente del broker. Permite reconciliar ordenes y calcular PnL granular.
*/
const executionSchema = new mongoose.Schema(
  {
    // Identificador unico del broker (ej. "ABC12345"). String para preservar formato textual.
    EXEC_ID: { type: String, unique: true, required: true, trim: true },
    // Orden asociada. ObjectId enlaza con la coleccion Order.
    ORDER_ID: { type: String, unique: true, required: true, trim: true },
    // Momento exacto de la ejecucion.
    TS: { type: Date, required: true, index: true },
    // Precio y cantidad ejecutada. Number (double) facilita calculos financieros.
    PRICE: Number,
    QTY: Number,
    // Costos de la ejecucion y PnL incremental.
    COMMISSION: { type: Number, default: 0 },
    PNL: { type: Number, default: 0 },
    // Auditoria Mongo.
    //createdAt: Date,
    //updatedAt: Date
  },
  { collection: 'Execution', versionKey: false }
);

// index ejecutando las órdenes recientes primero
executionSchema.index({ order_id: 1, ts: -1 });

module.exports = mongoose.model(
    'Execution', 
    executionSchema, 
    'Execution'
);

