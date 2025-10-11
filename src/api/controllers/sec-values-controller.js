// src/api/controllers/sec-values-controller.js
const cds = require('@sap/cds');

const {
  GetAllValues,
  AddOneValue,
  UpdateOneValue,
  DeleteOneValue
} = require('../services/sec-values-service');

class ValuesController extends cds.ApplicationService {
  async init() {

    // GET list/filters
    this.on('getall',    (req) => GetAllValues(req));

    // POST create (uno o varios)
    this.on('addone',    (req) => AddOneValue(req));

    // POST update
    this.on('updateone', (req) => UpdateOneValue(req));

    // POST delete lógico
    this.on('deleteone', (req) => DeleteOneValue(req));

    return super.init();
  }
}

module.exports = ValuesController;
