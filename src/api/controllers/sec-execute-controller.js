// src/api/controllers/sec-execute-controller.js
const cds = require('@sap/cds');
const { crudExecute } = require('../services/execute.service')

class ExecuteController extends cds.ApplicationService {
  async init () {
    // Delegamos TODO al dispatcher del service:
    this.on('crudExecute', req => crudExecute(req));
    return super.init();
  }
}

module.exports = ExecuteController;