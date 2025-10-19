// src/api/controllers/sec-gruposet-controller.js
const cds = require('@sap/cds');
const { crudGruposet } = require('../services/sec-gruposet-service');

class GruposetController extends cds.ApplicationService {
  async init () {
    // Delegamos TODO al dispatcher del service:
    this.on('crud', req => crudGruposet(req));
    return super.init();
  }
}

module.exports = GruposetController;
