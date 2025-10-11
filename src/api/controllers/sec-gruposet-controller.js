const cds = require('@sap/cds');

const {
  GetAllGrupoSet,
  AddOneGrupoSet,
  UpdateOneGrupoSet,
  DeleteOneGrupoSet,
  DeleteHardGrupoSet,
  GetByIdGrupoSet
} = require('../services/sec-gruposet-service');

class GruposetController extends cds.ApplicationService {
  async init() {

    // GET all / detail
    this.on('getall', async (req) => GetAllGrupoSet(req));

    //  detail by id
   this.on('getbyid',   req => GetByIdGrupoSet(req)); 

    // POST insert (uno o varios)
    this.on('addone', async (req) => AddOneGrupoSet(req));

    // POST update (por clave compuesta en query + body parcial)
    this.on('updateone', async (req) => UpdateOneGrupoSet(req));

    // POST delete lógico (por clave compuesta en query)
    this.on('deleteone', async (req) => DeleteOneGrupoSet(req));

    // delete fisico
    this.on('deletehard', req => DeleteHardGrupoSet(req));

    return await super.init();
  }
}

module.exports = GruposetController;
