// src/api/routes/sec-gruposet-router.cds
using Security as mysec from '../models/sec_gruposet';

// Vincula este router con tu controller JS
@impl: 'src/api/controllers/sec-gruposet-controller.js'
service SecurityGruposetRoute @(path:'/api/security/gruposet') {

  // <- IMPORTANTE: No persistir ni exponer entity set
  @cds.persistence.skip
  entity gruposet as projection on mysec.ZTGRUPOSET;

  @Core.Description: 'Obtener todos los registros'
  @path: 'getall'
  function getall() returns array of gruposet;

  @path: 'getbyid'
  function getbyid(ID: String) returns gruposet;

  @Core.Description: 'Crear uno o varios registros'
  @path: 'addone'
  action addone(gruposet: array of gruposet) returns array of gruposet;

  @Core.Description: 'Actualizar un registro (llaves por query, cambios en body)'
  @path: 'updateone'
  action updateone(gruposet: gruposet) returns gruposet;

  @Core.Description: 'Borrado lógico (llaves por query)'
  @path: 'deleteone'
  action deleteone() returns gruposet;

  // Opcional: borrado físico (llaves por query string)
  @Core.Description: 'Borrado físico (llaves por query)'
  @path: 'deletehard'
  action deletehard() returns String;
}
