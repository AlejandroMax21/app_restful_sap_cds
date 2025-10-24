// src/api/routes/sec-gruposet-router.cds
// == Router CDS con acción única `crud` (formato igual al de roles) ==

using Security as mysec from '../models/sec_gruposet';

// Vincula este router con tu controller JS
@impl: 'src/api/controllers/sec-gruposet-controller.js'
service SecurityGruposetRoute @(path: '/api/security/gruposet') {

  // Exponemos la entidad como proyección (igual que en el ejemplo de roles)
  @cds.autoexpose
  entity ZTGRUPOSET as projection on mysec.ZTGRUPOSET;

  // Dispatcher único (CRUD)
  @Core.Description: 'CRUD dispatcher para gruposet'
  @path: 'crud'
  action crud(
    // Tipo de operación (p.ej. 'getAll', 'getById', 'create', 'updateone', 'deleteone', 'deletehard')
    ProcessType : String,

    // Clave compuesta de ZTGRUPOSET (todas opcionales; usa las que apliquen)
    IDSOCIEDAD  : Integer,
    IDCEDI      : Integer,
    IDETIQUETA  : String,
    IDVALOR     : String,
    IDGRUPOET   : String,
    ID          : String,
    INFOAD      : String,
    ACTIVO      : Boolean,
    BORRADO     : Boolean,

    // Carga útil flexible. Para create/update manda aquí el json del/los registros
    data        : Map
  ) returns ZTGRUPOSET;
}
