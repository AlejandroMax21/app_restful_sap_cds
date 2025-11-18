
using sec_Execute as mysec from '../models/execute';

// Vincula este router con tu controller JS
@impl: 'src/api/controllers/sec-execute-controller.js'
service SecurityExecuteRoute @(path: '/api/security/execute') {

  // Exponemos la entidad como proyección (igual que en el ejemplo de roles)
  @cds.autoexpose
  entity Execute as projection on mysec.Execute;

  // Dispatcher único (CRUD)
  @Core.Description: 'CRUD dispatcher para execute'
  @path: 'crudExecute'
  action crudExecute(
    // Tipo de operación (p.ej. 'getAll', 'getById', 'create', 'updateone', 'deleteone', 'deletehard')
    ProcessType : String,

    // Clave compuesta de Execute (todas opcionales; usa las que apliquen)
    EXEC_ID:    String,
    ORDER_ID:   String,
    TS:         Date,
    PRICE:      Double,
    QTY:        Integer,
    COMMISSION:  Integer,
    PNL:        Integer,

    // Carga útil flexible. Para create/update manda aquí el json del/los registros
    data        : Map
  ) returns Execute;
}
