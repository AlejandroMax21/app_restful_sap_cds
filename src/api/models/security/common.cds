namespace sec;

entity AuditDetail {
  key ID          : UUID;
  ACTIVATED       : Boolean default true;
  DELETED         : Boolean default false;

  // Histórico (to-many) con backlink obligatorio al padre
  DETAIL_ROW_REG  : Composition of many AuditDetailReg
                    on DETAIL_ROW_REG.parent = $self;
}

entity AuditDetailReg {
  key ID          : UUID;
  // Backlink al padre de la composición
  parent   : Association to AuditDetail;

  CURRENT  : Boolean default true;
  REGDATE  : Timestamp;          // o Date, si prefieres dividir fecha/hora
  REGTIME  : Timestamp;          // o Time
  REGUSER  : String(20);
}