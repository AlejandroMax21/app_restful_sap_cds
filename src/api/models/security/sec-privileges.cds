using { sec.AuditDetail }  from './common';
using { sec.RolePrivileges } from './sec-roles';

namespace sec;

entity Privileges {
  key PRIVILEGEID   : String(50);
      PRIVILEGENAME : String(100);
      DESCRIPTION   : String(200);

  DETAIL_ROW : Composition of one AuditDetail;
}

// Completar la asociación desde RolePrivileges → Privileges
extend entity sec.RolePrivileges with {
  PRIVILEGE : Association to Privileges
                on PRIVILEGE.PRIVILEGEID = PRIVILEGEID;
}
