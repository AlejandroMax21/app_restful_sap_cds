using { sec.AuditDetail } from './common';

namespace sec;

entity Roles {
  key ROLEID       : String(50);
  ROLENAME         : String(100);
  DESCRIPTION      : String(200);

  // relación a privilegios del rol
  PRIVILEGES       : Association to many RolePrivileges
                      on PRIVILEGES.ROLEID = $self;

  // auditoría
  DETAIL_ROW       : Composition of one AuditDetail;
}

entity RolePrivileges {
  key ROLEID       : Association to Roles;  // <-- key
  key PROCESSID    : String(50);            // <-- key
  key PRIVILEGEID  : String(50);            // <-- key
}

entity UserRoles {
  key USERID       : String(20);            // <-- key
  key ROLEID       : Association to Roles;  // <-- key
}
