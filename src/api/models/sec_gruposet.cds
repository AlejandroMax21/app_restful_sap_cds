namespace Security;

/** ZTGRUPOSET (Gestión de Grupos / Subgrupos de SKU) */
entity ZTGRUPOSET {
  key IDSOCIEDAD : Integer;
  key IDCEDI     : Integer;
  key IDETIQUETA : String(100);
  key IDVALOR    : String(100);
  key IDGRUPOET  : String(100);
  key ID         : String(50);

  INFOAD        : String(255);

  FECHAREG      : Date;
  HORAREG       : Time;
  USUARIOREG    : String(20);

  FECHAULTMOD   : Date;
  HORAULTMOD    : Time;
  USUARIOMOD    : String(20);

  ACTIVO        : Boolean @cds.default: true;
  BORRADO       : Boolean @cds.default: false;
}
