namespace sec_Execute;

entity Execute{
    key EXEC_ID: String(100);
    key ORDER_ID: String(100);
    key TS: Date;

    PRICE: Double;
    QTY: Integer;

    COMISSION:Integer @cds.default: 0;
    PNL: Integer @cds.default: 0;
}