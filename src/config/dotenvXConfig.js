const dotenvx = require('@dotenvx/dotenvx');
dotenvx.config();


module.exports = {
    HOST: 'localhost' || 'NO ENCONTRE VARIABE DE ENTORNO',
    PORT: 3333 || 'NO ENCONTRE PORT',
    API_URL: '/api/v1'  || '/api/v1',
    CONNECTION_STRING: 'mongodb+srv://reacherdaniel:LifeHouseWEL@iwteam3.scpqhig.mongodb.net/' || 'SIN Cadena de CONEXION A LA BD MONGO', 
    DATABASE: 'db_Grupos'  || 'db_default',  
    DB_USER: 'reacherdaniel'  || 'admin',  
    DB_PASSWORD: 'LifeHouseWEL'   || 'admin', 
}

