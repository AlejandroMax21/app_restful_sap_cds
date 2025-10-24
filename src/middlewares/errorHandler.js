// src/api/middleware/errorHandler.js

module.exports = function errorHandler(err, req, res, next) {
  console.error("Error capturado:", err);

  // Si el error proviene del parser JSON
  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    return res.status(400).json({
      status: 400,
      messageUSR: 'El formato del JSON enviado es inválido.',
      messageDEV: 'JSON mal formado en el cuerpo de la solicitud.',
    });
  }

  // Si el error tiene datos personalizados (como en tus controladores)
  const status = err.status || 500;
  const messageUSR = err.messageUSR || 'Error interno del servidor.';
  const messageDEV = err.messageDEV || err.message || 'Error desconocido en el servidor.';

  return res.status(status).json({
    status,
    messageUSR,
    messageDEV,
  });
};
