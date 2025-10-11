const cds = require('@sap/cds');

const {
  GetAllUsers,
  GetUserById,
  CreateUser,
  UpdateOneUser,
  DeleteUsers,
  PhysicalDeleteUser,
  ActivateUsers
} = require('../services/sec-users-service');

class UsersController extends cds.ApplicationService {
  async init() {

    this.on('getAllUsers',       (req) => GetAllUsers(req));
    this.on('getUserById',       (req) => GetUserById(req));
    this.on('createUser',        (req) => CreateUser(req));
    this.on('updateone',         (req) => UpdateOneUser(req));
    this.on('deleteusers',       (req) => DeleteUsers(req));        // soft
    this.on('physicalDeleteUser',(req) => PhysicalDeleteUser(req)); // hard
    this.on('activateusers',     (req) => ActivateUsers(req));      // quitar soft delete

    return super.init();
  }
}

module.exports = UsersController;
