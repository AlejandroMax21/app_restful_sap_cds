using { sec as mysec } from '../models/security/sec-users';  // o con .cds, ambas valen
@impl: 'src/api/controllers/sec-users-controller.js'

service SecurityUsersRoute @(path:'/api/security/users') {
  entity users as projection on mysec.Users;

  @path:'users'              
  function getAllUsers() returns array of users;

  @path:'users/:userid'      
  function getUserById(userid: String) returns users;

  @path:'createuser'         
  action createUser(user: users) returns users;

  @path:'updateone'          
  action updateone(user: users) returns users;

  @path:'deleteusers'        
  action deleteusers(USERID: String) returns users;

  @path:'physicalDeleteUser' 
  action physicalDeleteUser(userid: String) returns String;
  
  @path:'activateusers'      
  action activateusers(USERID: String) returns users;
}