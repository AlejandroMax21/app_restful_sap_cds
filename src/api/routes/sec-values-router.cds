using { sec as mysec } from '../models/security/sec-values';

@impl: 'src/api/controllers/sec-values-controller.js'
service SecurityValuesRoute @(path:'/api/security/values') {

  entity values as projection on mysec.Values;

  @path:'values'
  function getall() returns array of values;

  @path:'create'
  action addone(values: array of values) returns array of values;

  @path:'updateone'
  action updateone(values: values) returns values;

  @path:'deleteone'
  action deleteone() returns values; // VALUEID por query string
}
