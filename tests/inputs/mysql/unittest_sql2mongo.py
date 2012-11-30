#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os,sys
import unittest
from pprint import pprint

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../../src"))

from inputs.mysql import sql2mongo

class TestConversions (unittest.TestCase) :
    
    def setUp(self) :
        schema = {
            'users': ['a', 'b', 'name'],
            'review' : ['rating'],
            'trust' : [],
            'user' : []
        }
        self.mongo = sql2mongo.Sql2Mongo(schema)
    
    #def testSelectQueryInClause(self):
        #sql = 'SELECT name FROM users WHERE a IN (3)'
        #self.mongo.process_sql(sql)
        #result = self.mongo.render_mongo_command()
        #self.assertEqual(u"db.users.find({'a':{#gt:10,#lt:20}})", result[0])
     
    def testSelectQueryIsClauseTrace(self):
        sql = 'SELECT name FROM users WHERE a IS NULL'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {u'#query' : { u'a' : 'null'}}
        self.assertEqual(output, result[0])
        
    def testSelectQueryInClauseTrace(self):
        sql = 'SELECT name FROM users WHERE a IN (3)'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {u'#query' : { u'a' : { '#in': [ 3 ]}}}
        self.assertEqual(output, result[0])
        
    def testDeleteQuery01(self) :
        sql = 'DELETE FROM users WHERE z="abc"'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.remove({z:'abc'})", result[0])
        
    def testDeleteQuery01Trace(self) :
        sql = 'DELETE FROM users WHERE z="abc"'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {'z':'abc'}
        self.assertEqual(output, result[0])
    
    def testInsertQuery01(self) :
        sql = 'INSERT INTO users VALUES (3,5)'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u'db.users.insert({a:3,b:5})', result[0])
            
    def testInsertQuery01Trace(self) :
        sql = 'INSERT INTO users VALUES (3,5)'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {'a' : 3, 'b' : 5}
        self.assertEqual(output, result[0])
        
    def testQueryType02(self) :
        sql = 'INSERT INTO users (a, b, name) VALUES (1, 2, "rob");'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {'a' : 1, 'b' : 2, 'name': 'rob'}
        self.assertEqual(output, result[0])
        
    def testQueryTypeCommit(self) :
        sql = 'COMMIT'
        self.mongo.process_sql(sql)
        result = self.mongo.query_type
        self.assertEqual('UNKNOWN', result)
    
    def testQueryTypeDelete(self) :
        sql = 'DELETE FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.query_type
        self.assertEqual('DELETE', result)
        
    def testQueryTypeInsert(self) :
        sql = 'INSERT INTO users VALUES (1, 2);'
        self.mongo.process_sql(sql)
        result = self.mongo.query_type
        self.assertEqual('INSERT', result)
    
    def testQueryTypeSelect(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.query_type
        self.assertEqual('SELECT', result)
    
    def testQueryTypeUpdate(self) :
        sql = 'UPDATE users SET a = 1 WHERE b = 2'
        self.mongo.process_sql(sql)
        result = self.mongo.query_type
        self.assertEqual('UPDATE', result)
        
    def testSelectQuery01(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u'db.users.find({}, {a:1,b:1})', result[0])
    
    def testSelectQuery01Trace(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        self.assertEqual({u'#query' :{}}, result[0])
        
    def testSelectQuery01Op(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.generate_operations(100)
        self.assertEqual(1, len(result))
        self.assertIsNotNone(result[0]["query_fields"])
        for c in [ "a", "b" ]:
            self.assertIn(c, result[0]["query_fields"])
    
    def testSelectQuery02(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({}, {a:1,b:1})", result[0])
        
    def testSelectQuery02Trace(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        self.assertEqual({u'#query':{}}, result[0])
        
    def testSelectQuery02Op(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.generate_operations(100)
        self.assertEqual(1, len(result))
        self.assertIsNotNone(result[0]["query_fields"])
        for c in [ "a", "b" ]:
            self.assertIn(c, result[0]["query_fields"])
        
    def testSelectQuery03(self) :
        sql = 'SELECT * FROM users WHERE age=33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u'db.users.find({age:33})', result[0])
        
    def testSelectQuery03Trace(self) :
        sql = 'SELECT * FROM users WHERE age=33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {u'#query':{'age':33.0}}
        self.assertEqual(output, result[0])
        
    def testSelectQuery03Op(self) :
        sql = 'SELECT * FROM users WHERE age=33'
        self.mongo.process_sql(sql)
        result = self.mongo.generate_operations(100)
        self.assertEqual(1, len(result))
        self.assertIsNone(result[0]["query_fields"])
    
    def testSelectQuery04(self) :
        sql = 'SELECT a,b FROM users WHERE age=33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({age:33}, {a:1,b:1})", result[0])
    
    def testSelectQuery04Trace(self) :
        sql = 'SELECT a,b FROM users WHERE age=33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {u'#query':{'age':33.0}}
        self.assertEqual(output, result[0])
        
    def testSelectQuery05(self) :
        sql = 'SELECT * FROM users WHERE age=33 ORDER BY name'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({age:33}).sort({name:1})", result[0])
    
    def testSelectQuery06(self) :
        sql = 'SELECT * FROM users WHERE age>33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({age:{#gt:33}})", result[0])
    
    def testSelectQuery06Trace(self) :
        sql = 'SELECT * FROM users WHERE age>33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {'#query' : {u'age' : { '#gt' : 33.0}}}
        self.assertEqual(output, result[0])
        
    def testSelectQuery07(self) :
        sql = 'SELECT * FROM users WHERE age!=33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({age:{#ne:33}})", result[0])
    
    def testSelectQuery08(self) :
        sql = 'SELECT * FROM users WHERE name LIKE "%Joe%"'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({name:/Joe/})", result[0])
        
    def testSelectQuery09(self) :
        sql = 'SELECT * FROM users WHERE name LIKE "Joe%"'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({name:^Joe/})", result[0])
        
    def testSelectQuery08(self) :
        sql = 'SELECT * FROM users WHERE name LIKE "%Joe"'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({name:/Joe^})", result[0])
    
    def testSelectQuery09(self) :
        sql = 'SELECT * FROM users ORDER BY name DESC'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find().sort({name:-1})", result[0])
    
    def testSelectQuery10(self) :
        sql = 'SELECT * FROM users WHERE a=1 and b="q"'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({a:1,b:'q'})", result[0])
        
    def testSelectQuery10Trace(self) :
        sql = 'SELECT * FROM users WHERE a=1 and b="q"'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {u'#query': {u'a':1.0,u'b':'q'}}
        self.assertEqual(output, result[0])
    
    def testSelectQuery11(self) :
        sql = 'SELECT * FROM users LIMIT 10 SKIP 20'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find().limit(10).skip(20)", result[0])
        
    def testSelectQuery12(self) :
        sql = 'SELECT * FROM users WHERE a=1 or b=2'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({$or:[{a:1},{b:2}]})", result[0])
        
    def testSelectQuery13(self) :
        sql = 'SELECT * FROM users LIMIT 1'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find().limit(1)", result[0])
    
    def testSelectQuery14(self) :
        sql = 'SELECT * FROM users u WHERE u.a > 10 AND u.a < 20'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({'a':{#gt:10,#lt:20}})", result[0])
        
    def testSelectQuery14Trace(self) :
        sql = 'SELECT * FROM users WHERE a > 10 AND a < 20'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {u'#query' : { 'a' : { '#gt':10.0, '#lt':20.0}}}
        self.assertEqual(output, result[0])
    
    def testSelectQuery15(self) :
        sql = 'SELECT avg(rating) FROM review r, user u WHERE u.u_id = r.u_id AND r.u_id=2000 ORDER BY rating LIMIT 10'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        #print self.mongo.render_trace()
    
    def testSelectQuery15Op(self) :
        sql = 'SELECT avg(rating) FROM review r, user u WHERE u.u_id = r.u_id AND r.u_id=2000 ORDER BY rating LIMIT 10'
        self.mongo.process_sql(sql)
        result = self.mongo.generate_operations(100)
        self.assertEqual(2, len(result))
    
    def testUpdateQuery01(self) :
        sql = "UPDATE users SET a=1 WHERE b='q'"
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.update({b:'q'}, {$set:{a:1}}, false, true)", result[0])

    def testUpdateQuery02(self) :
        sql = "UPDATE users SET name = 'XXXXXXXXXXX' WHERE u_id=2000"
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.update({u_id:2000}, {$set:{name:'XXXXXXXXXXX'}}, false, true)", result[0])

    def testUpdateQuery01Trace(self) :
        sql = "UPDATE users SET a=1 WHERE b='q'"
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        query = {'b' : 'q'}
        set = {'a' : 1.0}
        self.assertEqual(query, result[0])
        self.assertEqual(set, result[1])
    
## END CLASS

if __name__ == '__main__':
    unittest.main()
## END MAIN