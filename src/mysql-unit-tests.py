#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import sql2mongo

class TestConversions (unittest.TestCase) :
    
    def setUp(self) :
        self.schema = {'users': ['a', 'b']}
        self.mongo = sql2mongo.Sql2mongo(self.schema)
    
    def testDeleteQuery01(self) :
        sql = 'DELETE FROM users WHERE z="abc"'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.remove({z:'abc'})", result[0])
        
    def testInsertQuery01(self) :
        sql = 'INSERT INTO users VALUES (3,5)'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u'db.users.insert({a:3,b:5})', result[0])
        
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
        sql = 'INSERT INTO users VALUES (1, 2)'
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
        self.assertEqual({u'query' :{}}, result[0])
    
    def testSelectQuery02(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({}, {a:1,b:1})", result[0])
        
    def testSelectQuery02Trace(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        ## Figure out projects in TRACE format
        self.assertEqual({u'query':{}}, result[0])
        
    def testSelectQuery03(self) :
        sql = 'SELECT * FROM users WHERE age=33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        output = {u'query':{'age':'33'}}
        self.assertEqual(u'db.users.find({age:33})', result[0])
        
    def testSelectQuery03Trace(self) :
        sql = 'SELECT * FROM users WHERE age=33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_trace()
        output = {u'query':{'age':'33'}}
        self.assertEqual(output, result[0])
    
    def testSelectQuery04(self) :
        sql = 'SELECT a,b FROM users WHERE age=33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({age:33}, {a:1,b:1})", result[0])
    
    def testSelectQuery05(self) :
        sql = 'SELECT * FROM users WHERE age=33 ORDER BY name'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({age:33}).sort({name:1})", result[0])
    
    def testSelectQuery06(self) :
        sql = 'SELECT * FROM users WHERE age>33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({age:{$gt:33}})", result[0])
    
    def testSelectQuery07(self) :
        sql = 'SELECT * FROM users WHERE age!=33'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({age:{$ne:33}})", result[0])
    
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

    def testUpdateQuery01(self) :
        sql = "UPDATE users SET a=1 WHERE b='q'"
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.update({b:'q'}, {$set:{a:1}}, false, true)", result[0])
## END CLASS

if __name__ == '__main__':
    unittest.main()
## END MAIN